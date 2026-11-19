// Skeld server v4 (advance) — tanpa dependensi: node server.js  (butuh Node 18+, disarankan 22)
// Protokol WebSocket & REST 100% kompatibel dengan index.html lama (klien lama = protokol v1, tidak berubah).
// Klien baru boleh opt-in protokol v2 dengan  {t:'join',v:2,...}  (lihat net-v2.js):
//   - snapshot posisi BINER (21 byte/pemain, tanpa data diri sendiri) + stempel waktu server + nomor urut
//   - posisi masuk juga biner (18 byte), sinkron jam klien-server, RTT terukur server (field r di pong)
//   - token sesi: pemain yang putus saat match berjalan bisa masuk kembali ke slotnya (RESUME_MS)
// Konfigurasi lewat env / file .env. Opsi baru (semua opsional):
//   TICK_MS=33        interval siaran posisi (ms). 25 = 40Hz, 33 = 30Hz, 50 = 20Hz (lama)
//   WS_PER_IP=10      maks koneksi WebSocket per IP
//   WS_MAX=500        maks koneksi WebSocket total
//   WS_MPS=120        maks pesan masuk per detik per koneksi
//   API_RPM=240       rate limit REST per IP / menit (chat dibatasi 40/menit)
//   WS_PING_MS=5000   interval ping ke klien (juga untuk ukur RTT); WS_DEAD_MS=35000 klien tanpa respons dianggap putus
//   RESUME_MS=20000   lama slot ditahan untuk pemain v2 yang putus saat match
//   MAX_PROFILES=20000  batas profil tersimpan (yang paling lama tak aktif dibuang)
//   ALLOWED_ORIGINS=https://situsmu.com,https://www.situsmu.com   (kosong = semua boleh)
'use strict';
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto'),zlib=require('zlib');
const {monitorEventLoopDelay}=require('perf_hooks');
const {Pool}=require('pg');
try{for(const l of fs.readFileSync(path.join(__dirname,'.env'),'utf8').split(/\r?\n/)){const m=l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);if(m&&!(m[1] in process.env))process.env[m[1]]=m[2].replace(/^(['"])(.*)\1$/,'$2')}}catch(e){}

// ===== Konfigurasi =====
const E=process.env,PORT=E.PORT||3000,DB=path.join(__dirname,'data.json'),TMP=DB+'.tmp',IDX=path.join(__dirname,'index.html'),
POOL=E.DATABASE_URL?new Pool({connectionString:E.DATABASE_URL,ssl:{rejectUnauthorized:false},max:+E.DB_POOL_MAX||5,idleTimeoutMillis:30000,connectionTimeoutMillis:10000}):null,SERVER_ID=E.SERVER_ID||'primary',SERVER_URL=E.SERVER_URL||(E.RENDER_EXTERNAL_HOSTNAME?'https://'+E.RENDER_EXTERNAL_HOSTNAME:'http://localhost:'+PORT),
KEY=E.AI_API_KEY||E.ANTHROPIC_API_KEY||'',
PROV=(E.AI_PROVIDER||(E.AI_BASE_URL?'openai':'anthropic')).toLowerCase(),
BASE=(E.AI_BASE_URL||(PROV==='anthropic'?'https://api.anthropic.com':'https://api.openai.com/v1')).replace(/\/+$/,''),
GQ=/groq\.com/i.test(BASE),
MD=E.AI_MODEL||E.ANTHROPIC_MODEL||(PROV==='anthropic'?'claude-sonnet-5-5':GQ?'openai/gpt-oss-120b':''),
MODELS={default:MD,quick:E.AI_MODEL_QUICK||E.ANTHROPIC_MODEL_QUICK||(PROV==='anthropic'?'claude-haiku-5-5':GQ?'openai/gpt-oss-20b':MD)},
READY=!!(KEY&&MODELS.default),
CAP=+E.AI_DAILY_LIMIT||5000,RPM=+E.AI_RPM||60,
TICK=Math.max(16,+E.TICK_MS||33),MAXWS=+E.WS_MAX||500,WS_IP=+E.WS_PER_IP||10,WS_MPS=+E.WS_MPS||120,
MAXPROF=+E.MAX_PROFILES||20000,PING_MS=+E.WS_PING_MS||5000,HOLD_MS=+E.RESUME_MS||20000,DEAD_MS=+E.WS_DEAD_MS||35000,
ORIG=E.ALLOWED_ORIGINS?E.ALLOWED_ORIGINS.split(',').map(s=>s.trim()).filter(Boolean):null;

process.on('uncaughtException',e=>console.error('uncaught',e));
process.on('unhandledRejection',e=>console.error('unhandled',e));
const EL=monitorEventLoopDelay({resolution:10});EL.enable();setInterval(()=>EL.reset(),60000).unref();

// ===== Penyimpanan (atomic, non-blocking) =====
let data={chat:[],profiles:{},guests:0};
try{data=Object.assign(data,JSON.parse(fs.readFileSync(DB,'utf8')))}catch(e){}
let dirty=false,saving=false,chatC=null;
const dirtyProfiles=new Set(),pendingChat=[];
function prune(){const ks=Object.keys(data.profiles);if(ks.length<=MAXPROF)return;ks.sort((a,b)=>(data.profiles[a].t||0)-(data.profiles[b].t||0)).slice(0,ks.length-Math.floor(MAXPROF*0.9)).forEach(k=>delete data.profiles[k])}
async function flushNeon(){
 if(!POOL||saving||(!dirtyProfiles.size&&!pendingChat.length))return;
 saving=true;
 const profKeys=[...dirtyProfiles];profKeys.forEach(k=>dirtyProfiles.delete(k));
 const chats=pendingChat.splice(0,pendingChat.length);
 try{
  for(const id of profKeys){const d=data.profiles[id];if(!d)continue;await POOL.query(`INSERT INTO whos_sus.players(player_id,username,profile_data,updated_at) VALUES($1,$2,$3::jsonb,NOW()) ON CONFLICT(player_id) DO UPDATE SET username=EXCLUDED.username,profile_data=EXCLUDED.profile_data,updated_at=NOW()`,[id,String(d.name||id).slice(0,40),JSON.stringify(d)])}
  for(const c of chats)await POOL.query(`INSERT INTO whos_sus.chat_messages(player_id,username,message,created_at) VALUES($1,$2,$3,to_timestamp($4/1000.0))`,[c.u||null,String(c.n||'Pemain').slice(0,40),String(c.x||'').slice(0,60),c.t||Date.now()]);
 }catch(e){console.error('Neon write error:',e.message);for(const k of profKeys)dirtyProfiles.add(k);pendingChat.unshift(...chats)}
 finally{saving=false}
}
function save(){if(POOL){flushNeon().catch(e=>console.error('Neon flush:',e.message));return}if(!dirty||saving)return;dirty=false;saving=true;prune();
 fs.writeFile(TMP,JSON.stringify(data),e=>{if(e){dirty=true;saving=false;return}fs.rename(TMP,DB,e2=>{saving=false;if(e2)dirty=true})})}
setInterval(save,3000).unref();
function flush(){if(POOL){flushNeon().catch(e=>console.error('Neon shutdown flush:',e.message));return}try{if(dirty){prune();fs.writeFileSync(TMP,JSON.stringify(data));fs.renameSync(TMP,DB)}}catch(e){}}
async function initNeon(){if(!POOL)return;await POOL.query('SELECT 1');
 const pr=await POOL.query('SELECT player_id,profile_data FROM whos_sus.players ORDER BY updated_at DESC LIMIT $1',[MAXPROF]);
 data.profiles={};for(const row of pr.rows)data.profiles[row.player_id]=row.profile_data||{};
 const ch=await POOL.query('SELECT player_id,username,message,(extract(epoch from created_at)*1000)::bigint AS ts FROM whos_sus.chat_messages ORDER BY created_at DESC LIMIT 200');
 data.chat=ch.rows.reverse().map(x=>({n:x.username,u:x.player_id||'',x:x.message,t:Number(x.ts)}));chatC=null;
 await POOL.query(`INSERT INTO whos_sus.server_nodes(server_id,server_url,status,active_rooms,load_score,last_heartbeat,updated_at) VALUES($1,$2,'online',0,0,NOW(),NOW()) ON CONFLICT(server_id) DO UPDATE SET server_url=EXCLUDED.server_url,status='online',last_heartbeat=NOW(),updated_at=NOW()`,[SERVER_ID,SERVER_URL]);
 console.log('Neon connected · profiles '+pr.rowCount+' · chat '+ch.rowCount);
}
for(const s of['SIGINT','SIGTERM'])process.on(s,()=>{flush();for(const r of ROOMS.values())for(const p of r.ps.values())try{p.socket.write(Buffer.from([0x88,2,0x03,0xe9]))}catch(e){}setTimeout(()=>process.exit(0),200)});

// ===== Rate limit =====
const mkLim=max=>{const h=new Map();setInterval(()=>{const n=Date.now();for(const[k,a]of h)if(!a.length||n-a[a.length-1]>=60000)h.delete(k)},60000).unref();
 return ip=>{const n=Date.now();let a=h.get(ip);if(!a)h.set(ip,a=[]);while(a.length&&n-a[0]>=60000)a.shift();if(a.length>=max)return false;a.push(n);return true}};
const limAI=mkLim(RPM),limApi=mkLim(+E.API_RPM||240),limChat=mkLim(40);
let day=new Date().toDateString(),used=0;
const ipOf=q=>String(q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim();

// ===== Util HTTP =====
const str=(v,n)=>String(v==null?'':v).slice(0,n),num=v=>Math.max(0,Math.min(1e9,+v||0));
const send=(r,c,o,t)=>{const b=typeof o==='string'||Buffer.isBuffer(o)?o:JSON.stringify(o);r.writeHead(c,{'content-type':t||'application/json','cache-control':'no-store','x-content-type-options':'nosniff','content-length':Buffer.byteLength(b)});r.end(b)};
const body=(q,max=4096)=>new Promise((ok,no)=>{let n=0;const a=[];q.on('data',c=>{n+=c.length;if(n>max){no();q.destroy();return}a.push(c)});q.on('end',()=>{try{ok(JSON.parse(Buffer.concat(a).toString('utf8')||'{}'))}catch(e){no()}});q.on('error',no)});
const mkV=j=>({j,tag:'"'+crypto.createHash('sha1').update(j).digest('base64url').slice(0,16)+'"',gz:j.length>800?zlib.gzipSync(j):null});
const memo=(ttl,fn)=>{let t=0,v=null;return()=>{const n=Date.now();if(!v||n-t>ttl){v=mkV(JSON.stringify(fn()));t=n}return v}};
const sendC=(q,r,v)=>{const h={'content-type':'application/json','cache-control':'no-cache',etag:v.tag,vary:'accept-encoding'};if(q.headers['if-none-match']===v.tag){r.writeHead(304,h);return r.end()}
 let b=v.j;if(v.gz&&/\bgzip\b/.test(String(q.headers['accept-encoding']||''))){h['content-encoding']='gzip';b=v.gz}h['content-length']=Buffer.byteLength(b);r.writeHead(200,h);r.end(b)};
const profList=memo(3000,()=>Object.entries(data.profiles).sort((a,b)=>b[1].t-a[1].t).slice(0,40).map(([id,d])=>({id,d})));
const board=memo(3000,()=>Object.entries(data.profiles).filter(([id,d])=>d.mt>0).sort((a,b)=>b[1].rp-a[1].rp||b[1].wn-a[1].wn||b[1].lv-a[1].lv).slice(0,50).map(([id,d])=>({id,d:{name:d.name,lv:d.lv,rp:d.rp,wn:d.wn,mt:d.mt,kl:d.kl,reg:d.sreg?d.reg:'',t:d.t}})));
const chatV=()=>chatC||(chatC=mkV(JSON.stringify(data.chat.slice(-40).reverse().map((d,i)=>({id:String(i),d})))));

// ===== index.html: di-cache di memori, brotli/gzip, ETag (tanpa readFileSync per request) =====
let PAGE=null;
function loadPage(){try{const b=fs.readFileSync(IDX);PAGE={tag:'"'+crypto.createHash('sha1').update(b).digest('base64url').slice(0,20)+'"',raw:b,
 br:zlib.brotliCompressSync(b,{params:{[zlib.constants.BROTLI_PARAM_QUALITY]:9,[zlib.constants.BROTLI_PARAM_SIZE_HINT]:b.length}}),gz:zlib.gzipSync(b,{level:9})}}catch(e){PAGE=null}}
loadPage();fs.watchFile(IDX,{interval:2000,persistent:false},loadPage);
function page(q,r){if(!PAGE)return send(r,404,{});const ae=String(q.headers['accept-encoding']||''),
 h={'content-type':'text/html;charset=utf-8','cache-control':'no-cache',etag:PAGE.tag,vary:'accept-encoding'};
 if(q.headers['if-none-match']===PAGE.tag){r.writeHead(304,h);return r.end()}
 let b=PAGE.raw;if(/\bbr\b/.test(ae)){h['content-encoding']='br';b=PAGE.br}else if(/\bgzip\b/.test(ae)){h['content-encoding']='gzip';b=PAGE.gz}
 h['content-length']=b.length;r.writeHead(200,h);r.end(b)}

const rttStats=()=>{const a=[...ALL].map(w=>w.rtt).filter(Boolean).sort((x,y)=>x-y);return a.length?{rtt_p50_ms:Math.round(a[a.length>>1]),rtt_p95_ms:Math.round(a[Math.min(a.length-1,Math.floor(a.length*0.95))])}:{}};
// ===== Router HTTP =====
async function route(q,r){const u=q.url.split('?')[0],M=q.method;
 if(u==='/api/ping')return send(r,200,{ok:1,ai:READY,gap:Math.max(3,+E.AI_GAP||10),t:Date.now()});
 if(u==='/'||u==='/index.html')return page(q,r);
 if(u==='/api/stats')return send(r,200,{rooms:ROOMS.size,ws:WSN,up:process.uptime()|0,lag_p99_ms:+(EL.percentile(99)/1e6).toFixed(1),lag_max_ms:+(EL.max/1e6).toFixed(1),mem_mb:(process.memoryUsage().rss/1048576)|0,v2:[...ALL].filter(w=>w.v===2).length,held:HELD.size,...rttStats()});
 if(u==='/api/ai'&&M==='POST'){
  if(!READY)return send(r,503,{error:'AI_API_KEY / AI_MODEL belum diatur'});
  if(!limAI(ipOf(q)))return send(r,429,{error:'terlalu cepat'});
  const d=new Date().toDateString();if(d!==day){day=d;used=0}if(used>=CAP)return send(r,429,{error:'batas harian AI tercapai'});
  const b=await body(q,64000),p=str(b.prompt,48000);if(!p)return send(r,400,{});used++;
  const model=b.tier==='quick'?MODELS.quick:MODELS.default,A=PROV==='anthropic';
  const rs=await fetch(BASE+(A?'/v1/messages':'/chat/completions'),{method:'POST',signal:AbortSignal.timeout(40000),headers:A?{'content-type':'application/json','x-api-key':KEY,'anthropic-version':'2023-06-01'}:{'content-type':'application/json',authorization:'Bearer '+KEY},body:JSON.stringify(A?{model,max_tokens:4000,messages:[{role:'user',content:p}]}:{model,messages:[{role:'user',content:p}]})});
  if(!rs.ok){console.error('AI API',rs.status,(await rs.text()).slice(0,200));return send(r,502,{error:'AI gagal'})}
  const j=await rs.json();return send(r,200,{text:A?(j.content||[]).map(c=>c.text||'').join(''):str(j.choices&&j.choices[0]&&j.choices[0].message&&j.choices[0].message.content,8000)})}
 if(u==='/api/guest'&&M==='POST'){if(!limApi(ipOf(q)))return send(r,429,{error:'terlalu cepat'});data.guests=(data.guests||0)+1;dirty=true;return send(r,200,{n:data.guests})}
 if(u==='/api/chat'&&M==='GET')return sendC(q,r,chatV());
 if(u==='/api/chat'&&M==='POST'){if(!limChat(ipOf(q)))return send(r,429,{error:'terlalu cepat'});
  const b=await body(q),x=str(b.x,60).trim(),uid=str(b.u,24).replace(/[^\w-]/g,'');if(!x)return send(r,400,{});
  const entry={n:str(b.n,12),u:uid,x,t:Date.now()};data.chat.push(entry);if(data.chat.length>200)data.chat.splice(0,data.chat.length-200);if(POOL)pendingChat.push(entry);chatC=null;dirty=true;return send(r,200,{ok:1})}
 if(u==='/api/profiles'&&M==='GET')return sendC(q,r,profList());
 if(u==='/api/leaderboard'&&M==='GET')return sendC(q,r,board());
 const m=u.match(/^\/api\/profiles\/([\w-]{1,24})$/);
 if(m&&M==='PUT'){if(!limApi(ipOf(q)))return send(r,429,{error:'terlalu cepat'});
  const b=await body(q),o_=data.profiles[m[1]]||{};
  data.profiles[m[1]]={name:str(b.name,12),lv:num(b.lv),xp:num(b.xp),nx:num(b.nx),mt:num(b.mt),wn:num(b.wn),kl:num(b.kl),rp:Math.min(num(b.rp),99999),pk:Math.min(Math.max(num(b.pk),num(b.rp),num(o_.pk)),99999),age:num(b.age),reg:str(b.reg,20),bio:str(b.bio,40),show:b.show?1:0,sreg:b.sreg?1:0,t:Date.now()};
  if(POOL)dirtyProfiles.add(m[1]);dirty=true;return send(r,200,{ok:1})}
 send(r,404,{})}

const SRV=http.createServer((q,r)=>{route(q,r).catch(()=>{if(!r.headersSent)send(r,400,{});else r.end()})});
SRV.keepAliveTimeout=65000;SRV.headersTimeout=66000;SRV.requestTimeout=30000;
SRV.on('connection',s=>s.setNoDelay(true));
SRV.on('clientError',(e,s)=>{try{s.destroy()}catch(_){}});
async function boot(){try{await initNeon();SRV.listen(PORT,()=>console.log('Skeld server di http://localhost:'+PORT+' · tick '+TICK+'ms · DB: '+(POOL?'Neon':'local JSON')+' · AI: '+(READY?'aktif ['+PROV+'] '+MODELS.quick+' / '+MODELS.default:'MATI (set AI_API_KEY'+(PROV==='anthropic'?'':' dan AI_MODEL')+')')))}catch(e){console.error('Database startup failed:',e.message);process.exit(1)}}
boot();
if(POOL){const hb=setInterval(()=>{POOL.query(`UPDATE whos_sus.server_nodes SET status='online',active_rooms=$2,load_score=$3,last_heartbeat=NOW(),updated_at=NOW() WHERE server_id=$1`,[SERVER_ID,ROOMS.size,+(process.memoryUsage().rss/1048576).toFixed(1)]).catch(e=>console.error('Heartbeat error:',e.message))},15000);hb.unref();for(const sig of ['SIGINT','SIGTERM'])process.once(sig,()=>{POOL.query(`UPDATE whos_sus.server_nodes SET status='offline',last_heartbeat=NOW(),updated_at=NOW() WHERE server_id=$1`,[SERVER_ID]).catch(()=>{}).finally(()=>POOL.end().catch(()=>{}))})}


// ===== WebSocket multiplayer (tanpa dependensi) =====
// Server memegang peran, kill, rapat/vote, tugas, vent, revive, tebakan Blackjack, dan kondisi menang.
// Klien hanya mengirim niat (event); server memvalidasi lalu menyiarkan hasilnya. Bot disimulasikan oleh 'host' (human ber-id terkecil).
const ROOMS=new Map(),MAXP=10,FILL_MS=+E.FILL_MS||20000,MEET_MS=+E.MEET_MS||120000,GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11',IPC=new Map(),HELD=new Map(),ALL=new Set(),pingF=()=>{const b=Buffer.allocUnsafe(10);b[0]=0x89;b[1]=8;b.writeDoubleBE(Date.now(),2);return b};
let PID=0,WSN=0;
// 1 alokasi buffer per frame (tanpa Buffer.concat), mendukung payload >64KB
const frame=o=>{const s=typeof o==='string'?o:JSON.stringify(o),n=Buffer.byteLength(s);let h=2;if(n>65535)h=10;else if(n>125)h=4;
 const b=Buffer.allocUnsafe(h+n);b[0]=0x81;if(h===2)b[1]=n;else if(h===4){b[1]=126;b.writeUInt16BE(n,2)}else{b[1]=127;b.writeUInt32BE(0,2);b.writeUInt32BE(n,6)}b.write(s,h);return b};
const bframe=p=>{const n=p.length;let h=2;if(n>65535)h=10;else if(n>125)h=4;const b=Buffer.allocUnsafe(h+n);b[0]=0x82;if(h===2)b[1]=n;else if(h===4){b[1]=126;b.writeUInt16BE(n,2)}else{b[1]=127;b.writeUInt32BE(0,2);b.writeUInt32BE(n,6)}p.copy(b,h);return b};
// Snapshot biner v2: [type=1][u32 waktu server][u16 seq][u8 n] + n x [u32 id][i32 x][i32 y][i16 vx][i16 vy][i8 f][f32 ph]; entri diri sendiri dibuang
function snapFor(L,seq,self){let c=0;for(const e of L)if(e[0]!==self)c++;if(!c)return null;const b=Buffer.allocUnsafe(8+c*21);b[0]=1;b.writeUInt32BE(Date.now()>>>0,1);b.writeUInt16BE(seq,5);b[7]=c;let o=8;
 for(const e of L){if(e[0]===self)continue;b.writeUInt32BE(e[0]>>>0,o);b.writeInt32BE(e[1],o+4);b.writeInt32BE(e[2],o+8);b.writeInt16BE(e[3],o+12);b.writeInt16BE(e[4],o+14);b.writeInt8(e[5],o+16);b.writeFloatBE(e[6],o+17);o+=21}return bframe(b)}
// lossy: lewati bila antrean kirim klien menumpuk (posisi basi tak perlu dikirim). Non-lossy: putuskan klien yang macet >1MB agar memori aman.
const raw=(ws,f,lossy)=>{const s=ws.socket;if(ws.dead||!s||s.destroyed||!s.writable)return;try{const w=s.writableLength;if(lossy){if(w>16384)return}else if(w>1048576){ws.dead=1;s.destroy();return}s.write(f)}catch(e){ws.dead=1}};
const wsSend=(ws,o)=>raw(ws,frame(o));
const bcast=(r,o,lossy)=>{const f=frame(o);for(const p of r.ps.values())raw(p,f,lossy)};
const lobbyInfo=r=>({t:'lobby',code:r.code,left:r.dl?Math.max(0,Math.ceil((r.dl-Date.now())/1000)):null,ps:[...r.ps.values()].map(p=>({id:p.id,n:p.n,c:p.c}))});
function getRoom(){for(const r of ROOMS.values())if(r.st==='lobby'&&r.ps.size<MAXP)return r;const r={code:'SKLD-'+(1000+crypto.randomInt(9000)),st:'lobby',ps:new Map(),dl:0};ROOMS.set(r.code,r);return r}
const shuf=a=>{for(let i=a.length-1;i>0;i--){const j=crypto.randomInt(i+1);[a[i],a[j]]=[a[j],a[i]]}return a},IMP=x=>x.role==='imp'||x.role==='bj',BC=['#ff3b30','#3b82f6','#22c55e','#eab308','#a855f7','#f97316','#ec4899','#14b8a6','#94a3b8','#84cc16'];
function startRoom(r){if(r.st!=='lobby')return;r.st='play';const hs=[...r.ps.values()];r.sl=new Map();for(const p of hs)r.sl.set(p.id,{id:p.id,n:p.n,c:p.c,bot:0,ws:p});for(let i=1;r.sl.size<MAXP;i++)r.sl.set('b'+i,{id:'b'+i,n:'Bot'+i,c:BC[i%BC.length],bot:1});
 const ids=shuf([...r.sl.keys()]);ids.forEach((id,i)=>{const x=r.sl.get(id);x.role=i===0?'imp':i===1?(crypto.randomInt(100)<65?'bj':'imp'):i===2?'doc':i===3?'joker':'crew';Object.assign(x,{alive:1,bd:0,vent:0,em:0,tasks:0,cd:Date.now()+10000,rv:0,ut:0})});
 r.host=Math.min(...hs.map(p=>p.id));r.prog=0;r.sq=0;r.meet=null;r.goal=[...r.sl.values()].filter(x=>!IMP(x)&&x.role!=='joker').length*4;
 bcast(r,{t:'start',humans:hs.map(p=>({id:p.id,n:p.n,c:p.c})),bots:r.sl.size-hs.length,slots:[...r.sl.values()].map(x=>({id:x.id,n:x.n,c:x.c,bot:x.bot})),host:r.host,seed:crypto.randomInt(2**31),goal:r.goal});
 const imps=[...r.sl.values()].filter(IMP).map(x=>x.id);for(const p of hs)wsSend(p,{t:'role',role:r.sl.get(p.id).role,mates:IMP(r.sl.get(p.id))?imps:[]})}
function leave(ws){ws.dead=1;ALL.delete(ws);const r=ws.r;if(!r)return;ws.r=null;r.ps.delete(ws.id);
 if(r.st==='play'&&ws.v===2&&ws.tok&&r.sl&&r.sl.get(ws.id)){HELD.set(ws.tok,{tok:ws.tok,r,id:ws.id,until:Date.now()+HOLD_MS});(r.held||(r.held=new Set())).add(ws.tok)}
 if(!r.ps.size){if(!(r.held&&r.held.size))ROOMS.delete(r.code);return}if(r.st==='lobby'){bcast(r,lobbyInfo(r));return}
 const x=r.sl&&r.sl.get(ws.id);if(x){x.bot=1;x.ws=null}bcast(r,{t:'left',id:ws.id,bot:1});if(r.host===ws.id){r.host=Math.min(...r.ps.keys());bcast(r,{t:'host',id:r.host})}}
// Masuk kembali ke slot sendiri (v2 + token) selama RESUME_MS; slot dimainkan bot-host selama pemain putus
function resume(ws,h,x){const r=h.r;HELD.delete(h.tok);if(r.held)r.held.delete(h.tok);ws.v=2;ws.tok=h.tok;ws.id=h.id;ws.n=x.n;ws.c=x.c;ws.has=0;ws.dirty=0;ws.sent=0;ws.r=r;r.ps.set(ws.id,ws);x.bot=0;x.ws=ws;
 if(!r.ps.has(r.host))r.host=ws.id;
 wsSend(ws,{t:'hi',id:ws.id,v:2,tok:ws.tok,st:Date.now(),resumed:1});
 const imps=[...r.sl.values()].filter(IMP).map(q=>q.id);
 wsSend(ws,{t:'resume',role:x.role,mates:IMP(x)?imps:[],host:r.host,goal:r.goal,prog:r.prog,tasks:x.tasks,
  slots:[...r.sl.values()].map(q=>({id:q.id,n:q.n,c:q.c,bot:q.bot,alive:q.alive,bd:q.bd,x:q.x==null?null:Math.round(q.x),y:q.y==null?null:Math.round(q.y)})),
  meet:r.meet?{ms:Math.max(0,r.meet.end-Date.now()),voted:[...r.meet.votes.keys()]}:null});
 for(const p of r.ps.values())if(p!==ws&&p.v===2)wsSend(p,{t:'back',id:ws.id});
 if(r.host===ws.id)bcast(r,{t:'host',id:r.host})}
const fin=(v,m)=>Math.max(-m,Math.min(m,Number.isFinite(+v)?+v:0));
function onMsg(ws,txt){let m;try{m=JSON.parse(txt)}catch(e){return}if(!m||typeof m!=='object')return;
 if(m.t==='ping'){wsSend(ws,{t:'pong',c:+m.c||0,s:Date.now(),r:Math.round(ws.rtt||0)});return}
 if(onGame(ws,m))return;
 if(m.t==='join'&&m.v===2&&!ws.r&&typeof m.tok==='string'){const h=HELD.get(m.tok);if(h&&Date.now()<h.until&&h.r.st==='play'&&ROOMS.get(h.r.code)===h.r){const x=h.r.sl.get(h.id);if(x&&x.bot){resume(ws,h,x);return}}}
 // join juga boleh saat ronde sebelumnya sudah 'end' (tidak perlu menunggu 15 detik / reconnect)
 if(m.t==='join'&&(!ws.r||ws.r.st==='end')){if(ws.r){ws.r.ps.delete(ws.id);ws.r=null}
  const r=getRoom();ws.id=++PID;ws.n=String(m.n==null?'':m.n).replace(/[^\w\s.\-#]/g,'').trim().slice(0,12)||'Pemain'+ws.id;ws.c=/^#[0-9a-f]{6}$/i.test(m.c)?m.c:'#ff3b30';ws.has=0;ws.dirty=0;ws.sent=0;ws.r=r;ws.v=m.v===2?2:1;ws.tok=ws.v===2?crypto.randomBytes(12).toString('base64url'):'';r.ps.set(ws.id,ws);if(!r.dl)r.dl=Date.now()+FILL_MS;wsSend(ws,ws.v===2?{t:'hi',id:ws.id,v:2,tok:ws.tok,st:Date.now()}:{t:'hi',id:ws.id});bcast(r,lobbyInfo(r));if(r.ps.size>=MAXP)startRoom(r);return}
 if(m.t==='pos'&&ws.r&&ws.r.st==='play'){ws.x=fin(m.x,5e4);ws.y=fin(m.y,5e4);ws.vx=fin(m.vx,2e3);ws.vy=fin(m.vy,2e3);ws.f=m.f===-1?-1:1;ws.ph=fin(m.ph,1e5);ws.has=1;ws.dirty=1}}

// ===== Logika match (server-authoritative) =====
const rid=v=>typeof v==='number'?v:String(v==null?'':v).slice(0,8),dist=(a,b)=>a.x==null||b.x==null?0:Math.hypot(a.x-b.x,a.y-b.y);
function endRoom(r,w){if(r.st!=='play')return;r.st='end';bcast(r,{t:'end',w,roles:[...r.sl.values()].map(x=>[x.id,x.role])});setTimeout(()=>{for(const p of r.ps.values())if(p.r===r)p.r=null;ROOMS.delete(r.code)},15000)}
function chk(r){if(r.st!=='play')return;const L=[...r.sl.values()].filter(x=>x.alive),im=L.filter(IMP).length;if(!im)endRoom(r,1);else if(im>=L.length-im)endRoom(r,0);else if(r.prog>=r.goal)endRoom(r,1)}
function tally(r){const m=r.meet;if(!m)return;r.meet=null;const c=new Map();let sk=0;for(const[id,v]of m.votes){const x=r.sl.get(id);if(!x||!x.alive)continue;if(v==='skip'||!r.sl.get(v)||!r.sl.get(v).alive)sk++;else c.set(v,(c.get(v)||0)+1)}
 let top=null,mx=0,tie=0;for(const[id,n]of c){if(n>mx){mx=n;top=id;tie=0}else if(n===mx)tie=1}const ej=!tie&&top!=null&&mx>sk?r.sl.get(top):null;
 if(ej)ej.alive=0;for(const x of r.sl.values()){x.bd=0;x.cd=Date.now()+12000}
 bcast(r,{t:'res',ej:ej?ej.id:null,k:ej?(IMP(ej)?'imp':ej.role==='joker'?'joker':'crew'):null,votes:[...m.votes]});
 if(ej&&ej.role==='joker')endRoom(r,2);else chk(r)}
function openMeet(r,s,why,em){if(r.st!=='play'||r.meet||!s.alive)return;if(em){if(s.em)return;s.em=1}r.meet={votes:new Map(),bj:new Set(),end:Date.now()+MEET_MS};for(const x of r.sl.values())x.vent=0;bcast(r,{t:'meet',by:s.id,em:em?1:0,why:str(why,60),ms:MEET_MS})}
function onGame(ws,m){const r=ws.r;if(!r||r.st!=='play'||!r.sl)return false;const T=m.t;
 if(T==='bs'){if(ws.id!==r.host||!Array.isArray(m.p))return true;const out=[];for(const q of m.p.slice(0,12)){const x=r.sl.get(rid(q&&q[0]));if(x&&x.bot){x.x=fin(q[1],5e4);x.y=fin(q[2],5e4);out.push([x.id,x.x,x.y,fin(q[3],2e3),fin(q[4],2e3),q[5]===-1?-1:1,fin(q[6],1e5)])}}{const f=frame({t:'bs',p:out});for(const p of r.ps.values())if(p!==ws)raw(p,f,1)}return true}
 if(T!=='ev'&&T!=='chat'&&T!=='vote')return false;
 let s=r.sl.get(ws.id);if(m.a!=null&&ws.id===r.host){const b=r.sl.get(rid(m.a));if(b&&b.bot)s=b}if(!s||!s.alive)return true;if(!s.bot&&ws.has){s.x=ws.x;s.y=ws.y}
 const now=Date.now(),tg=r.sl.get(rid(m.v)),k=m.k;
 if(T==='chat'){if(r.meet&&now-(s.ct||0)>1500){s.ct=now;const x=str(m.x,80).trim();if(x)bcast(r,{t:'chat',a:s.id,x})}return true}
 if(T==='vote'){if(!r.meet)return true;r.meet.votes.set(s.id,m.v==='skip'?'skip':tg&&tg.alive?tg.id:'skip');bcast(r,{t:'voted',a:s.id});if([...r.sl.values()].filter(x=>x.alive).every(x=>r.meet.votes.has(x.id)))tally(r);return true}
 if(k==='kill'){if(r.meet||!IMP(s)||now<s.cd||!tg||!tg.alive||IMP(tg)||now<tg.ut||dist(s,tg)>320)return true;tg.alive=0;tg.bd=1;tg.x=tg.x==null?s.x:tg.x;s.cd=now+15000;bcast(r,{t:'ev',k:'kill',a:s.id,v:tg.id});chk(r)}
 else if(k==='vent'){if(r.meet||!(IMP(s)||s.role==='joker'))return true;s.vent=m.on?1:0;bcast(r,{t:'ev',k:'vent',a:s.id,on:s.vent})}
 else if(k==='revive'){if(r.meet||s.role!=='doc'||now<s.rv||!tg||tg.alive||!tg.bd)return true;tg.alive=1;tg.bd=0;s.rv=now+20000;s.ut=tg.ut=now+4000;bcast(r,{t:'ev',k:'revive',a:s.id,v:tg.id})}
 else if(k==='task'){if(r.meet||IMP(s)||s.role==='joker'||s.tasks>=4)return true;s.tasks++;r.prog++;bcast(r,{t:'prog',p:r.prog,g:r.goal});chk(r)}
 else if(k==='report'){if(!tg||tg.alive||!tg.bd)return true;openMeet(r,s,'Mayat '+tg.n+' dilaporkan',0)}
 else if(k==='emg'){openMeet(r,s,'RAPAT DARURAT',1)}
 else if(k==='bj'){const M=r.meet;if(!M||s.role!=='bj'||M.bj.has(s.id)||!tg||!tg.alive||IMP(tg)||!['crew','doc','joker'].includes(m.r))return true;M.bj.add(s.id);const ok=tg.role===m.r,x=ok?tg:s;x.alive=0;bcast(r,{t:'ev',k:'bj',a:s.id,v:tg.id,r:m.r,ok:ok?1:0});chk(r)}
 return true}

// ===== Parser frame WebSocket =====
const rl=ws=>{const now=Date.now();if(now-ws.mt>1000){ws.mt=now;ws.mc=0}if(++ws.mc>WS_MPS*4){ws.socket.destroy();return 2}return ws.mc>WS_MPS?1:0};
// posisi biner dari klien v2: [type=1][i32 x][i32 y][i16 vx][i16 vy][i8 f][f32 ph] = 18 byte
function onBin(ws,d){if(d.length>=18&&d[0]===1&&ws.r&&ws.r.st==='play'){ws.x=fin(d.readInt32BE(1),5e4);ws.y=fin(d.readInt32BE(5),5e4);ws.vx=fin(d.readInt16BE(9),2e3);ws.vy=fin(d.readInt16BE(11),2e3);ws.f=d.readInt8(13)===-1?-1:1;ws.ph=fin(d.readFloatBE(14),1e5);ws.has=1;ws.dirty=1}}
function wsParse(ws,chunk){ws.seen=Date.now();ws.buf=ws.buf.length?Buffer.concat([ws.buf,chunk]):chunk;
 for(;;){const b=ws.buf;if(b.length<2)return;const op=b[0]&15,mk=b[1]&128;let n=b[1]&127,o=2;if(n===126){if(b.length<4)return;n=b.readUInt16BE(2);o=4}else if(n===127){ws.socket.destroy();return}
  if(!mk||n>2048){ws.socket.destroy();return}if(b.length<o+4+n)return;
  const k0=b[o],k1=b[o+1],k2=b[o+2],k3=b[o+3],d=Buffer.allocUnsafe(n),p=o+4;for(let i=0;i<n;i++)d[i]=b[p+i]^(i&3?i&2?i&1?k3:k2:k1:k0);
  ws.buf=b.subarray(p+n);
  if(op===8){try{ws.socket.end(Buffer.from([0x88,0]))}catch(e){}return}
  if(op===9){if(n<=125){const f=Buffer.allocUnsafe(2+n);f[0]=0x8a;f[1]=n;d.copy(f,2);raw(ws,f)}continue}
  if(op===10){if(n===8){const rt=Date.now()-d.readDoubleBE(0);if(rt>=0&&rt<60000)ws.rtt=ws.rtt?ws.rtt*0.7+rt*0.3:rt}continue}
  if(op===1||op===2){const x=rl(ws);if(x===2)return;if(x)continue;if(op===1)onMsg(ws,d.toString('utf8'));else onBin(ws,d)}}}
SRV.on('upgrade',(q,socket,head)=>{socket.on('error',()=>{});const k=q.headers['sec-websocket-key'],ip=ipOf(q);
 const bad=c=>{try{socket.end('HTTP/1.1 '+c+'\r\nConnection: close\r\n\r\n')}catch(e){}setTimeout(()=>socket.destroy(),1000).unref()};
 if(q.url.split('?')[0]!=='/ws'||!k||String(q.headers.upgrade).toLowerCase()!=='websocket')return bad('400 Bad Request');
 if(ORIG&&!ORIG.includes(q.headers.origin))return bad('403 Forbidden');
 if(WSN>=MAXWS||(IPC.get(ip)||0)>=WS_IP)return bad('503 Service Unavailable');
 socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(k+GUID).digest('base64')+'\r\n\r\n');
 const ws={socket,ip,buf:Buffer.alloc(0),seen:Date.now(),mc:0,mt:0,v:1,rtt:0};ALL.add(ws);WSN++;IPC.set(ip,(IPC.get(ip)||0)+1);
 socket.setNoDelay(true);socket.setKeepAlive(true,30000);socket.setTimeout(0);
 socket.on('data',c=>wsParse(ws,c));socket.on('end',()=>socket.destroy());
 socket.on('close',()=>{WSN--;const n=(IPC.get(ip)||1)-1;if(n>0)IPC.set(ip,n);else IPC.delete(ip);leave(ws)});
 if(head&&head.length)wsParse(ws,head)});

// ===== Loop utama: timestep stabil (koreksi drift), waktu berbasis jam bukan hitungan tick =====
let lastLobby=0,lastPing=0,lastSweep=0;
function tick(){const now=Date.now(),lob=now-lastLobby>=1000,png=now-lastPing>=PING_MS,swp=now-lastSweep>=1000;if(lob)lastLobby=now;if(png)lastPing=now;if(swp)lastSweep=now;
 if(swp)for(const[t,h]of HELD)if(now>=h.until||ROOMS.get(h.r.code)!==h.r){HELD.delete(t);if(h.r.held)h.r.held.delete(t)}
 const pf=png?pingF():null;
 for(const r of ROOMS.values()){try{
  if(!r.ps.size){if(!(r.held&&r.held.size))ROOMS.delete(r.code);continue}
  if(r.st==='lobby'){if(r.dl&&now>=r.dl)startRoom(r);else if(lob)bcast(r,lobbyInfo(r))}
  else if(r.st==='play'){
   if(r.meet){if(now>=r.meet.end)tally(r)}
   else if(r.ps.size>1){const L=[];for(const p of r.ps.values()){if(!p.has)continue;const hb=now-(p.sent||0)>1000;if(!p.dirty&&!hb)continue;p.dirty=0;
    const x=Math.round(p.x),y=Math.round(p.y),vx=Math.round(p.vx),vy=Math.round(p.vy),ph=+p.ph.toFixed(2);
    if(!hb&&x===p.lx&&y===p.ly&&vx===p.lvx&&vy===p.lvy&&p.f===p.lfc&&ph===p.lph)continue; // tidak berubah: hemat bandwidth
    p.sent=now;p.lx=x;p.ly=y;p.lvx=vx;p.lvy=vy;p.lfc=p.f;p.lph=ph;L.push([p.id,x,y,vx,vy,p.f,ph])}
    if(L.length){r.sq=((r.sq|0)+1)&65535;let f1=null;for(const p of r.ps.values()){if(p.v===2){const fr=snapFor(L,r.sq,p.id);if(fr)raw(p,fr,1)}else{if(!f1)f1=frame({t:'s',p:L});raw(p,f1,1)}}}}}
  if(swp||png)for(const p of r.ps.values()){if(swp&&now-p.seen>DEAD_MS){p.socket.destroy();continue}if(png)raw(p,pf)}
 }catch(e){console.error('room',r.code,e)}}}
let nx=Date.now()+TICK;
(function loop(){try{tick()}catch(e){console.error('tick',e)}nx+=TICK;const now=Date.now();if(nx<now)nx=now+TICK;setTimeout(loop,Math.max(1,nx-now))})();
