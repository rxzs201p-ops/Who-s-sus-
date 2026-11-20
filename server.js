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
//   CF_SECRET=rahasia   bila diset: origin hanya menerima permintaan yang membawa header x-cf-secret (diset lewat Cloudflare Transform Rule) + IP asli dari cf-connecting-ip
//   WA_TOKEN, WA_PHONE_ID, WA_PROVIDER=meta|fonnte, WA_TO=601128762712, WA_FROM=601130863381, WA_TEMPLATE, WA_LANG  (amaran serangan ke WhatsApp admin)
//   SESSION_SECRET=rahsia-panjang (SAMA pada semua server; kalau kosong diturunkan dari DATABASE_URL), TERMS_VER, ACC_PER_PHONE=2, WA_OTP_TEMPLATE (kod pengesahan Meta), DEV_OTP=1 (UJI sahaja)
//   ATK_ABUSE=120 (abuse/30dtk = serangan), BUSY_LOAD=75 (beban % = sesak), BAN_AT=40, BAN_MS=600000 (blok IP abusif)
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

const STALE_MS=+E.STALE_MS||45000; // heartbeat lebih lama dari ini = server dianggap offline (degraded bila > 2/3 nilai ini)
let srvC={t:0,v:null};
const LEASE_MS=+E.LEASE_MS||20000,RENEW_MS=Math.max(2000,Math.floor(LEASE_MS/3)); // lease bilik di Neon: satu server pemilik; diperbarui tiap ~LEASE_MS/3
let DOWN=0;
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
async function initNeon(){if(!POOL)return;await POOL.query('SELECT 1');await ensureSchema();await accSchema().catch(e=>console.error('accSchema:',e.message));await POOL.query("ALTER TABLE whos_sus.server_nodes ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'ok'").catch(e=>console.error('mode col:',e.message));
 const pr=await POOL.query('SELECT player_id,profile_data FROM whos_sus.players ORDER BY updated_at DESC LIMIT $1',[MAXPROF]);
 data.profiles={};for(const row of pr.rows)data.profiles[row.player_id]=row.profile_data||{};
 const ch=await POOL.query('SELECT player_id,username,message,(extract(epoch from created_at)*1000)::bigint AS ts FROM whos_sus.chat_messages ORDER BY created_at DESC LIMIT 200');
 data.chat=ch.rows.reverse().map(x=>({n:x.username,u:x.player_id||'',x:x.message,t:Number(x.ts)}));chatC=null;
 await POOL.query(`INSERT INTO whos_sus.server_nodes(server_id,server_url,status,active_rooms,load_score,last_heartbeat,updated_at) VALUES($1,$2,'online',0,0,NOW(),NOW()) ON CONFLICT(server_id) DO UPDATE SET server_url=EXCLUDED.server_url,status='online',last_heartbeat=NOW(),updated_at=NOW()`,[SERVER_ID,SERVER_URL]);
 console.log('Neon connected · profiles '+pr.rowCount+' · chat '+ch.rowCount);
}
for(const s of['SIGINT','SIGTERM'])process.on(s,()=>{DOWN=1;flush();for(const r of ROOMS.values())for(const p of r.ps.values())try{p.socket.write(Buffer.from([0x88,2,0x03,0xe9]))}catch(e){}setTimeout(()=>process.exit(0),1500)});

// ===== Rate limit =====
const mkLim=max=>{const h=new Map();setInterval(()=>{const n=Date.now();for(const[k,a]of h)if(!a.length||n-a[a.length-1]>=60000)h.delete(k)},60000).unref();
 return ip=>{const n=Date.now();let a=h.get(ip);if(!a)h.set(ip,a=[]);while(a.length&&n-a[0]>=60000)a.shift();if(a.length>=max){abuse(ip);return false}a.push(n);return true}};
const limAI=mkLim(RPM),limApi=mkLim(+E.API_RPM||240),limChat=mkLim(40);
let day=new Date().toDateString(),used=0;
const CF_SECRET=E.CF_SECRET||'',cfOk=q=>!CF_SECRET||q.headers['x-cf-secret']===CF_SECRET;
const ipOf=q=>String((CF_SECRET&&cfOk(q)&&q.headers['cf-connecting-ip'])||q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim();
// ===== Bedakan SESAK (trafik sah banyak) vs SERANGAN (banyak permintaan abusif) =====
// abusif = kena rate-limit, handshake rusak, melebihi batas per-IP, spam pesan. sesak = kapasitas penuh / beban tinggi tanpa abuse.
const ATK_ABUSE=+E.ATK_ABUSE||120,BUSY_LOAD=+E.BUSY_LOAD||75,BAN_MS=+E.BAN_MS||600000,BAN_AT=+E.BAN_AT||40;
const TH={abuse:0,full:0,w:[],f:[],ips:new Map(),bans:new Map(),mode:'ok',until:0};
function abuse(ip,n){n=n||1;TH.abuse+=n;const c=(TH.ips.get(ip)||0)+n;TH.ips.set(ip,c);if(c>=BAN_AT&&!TH.bans.has(ip))TH.bans.set(ip,Date.now()+BAN_MS)}
const banned=ip=>{const t=TH.bans.get(ip);if(!t)return false;if(t<Date.now()){TH.bans.delete(ip);return false}return true};
// ===== Amaran serangan ke admin lewat WhatsApp =====
// Pengirim ialah nombor yang didaftarkan pada penyedia (WA_FROM hanya catatan). Dua pilihan:
//  1) Meta WhatsApp Cloud API: WA_TOKEN + WA_PHONE_ID (+ WA_TEMPLATE & WA_LANG bila perlu template untuk mesej proaktif)
//  2) Fonnte (WA_PROVIDER=fonnte): WA_TOKEN = token peranti Fonnte
// Tanpa konfigurasi, amaran hanya ditulis ke log Render.
const WA_TO=String(E.WA_TO||'601128762712').replace(/\D/g,''),WA_FROM=String(E.WA_FROM||'601130863381').replace(/\D/g,''),WA_TOKEN=E.WA_TOKEN||'',WA_PHONE=E.WA_PHONE_ID||'',WA_PROV=(E.WA_PROVIDER||'meta').toLowerCase(),WA_TPL=E.WA_TEMPLATE||'',WA_LANG=E.WA_LANG||'ms';
const waLast={};
const waReady=()=>!!WA_TOKEN&&(WA_PROV==='fonnte'||!!WA_PHONE)&&typeof fetch==='function';
// tpl: undefined = pakai WA_TEMPLATE; '' = mesej teks; param = isi pembolehubah {{1}} template
async function sendWA(to,msg,tpl,param){if(!waReady())return false;
 try{let r;
  if(WA_PROV==='fonnte')r=await fetch('https://api.fonnte.com/send',{method:'POST',headers:{Authorization:WA_TOKEN,'content-type':'application/x-www-form-urlencoded'},body:new URLSearchParams({target:to,message:msg,countryCode:'60',typing:'true',delay:'2'}).toString()});
  else{const T=tpl===undefined?WA_TPL:tpl,P=String(param!=null?param:msg).replace(/\s*\n\s*/g,' | ').slice(0,900);
   r=await fetch('https://graph.facebook.com/v21.0/'+encodeURIComponent(WA_PHONE)+'/messages',{method:'POST',headers:{Authorization:'Bearer '+WA_TOKEN,'content-type':'application/json'},body:JSON.stringify(T?{messaging_product:'whatsapp',to,type:'template',template:{name:T,language:{code:WA_LANG},components:[{type:'body',parameters:[{type:'text',text:P}]}]}}:{messaging_product:'whatsapp',to,type:'text',text:{body:msg}})})}
  const tx=await r.text();let j={};try{j=JSON.parse(tx)}catch(e){}console.log('WA resp:',WA_PROV,r.status,tx.slice(0,300));
  if(!r.ok||j.status===false){console.error('WA gagal:',r.status,tx.slice(0,300));return false}return true}
 catch(e){console.error('WA error:',e.message);return false}}
async function notifyAdmin(kind,text,cool){const now=Date.now();if(waLast[kind]&&now-waLast[kind]<(cool==null?300000:cool))return;waLast[kind]=now;
 const msg='['+SERVER_ID+'] '+text;console.log('[AMARAN-ADMIN]',msg);await sendWA(WA_TO,msg)}
function alertMode(from,to,a,f){
 if(to==='attack')notifyAdmin('attack','🛡️ SERANGAN dikesan\nServer: '+SERVER_URL+'\nPermintaan abusif (30s): '+a+'\nBeban: '+curLoad()+'%\nPemain online: '+WSN+'\nIP disekat: '+TH.bans.size+'\nSistem: pengetatan had IP + pemindahan match ke server lain.',300000);
 else if(from==='attack')notifyAdmin('recover','✅ Serangan reda. Mod server: '+to+'\nServer: '+SERVER_URL+'\nBeban: '+curLoad()+'%',0)}
const curLoad=()=>Math.min(100,Math.round(WSN/MAXWS*100+EL.percentile(99)/1e6));
setInterval(()=>{TH.w.push(TH.abuse);TH.f.push(TH.full);TH.abuse=TH.full=0;if(TH.w.length>6){TH.w.shift();TH.f.shift()}
 const a=TH.w.reduce((x,y)=>x+y,0),f=TH.f.reduce((x,y)=>x+y,0),now=Date.now();let m='ok';
 if(a>=ATK_ABUSE){TH.until=now+120000;m='attack'}else if(TH.until>now)m='attack';else if(curLoad()>=BUSY_LOAD||f>0)m='busy';
 if(Math.random()<0.1)TH.ips.clear();
 if(m!==TH.mode){console.log('Mode server:',TH.mode,'->',m,'(abuse30s='+a+', penuh30s='+f+', load='+curLoad()+')');alertMode(TH.mode,m,a,f);TH.mode=m;
  for(const w of ALL)try{wsSend(w,{t:'srvmode',mode:m})}catch(e){}}
 if(m!=='ok')handoff(m).catch(e=>console.error('handoff:',e.message));
},5000).unref();
// Pindahkan match yang sedang berjalan ke server lain (server asal masih hidup tapi sesak/diserang).
// Alur: simpan snapshot terakhir -> lepas lease di Neon -> suruh pemain redirect -> server tujuan adoptRoom + dbResume (token). Lobby tidak dipindah.
let hoT=0;
async function handoff(why){
 if(!POOL||DOWN||Date.now()-hoT<120000)return;
 let rooms=[...ROOMS.values()].filter(r=>!r.lost&&r.st==='play');if(!rooms.length)return;
 const q=await POOL.query(`SELECT server_id,server_url,load_score FROM whos_sus.server_nodes WHERE server_id<>$1 AND status='online' AND COALESCE(mode,'ok')='ok' AND last_heartbeat>NOW()-($2::int*INTERVAL '1 millisecond') ORDER BY load_score ASC NULLS LAST LIMIT 1`,[SERVER_ID,STALE_MS]);
 const t=q.rows[0];if(!t||!t.server_url)return;
 if(why==='busy'){if((+t.load_score||0)>curLoad()-20)return;rooms=rooms.slice(0,Math.ceil(rooms.length/2))} // sesak: pindah separuh saja agar tidak membanjiri server tujuan
 hoT=Date.now();
 for(const r of rooms){try{await persistRoom(r)}catch(e){}}
 await POOL.query(`UPDATE whos_sus.game_rooms SET lease_until=NOW(),updated_at=NOW() WHERE server_id=$1 AND status<>'ended' AND room_id=ANY($2::text[])`,[SERVER_ID,rooms.map(r=>r.code)]);
 let n=0;for(const r of rooms){r.lost=1;ROOMS.delete(r.code);
  for(const p of[...r.ps.values()]){n++;try{wsSend(p,{t:'redirect',url:t.server_url,status:'online',wait:400,why})}catch(e){}setTimeout(()=>{try{p.socket.end(Buffer.from([0x88,0]))}catch(e){}},400)}}
 console.log('Handoff ('+why+'): '+rooms.length+' bilik, '+n+' pemain -> '+t.server_id);notifyAdmin('handoff','🔄 '+rooms.length+' match ('+n+' pemain) dipindah ke '+t.server_id+' kerana '+(why==='attack'?'serangan':'trafik sesak')+'.',60000)}

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
const SPG={};
function staticPage(r,name){try{const f=path.join(__dirname,name),st=fs.statSync(f);let c=SPG[name];if(!c||c.m!==st.mtimeMs)c=SPG[name]={m:st.mtimeMs,b:fs.readFileSync(f)};
 r.writeHead(200,{'content-type':'text/html;charset=utf-8','cache-control':'no-cache','x-content-type-options':'nosniff','referrer-policy':'no-referrer','content-length':c.b.length});r.end(c.b)}catch(e){send(r,404,{})}}
// ===== Router HTTP =====
// ===== Akaun Project 41: daftar (kod WhatsApp) + login ID 6 digit =====
// Profil akaun disimpan sebagai 'a<ID6>' dan hanya boleh ditulis dengan token sesi. Token ditandatangani HMAC (SESSION_SECRET sama pada semua server).
const SECRET=E.SESSION_SECRET||crypto.createHash('sha256').update('p41|'+(E.DATABASE_URL||'dev')).digest('hex'),TERMS_VER=E.TERMS_VER||'2026-11-20',ACC_PER_PHONE=+E.ACC_PER_PHONE||2,DEV_OTP=E.DEV_OTP==='1';
const hmac=x=>crypto.createHmac('sha256',SECRET).update(x).digest('base64url'),sha2=x=>crypto.createHash('sha256').update(x).digest('hex');
const mkTok=id=>{const p=id+'.'+(Date.now()+30*864e5);return p+'.'+hmac(p)};
const chkTok=t=>{const a=String(t||'').split('.');if(a.length!==3||!/^\d{6}$/.test(a[0])||!(+a[1]>Date.now()))return null;const x=Buffer.from(hmac(a[0]+'.'+a[1])),y=Buffer.from(a[2]);return x.length===y.length&&crypto.timingSafeEqual(x,y)?a[0]:null};
const bearer=q=>String(q.headers.authorization||'').replace(/^Bearer\s+/i,'');
const scr=(pw,salt)=>new Promise((ok,no)=>crypto.scrypt(pw,salt,32,{N:16384,r:8,p:1},(e,k)=>e?no(e):ok(k)));
async function hashPw(pw){const sl=crypto.randomBytes(16);return 's1$'+sl.toString('base64url')+'$'+(await scr(pw,sl)).toString('base64url')}
async function checkPw(pw,h){const a=String(h).split('$');if(a[0]!=='s1')return false;const k=await scr(pw,Buffer.from(a[1],'base64url')),y=Buffer.from(a[2],'base64url');return k.length===y.length&&crypto.timingSafeEqual(k,y)}
const DUMMY_H='s1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
const ACC=new Map(),OTPS=new Map(),PHT=new Map(),LK=new Map(); // ACC/OTPS = cadangan tanpa Neon; PHT = had hantar kod; LK = kunci login
const limReg=mkLim(8),limLogin=mkLim(12),limVer=mkLim(15);
async function accSchema(){if(!POOL)return;
 await POOL.query(`CREATE TABLE IF NOT EXISTS whos_sus.accounts(acc_id TEXT PRIMARY KEY,username TEXT NOT NULL,pass_hash TEXT NOT NULL,phone_hash TEXT NOT NULL,phone_tail TEXT,terms_ver TEXT,terms_at TIMESTAMPTZ DEFAULT NOW(),created_at TIMESTAMPTZ DEFAULT NOW(),banned BOOLEAN DEFAULT FALSE)`);
 await POOL.query('CREATE UNIQUE INDEX IF NOT EXISTS ux_acc_user ON whos_sus.accounts(lower(username))');
 await POOL.query('CREATE INDEX IF NOT EXISTS ix_acc_phone ON whos_sus.accounts(phone_hash)');
 await POOL.query(`CREATE TABLE IF NOT EXISTS whos_sus.reg_otps(rid TEXT PRIMARY KEY,phone_hash TEXT,username TEXT,pass_hash TEXT,code_hash TEXT,tries INT DEFAULT 0,tail TEXT,terms_ver TEXT,expires_at TIMESTAMPTZ)`)}
const accUserTaken=async n=>POOL?(await POOL.query('SELECT 1 FROM whos_sus.accounts WHERE lower(username)=lower($1)',[n])).rowCount>0:[...ACC.values()].some(a=>a.username.toLowerCase()===n.toLowerCase());
const accPhoneCount=async h=>POOL?+(await POOL.query('SELECT count(*)::int AS c FROM whos_sus.accounts WHERE phone_hash=$1',[h])).rows[0].c:[...ACC.values()].filter(a=>a.phone_hash===h).length;
async function accGet(id){if(!POOL)return ACC.get(id)||null;const q=await POOL.query('SELECT acc_id AS id,username,pass_hash,banned FROM whos_sus.accounts WHERE acc_id=$1',[id]);return q.rows[0]||null}
async function accCreate(a){if(!POOL){if(ACC.has(a.id))return 'dup_id';if([...ACC.values()].some(x=>x.username.toLowerCase()===a.username.toLowerCase()))return 'dup_user';ACC.set(a.id,a);return 'ok'}
 try{await POOL.query('INSERT INTO whos_sus.accounts(acc_id,username,pass_hash,phone_hash,phone_tail,terms_ver) VALUES($1,$2,$3,$4,$5,$6)',[a.id,a.username,a.pass_hash,a.phone_hash,a.tail,a.tv]);return 'ok'}
 catch(e){if(e.code==='23505')return /user/.test(e.constraint||'')?'dup_user':'dup_id';throw e}}
async function otpPut(o){if(!POOL){OTPS.set(o.rid,o);return}await POOL.query(`INSERT INTO whos_sus.reg_otps(rid,phone_hash,username,pass_hash,code_hash,tries,tail,terms_ver,expires_at) VALUES($1,$2,$3,$4,$5,0,$6,$7,to_timestamp($8/1000.0))`,[o.rid,o.ph,o.username,o.pass_hash,o.code_hash,o.tail,o.tv,o.exp])}
async function otpGet(rid){if(!POOL)return OTPS.get(rid)||null;const q=await POOL.query('SELECT rid,phone_hash AS ph,username,pass_hash,code_hash,tries,tail,terms_ver AS tv,(extract(epoch from expires_at)*1000)::bigint AS exp FROM whos_sus.reg_otps WHERE rid=$1',[rid]);const o=q.rows[0];if(o)o.exp=+o.exp;return o||null}
const otpDel=async rid=>{if(!POOL)OTPS.delete(rid);else await POOL.query('DELETE FROM whos_sus.reg_otps WHERE rid=$1',[rid]).catch(()=>{})};
const otpTry=async o=>{o.tries++;if(POOL)await POOL.query('UPDATE whos_sus.reg_otps SET tries=tries+1 WHERE rid=$1',[o.rid]).catch(()=>{})};
setInterval(()=>{const n=Date.now();for(const[k,v]of OTPS)if(v.exp<n)OTPS.delete(k);for(const[k,v]of PHT)if(!v.length||n-v[v.length-1]>36e5)PHT.delete(k);for(const[k,v]of LK)if(n-v.t>6e5)LK.delete(k);if(POOL)POOL.query('DELETE FROM whos_sus.reg_otps WHERE expires_at<NOW()').catch(()=>{})},60000).unref();
const normPhone=p=>{let d=String(p||'').replace(/\D/g,'');if(d.startsWith('00'))d=d.slice(2);else if(d.startsWith('0'))d='60'+d.slice(1);return /^\d{9,15}$/.test(d)?d:''};
const err=(r,c,m,x)=>send(r,c,Object.assign({error:m},x||{}));
async function authRoute(q,r,u,M){
 if(u==='/api/auth/register'&&M==='POST'){
  if(!limReg(ipOf(q)))return err(r,429,'Terlalu banyak percubaan. Cuba lagi sebentar.');
  const b=await body(q),user=String(b.username||'').trim(),pw=String(b.password||''),ph=normPhone(b.phone);
  if(b.agree!==true||String(b.tv)!==TERMS_VER)return err(r,400,'Anda mesti membaca dan bersetuju dengan Terma & Syarat dan Dasar Perkhidmatan.');
  if(!/^[A-Za-z0-9_]{3,12}$/.test(user))return err(r,400,'Username 3-12 aksara: huruf, nombor atau _ sahaja.');
  if(pw.length<8||pw.length>64||pw.toLowerCase()===user.toLowerCase())return err(r,400,'Kata laluan mesti 8-64 aksara dan tidak sama dengan username.');
  if(!ph)return err(r,400,'Nombor WhatsApp tidak sah. Gunakan format dengan kod negara, cth 60123456789.');
  if(!waReady()&&!DEV_OTP)return err(r,503,'Perkhidmatan kod pengesahan belum tersedia. Cuba lagi nanti.');
  if(await accUserTaken(user))return err(r,409,'Username sudah digunakan.');
  const phh=hmac('ph|'+ph);if(await accPhoneCount(phh)>=ACC_PER_PHONE)return err(r,409,'Nombor ini sudah mencapai had akaun.');
  const now=Date.now(),h=PHT.get(phh)||[];while(h.length&&now-h[0]>36e5)h.shift();
  if(h.length&&now-h[h.length-1]<60000)return err(r,429,'Tunggu sebentar sebelum meminta kod baharu.',{wait:Math.ceil((60000-(now-h[h.length-1]))/1000)});
  if(h.length>=5)return err(r,429,'Had permintaan kod sejam telah dicapai.');
  h.push(now);PHT.set(phh,h);
  const code=String(crypto.randomInt(100000,1000000)),rid=crypto.randomBytes(12).toString('base64url');
  await otpPut({rid,ph:phh,username:user,pass_hash:await hashPw(pw),code_hash:sha2(code+rid),tries:0,tail:ph.slice(-4),tv:TERMS_VER,exp:now+300000});
  const ok=await sendWA(ph,'Hai! Kod untuk daftar akaun Project 41 anda ialah '+code+' (sah 5 minit). Jangan kongsi dengan sesiapa.',E.WA_OTP_TEMPLATE||'',code);
  if(!ok&&!DEV_OTP){await otpDel(rid);return err(r,502,'Gagal menghantar kod WhatsApp. Semak nombor anda dan cuba lagi.')}
  if(DEV_OTP)console.log('[DEV_OTP]',user,code);
  return send(r,200,Object.assign({ok:1,rid,tail:ph.slice(-4),wait:60},DEV_OTP?{dev:code}:{}))}
 if(u==='/api/auth/verify'&&M==='POST'){
  if(!limVer(ipOf(q)))return err(r,429,'Terlalu banyak percubaan. Cuba lagi sebentar.');
  const b=await body(q),rid=String(b.rid||''),code=String(b.code||'').replace(/\D/g,''),o=await otpGet(rid);
  if(!o||o.exp<Date.now()){if(o)await otpDel(rid);return err(r,410,'Kod tamat tempoh. Minta kod baharu.')}
  if(o.tries>=5){await otpDel(rid);return err(r,429,'Terlalu banyak kod salah. Minta kod baharu.')}
  await otpTry(o);const x=Buffer.from(sha2(code+rid)),y=Buffer.from(o.code_hash);
  if(code.length!==6||x.length!==y.length||!crypto.timingSafeEqual(x,y))return err(r,400,'Kod salah.',{left:Math.max(0,5-o.tries)});
  let id='',res='';for(let i=0;i<10;i++){id=String(crypto.randomInt(100000,1000000));res=await accCreate({id,username:o.username,pass_hash:o.pass_hash,phone_hash:o.ph,tail:o.tail,tv:o.tv});if(res!=='dup_id')break}
  if(res==='dup_user'){await otpDel(rid);return err(r,409,'Username baru sahaja diambil orang lain. Pilih username lain.')}
  if(res!=='ok')return err(r,503,'Gagal mencipta akaun. Cuba lagi.');
  await otpDel(rid);return send(r,200,{ok:1,id,name:o.username,token:mkTok(id)})}
 if(u==='/api/auth/login'&&M==='POST'){
  if(!limLogin(ipOf(q)))return err(r,429,'Terlalu banyak percubaan. Cuba lagi sebentar.');
  const b=await body(q),id=String(b.id||'').trim(),pw=String(b.password||'').slice(0,64),lk=LK.get(id)||{n:0,t:0};
  if(!/^\d{6}$/.test(id))return err(r,400,'ID mesti 6 nombor.');
  if(lk.n>=5&&Date.now()-lk.t<6e5)return err(r,429,'Akaun dikunci sementara kerana terlalu banyak percubaan. Cuba lagi dalam 10 minit.');
  const a=await accGet(id);let ok=false;if(a&&!a.banned)ok=await checkPw(pw,a.pass_hash);else await checkPw(pw,DUMMY_H).catch(()=>{});
  if(!ok){lk.n=(Date.now()-lk.t<6e5?lk.n:0)+1;lk.t=Date.now();LK.set(id,lk);return err(r,401,'ID atau kata laluan salah.')}
  LK.delete(id);return send(r,200,{ok:1,id,name:a.username,token:mkTok(id)})}
 if(u==='/api/auth/me'&&M==='GET'){const id=chkTok(bearer(q));if(!id)return err(r,401,'Sesi tamat.');const a=await accGet(id);if(!a||a.banned)return err(r,401,'Akaun tidak sah.');return send(r,200,{ok:1,id,name:a.username})}
 return false}
async function route(q,r){const u=q.url.split('?')[0],M=q.method;
 if(!cfOk(q)&&u!=='/api/ping')return send(r,403,{error:'forbidden'}); // origin hanya lewat Cloudflare bila CF_SECRET diset
 if(banned(ipOf(q)))return send(r,429,{error:'diblokir sementara'});
 if(u.startsWith('/api/auth/')){const x=await authRoute(q,r,u,M);if(x!==false)return x}
 if(u==='/daftar'||u==='/daftar.html')return staticPage(r,'daftar.html');
 if(u==='/api/ping'||u==='/api/servers')r.setHeader('access-control-allow-origin','*'); // klien mengukur latency lintas-server
 if(u==='/api/ping')return send(r,200,{ok:1,ai:READY,gap:Math.max(3,+E.AI_GAP||10),t:Date.now(),mode:TH.mode,load:curLoad()});
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
 if(u==='/api/servers'&&M==='GET'){if(!limApi(ipOf(q)))return send(r,429,{error:'terlalu cepat'});if(!POOL)return send(r,200,{self:SERVER_ID,servers:[]});
  if(!srvC.v||Date.now()-srvC.t>2000){const rs=await POOL.query(`SELECT server_id,server_url,status,active_rooms,load_score,latency_ms,COALESCE(mode,'ok') AS mode FROM whos_sus.server_nodes WHERE status IN ('online','degraded') AND last_heartbeat>NOW()-($1::int*INTERVAL '1 millisecond') ORDER BY (status='online') DESC,load_score ASC NULLS LAST,latency_ms ASC NULLS LAST`,[STALE_MS]);srvC={t:Date.now(),v:rs.rows}}
  return send(r,200,{self:SERVER_ID,servers:srvC.v})}
 if(u==='/api/guest'&&M==='POST'){if(!limApi(ipOf(q)))return send(r,429,{error:'terlalu cepat'});data.guests=(data.guests||0)+1;dirty=true;return send(r,200,{n:data.guests})}
 if(u==='/api/chat'&&M==='GET')return sendC(q,r,chatV());
 if(u==='/api/chat'&&M==='POST'){if(!limChat(ipOf(q)))return send(r,429,{error:'terlalu cepat'});
  const b=await body(q),x=str(b.x,60).trim(),uid=str(b.u,24).replace(/[^\w-]/g,'');if(!x)return send(r,400,{});
  const entry={n:str(b.n,12),u:uid,x,t:Date.now()};data.chat.push(entry);if(data.chat.length>200)data.chat.splice(0,data.chat.length-200);if(POOL)pendingChat.push(entry);chatC=null;dirty=true;return send(r,200,{ok:1})}
 if(u==='/api/profiles'&&M==='GET')return sendC(q,r,profList());
 if(u==='/api/leaderboard'&&M==='GET')return sendC(q,r,board());
 const m=u.match(/^\/api\/profiles\/([\w-]{1,24})$/);
 if(m&&M==='PUT'){if(!limApi(ipOf(q)))return send(r,429,{error:'terlalu cepat'});
  if(/^a\d{6}$/.test(m[1])){const aid=chkTok(bearer(q));if(!aid||'a'+aid!==m[1])return send(r,401,{error:'login diperlukan'})}
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
if(POOL){let hbLat=0;
 async function heartbeat(){
  const t0=Date.now(),load=Math.min(100,Math.round(WSN/MAXWS*100+EL.percentile(99)/1e6));
  await POOL.query(`UPDATE whos_sus.server_nodes SET status='online',active_rooms=$2,load_score=$3,latency_ms=$4,mode=$5,last_heartbeat=NOW(),updated_at=NOW() WHERE server_id=$1`,[SERVER_ID,ROOMS.size,load,hbLat||null,TH.mode]);
  hbLat=Date.now()-t0;
  await POOL.query(`UPDATE whos_sus.server_nodes SET status='offline',updated_at=NOW() WHERE server_id<>$1 AND status<>'offline' AND last_heartbeat<NOW()-($2::int*INTERVAL '1 millisecond')`,[SERVER_ID,STALE_MS]);
  await POOL.query(`UPDATE whos_sus.server_nodes SET status='degraded',updated_at=NOW() WHERE server_id<>$1 AND status='online' AND last_heartbeat<NOW()-($2::int*INTERVAL '1 millisecond')`,[SERVER_ID,Math.round(STALE_MS*2/3)])}
 const hb=setInterval(()=>{heartbeat().catch(e=>console.error('Heartbeat error:',e.message))},15000);hb.unref();for(const sig of ['SIGINT','SIGTERM'])process.once(sig,()=>{POOL.query(`UPDATE whos_sus.game_rooms SET lease_until=NOW(),updated_at=NOW() WHERE server_id=$1 AND status<>'ended'`,[SERVER_ID]).catch(()=>{}).then(()=>POOL.query(`UPDATE whos_sus.server_nodes SET status='offline',last_heartbeat=NOW(),updated_at=NOW() WHERE server_id=$1`,[SERVER_ID])).catch(()=>{}).finally(()=>POOL.end().catch(()=>{}))})}


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
function getRoom(){for(const r of ROOMS.values())if(r.st==='lobby'&&r.ps.size<MAXP)return r;const r={code:'SKLD-'+crypto.randomBytes(4).toString('hex').toUpperCase(),st:'lobby',ps:new Map(),dl:0,nd:1,pt:0};ROOMS.set(r.code,r);return r}
const shuf=a=>{for(let i=a.length-1;i>0;i--){const j=crypto.randomInt(i+1);[a[i],a[j]]=[a[j],a[i]]}return a},IMP=x=>x.role==='imp'||x.role==='bj',BC=['#ff3b30','#3b82f6','#22c55e','#eab308','#a855f7','#f97316','#ec4899','#14b8a6','#94a3b8','#84cc16'];
function startRoom(r){if(r.st!=='lobby')return;r.st='play';r.nd=1;const hs=[...r.ps.values()];r.sl=new Map();for(const p of hs)r.sl.set(p.id,{id:p.id,n:p.n,c:p.c,bot:0,ws:p});for(let i=1;r.sl.size<MAXP;i++)r.sl.set('b'+i,{id:'b'+i,n:'Bot'+i,c:BC[i%BC.length],bot:1});
 const ids=shuf([...r.sl.keys()]);ids.forEach((id,i)=>{const x=r.sl.get(id);x.role=i===0?'imp':i===1?(crypto.randomInt(100)<65?'bj':'imp'):i===2?'doc':i===3?'joker':'crew';Object.assign(x,{alive:1,bd:0,vent:0,em:0,tasks:0,cd:Date.now()+10000,rv:0,ut:0})});
 r.host=Math.min(...hs.map(p=>p.id));r.prog=0;r.sq=0;r.meet=null;r.goal=[...r.sl.values()].filter(x=>!IMP(x)&&x.role!=='joker').length*4;
 bcast(r,{t:'start',humans:hs.map(p=>({id:p.id,n:p.n,c:p.c})),bots:r.sl.size-hs.length,slots:[...r.sl.values()].map(x=>({id:x.id,n:x.n,c:x.c,bot:x.bot})),host:r.host,seed:crypto.randomInt(2**31),goal:r.goal});
 const imps=[...r.sl.values()].filter(IMP).map(x=>x.id);for(const p of hs)wsSend(p,{t:'role',role:r.sl.get(p.id).role,mates:IMP(r.sl.get(p.id))?imps:[]})}
function leave(ws){ws.dead=1;ALL.delete(ws);const r=ws.r;if(!r)return;ws.r=null;r.ps.delete(ws.id);r.nd=1;
 if(r.st==='play'&&ws.v===2&&ws.tok&&r.sl&&r.sl.get(ws.id)){HELD.set(ws.tok,{tok:ws.tok,r,id:ws.id,until:Date.now()+HOLD_MS});(r.held||(r.held=new Set())).add(ws.tok)}
 if(r.st==='play'&&r.sl){const x0=r.sl.get(ws.id);if(x0){x0.bot=1;x0.ws=null}} // tandai slot sebagai bot walau ini pemain terakhir, agar resume diterima
 if(!r.ps.size){if(!(r.held&&r.held.size))closeRoom(r);return}if(r.st==='lobby'){bcast(r,lobbyInfo(r));return}
 const x=r.sl&&r.sl.get(ws.id);if(x){x.bot=1;x.ws=null}bcast(r,{t:'left',id:ws.id,bot:1});if(r.host===ws.id){r.host=Math.min(...r.ps.keys());bcast(r,{t:'host',id:r.host})}}
// Masuk kembali ke slot sendiri (v2 + token) selama RESUME_MS; slot dimainkan bot-host selama pemain putus
function resume(ws,h,x){const r=h.r;r.nd=1;HELD.delete(h.tok);if(r.held)r.held.delete(h.tok);ws.v=2;ws.tok=h.tok;ws.id=h.id;ws.n=x.n;ws.c=x.c;ws.has=0;ws.dirty=0;ws.sent=0;ws.r=r;r.ps.set(ws.id,ws);x.bot=0;x.ws=ws;
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
 if(ws.pend)return;
 if(m.t==='join'&&m.v===2&&!ws.r&&typeof m.tok==='string'){const h=HELD.get(m.tok);if(h&&Date.now()<h.until&&h.r.st==='play'&&ROOMS.get(h.r.code)===h.r){const x=h.r.sl.get(h.id);if(x&&x.bot){resume(ws,h,x);return}}
  // tidak ada di memori server ini: cari sesi di Neon (mungkin bilik milik server lain / server sebelumnya yang gagal)
  if(POOL){ws.pend=1;dbResume(ws,m).catch(e=>{console.error('dbResume:',e.message);return false}).then(ok=>{ws.pend=0;if(!ok&&!ws.dead&&!ws.r)joinNew(ws,m)});return}}
 // join juga boleh saat ronde sebelumnya sudah 'end' (tidak perlu menunggu 15 detik / reconnect)
 if(m.t==='join'&&(!ws.r||ws.r.st==='end')){joinNew(ws,m);return}
 if(m.t==='pos'&&ws.r&&ws.r.st==='play'){ws.x=fin(m.x,5e4);ws.y=fin(m.y,5e4);ws.vx=fin(m.vx,2e3);ws.vy=fin(m.vy,2e3);ws.f=m.f===-1?-1:1;ws.ph=fin(m.ph,1e5);ws.has=1;ws.dirty=1}}

// ===== Logika match (server-authoritative) =====
const rid=v=>typeof v==='number'?v:String(v==null?'':v).slice(0,8),dist=(a,b)=>a.x==null||b.x==null?0:Math.hypot(a.x-b.x,a.y-b.y);
function endRoom(r,w){if(r.st!=='play')return;r.st='end';r.nd=1;persistRoom(r);bcast(r,{t:'end',w,roles:[...r.sl.values()].map(x=>[x.id,x.role])});setTimeout(()=>{for(const p of r.ps.values())if(p.r===r)p.r=null;ROOMS.delete(r.code)},15000)}
function chk(r){if(r.st!=='play')return;const L=[...r.sl.values()].filter(x=>x.alive),im=L.filter(IMP).length;if(!im)endRoom(r,1);else if(im>=L.length-im)endRoom(r,0);else if(r.prog>=r.goal)endRoom(r,1)}
function tally(r){const m=r.meet;if(!m)return;r.meet=null;r.nd=1;const c=new Map();let sk=0;for(const[id,v]of m.votes){const x=r.sl.get(id);if(!x||!x.alive)continue;if(v==='skip'||!r.sl.get(v)||!r.sl.get(v).alive)sk++;else c.set(v,(c.get(v)||0)+1)}
 let top=null,mx=0,tie=0;for(const[id,n]of c){if(n>mx){mx=n;top=id;tie=0}else if(n===mx)tie=1}const ej=!tie&&top!=null&&mx>sk?r.sl.get(top):null;
 if(ej)ej.alive=0;for(const x of r.sl.values()){x.bd=0;x.cd=Date.now()+12000}
 bcast(r,{t:'res',ej:ej?ej.id:null,k:ej?(IMP(ej)?'imp':ej.role==='joker'?'joker':'crew'):null,votes:[...m.votes]});
 if(ej&&ej.role==='joker')endRoom(r,2);else chk(r)}
function openMeet(r,s,why,em){if(r.st!=='play'||r.meet||!s.alive)return;if(em){if(s.em)return;s.em=1}r.meet={votes:new Map(),bj:new Set(),end:Date.now()+MEET_MS};for(const x of r.sl.values())x.vent=0;bcast(r,{t:'meet',by:s.id,em:em?1:0,why:str(why,60),ms:MEET_MS})}
function onGame(ws,m){const r=ws.r;if(!r||r.st!=='play'||!r.sl)return false;const T=m.t;if(T==='ev'||T==='vote')r.nd=1;
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

// ===== Lobby & sesi di Neon: lease (satu pemilik per bilik), snapshot, resume lintas-server =====
const sha=t=>crypto.createHash('sha256').update(String(t)).digest('hex'),adopting=new Map(),cleanN=(v,id)=>String(v==null?'':v).replace(/[^\w\s.\-#]/g,'').trim().slice(0,12)||'Pemain'+id,cleanC=c=>/^#[0-9a-f]{6}$/i.test(c)?c:'#ff3b30';
const roomStatus=r=>r.st==='lobby'?'waiting':r.st==='play'?'in_game':'ended';
function joinNew(ws,m){if(ws.r){ws.r.ps.delete(ws.id);ws.r=null}
 const r=getRoom();ws.id=++PID;ws.n=cleanN(m.n,ws.id);ws.c=cleanC(m.c);ws.has=0;ws.dirty=0;ws.sent=0;ws.r=r;ws.v=m.v===2?2:1;ws.tok=ws.v===2?crypto.randomBytes(12).toString('base64url'):'';r.ps.set(ws.id,ws);r.nd=1;if(!r.dl)r.dl=Date.now()+FILL_MS;
 wsSend(ws,ws.v===2?{t:'hi',id:ws.id,v:2,tok:ws.tok,st:Date.now()}:{t:'hi',id:ws.id});if(ws.tok)dbSession(ws,r);bcast(r,lobbyInfo(r));if(r.ps.size>=MAXP)startRoom(r)}
function rejoinLobby(ws,r,id,tok,m){ws.id=id;ws.n=cleanN(m.n,id);ws.c=cleanC(m.c);ws.has=0;ws.dirty=0;ws.sent=0;ws.r=r;ws.v=2;ws.tok=tok;r.ps.set(id,ws);r.nd=1;if(!r.dl)r.dl=Date.now()+FILL_MS;wsSend(ws,{t:'hi',id,v:2,tok,st:Date.now(),resumed:1});bcast(r,lobbyInfo(r))}
// token disimpan sebagai hash SHA-256 (token asli tidak pernah masuk database)
function dbSession(ws,r){if(!POOL||DOWN||!ws.tok)return;POOL.query(`INSERT INTO whos_sus.resume_sessions(token_hash,player_id,room_id,expires_at,created_at) VALUES($1,$2,$3,NOW()+INTERVAL '2 hours',NOW()) ON CONFLICT(token_hash) DO NOTHING`,[sha(ws.tok),String(ws.id),r.code]).catch(e=>console.error('Session write:',e.message))}
function snap(r){const mem=[],s={st:r.st,dl:r.dl||0,host:r.host||0};
 if(r.st==='lobby'){for(const p of r.ps.values())mem.push({id:p.id,n:p.n,c:p.c,on:1});s.ps=mem.map(q=>({id:q.id,n:q.n,c:q.c}))}
 else{s.goal=r.goal||0;s.prog=r.prog||0;s.sq=r.sq|0;s.slots=[];for(const x of(r.sl?r.sl.values():[])){s.slots.push({id:x.id,n:x.n,c:x.c,bot:x.bot?1:0,role:x.role,alive:x.alive?1:0,bd:x.bd?1:0,vent:x.vent?1:0,em:x.em?1:0,tasks:x.tasks|0,x:x.x==null?null:Math.round(x.x),y:x.y==null?null:Math.round(x.y)});if(typeof x.id==='number')mem.push({id:x.id,n:x.n,c:x.c,on:x.ws?1:0})}}
 return{s,mem}}
function loseRoom(r){r.lost=1;console.warn('Bilik '+r.code+' dimiliki server lain, dilepas dari server ini');for(const p of[...r.ps.values()])try{p.socket.destroy()}catch(e){}ROOMS.delete(r.code)}
// simpan bilik + anggota dalam satu transaksi; hanya berhasil bila server ini masih pemilik (fencing)
async function persistRoom(r){if(!POOL||DOWN||r.lost||r.pb)return;r.pb=1;r.nd=0;r.pt=Date.now();
 const{s,mem}=snap(r);let c;try{c=await POOL.connect()}catch(e){console.error('Room persist connect:',e.message);r.nd=1;r.pb=0;return}
 try{await c.query('BEGIN');
  const g=await c.query(`INSERT INTO whos_sus.game_rooms(room_id,host_player_id,server_id,status,game_state,version,lease_until,created_at,updated_at,expires_at) VALUES($1,$2,$3,$4,$5::jsonb,1,NOW()+($6::int*INTERVAL '1 millisecond'),NOW(),NOW(),NOW()+INTERVAL '2 hours') ON CONFLICT(room_id) DO UPDATE SET host_player_id=EXCLUDED.host_player_id,status=EXCLUDED.status,game_state=EXCLUDED.game_state,version=whos_sus.game_rooms.version+1,lease_until=EXCLUDED.lease_until,updated_at=NOW() WHERE whos_sus.game_rooms.server_id=EXCLUDED.server_id`,[r.code,String(r.host||0),SERVER_ID,roomStatus(r),JSON.stringify(s),LEASE_MS]);
  if(!g.rowCount){await c.query('ROLLBACK');loseRoom(r);return}
  const ids=mem.map(q=>String(q.id));
  await c.query('DELETE FROM whos_sus.room_players WHERE room_id=$1 AND NOT (player_id=ANY($2::text[]))',[r.code,ids]);
  if(mem.length)await c.query(`INSERT INTO whos_sus.room_players(room_id,player_id,player_state,joined_at,last_seen) SELECT $1::text,x.p,x.s::jsonb,NOW(),NOW() FROM unnest($2::text[],$3::text[]) AS x(p,s) ON CONFLICT(room_id,player_id) DO UPDATE SET player_state=EXCLUDED.player_state,last_seen=NOW()`,[r.code,ids,mem.map(q=>JSON.stringify({n:q.n,c:q.c,on:q.on}))]);
  await c.query('COMMIT')}
 catch(e){try{await c.query('ROLLBACK')}catch(_){}r.nd=1;console.error('Room persist error:',e.message)}
 finally{c.release();r.pb=0}}
let pbusy=0;
async function persistAll(){if(!POOL||DOWN||pbusy)return;pbusy=1;try{const now=Date.now();for(const r of[...ROOMS.values()]){if(r.lost||(!r.nd&&now-(r.pt||0)<RENEW_MS))continue;await persistRoom(r)}}catch(e){console.error('persistAll:',e.message)}finally{pbusy=0}}
function dbEnd(r){if(!POOL||DOWN||r.lost)return;POOL.query(`UPDATE whos_sus.game_rooms SET status='ended',lease_until=NOW(),updated_at=NOW(),version=version+1 WHERE room_id=$1 AND server_id=$2`,[r.code,SERVER_ID]).catch(e=>console.error('Room end:',e.message))}
function closeRoom(r){ROOMS.delete(r.code);dbEnd(r)}
// ambil alih bilik: UPDATE atomik, hanya berhasil bila lease sudah habis (atau bilik memang milik server ini)
function adopt(code){if(adopting.has(code))return adopting.get(code);const p=adoptRoom(code).finally(()=>adopting.delete(code));adopting.set(code,p);return p}
async function adoptRoom(code){const L=ROOMS.get(code);if(L)return L.lost?null:{r:L};
 const g=await POOL.query(`UPDATE whos_sus.game_rooms SET server_id=$2,lease_until=NOW()+($3::int*INTERVAL '1 millisecond'),version=version+1,updated_at=NOW() WHERE room_id=$1 AND status<>'ended' AND expires_at>NOW() AND (server_id=$2 OR lease_until<NOW()) RETURNING game_state`,[code,SERVER_ID,LEASE_MS]);
 if(!g.rowCount){const o=await POOL.query(`SELECT s.server_url,s.status,GREATEST(0,extract(epoch from (g.lease_until-NOW()))*1000)::int AS wait FROM whos_sus.game_rooms g LEFT JOIN whos_sus.server_nodes s ON s.server_id=g.server_id WHERE g.room_id=$1 AND g.status<>'ended' AND g.expires_at>NOW()`,[code]);return o.rowCount?{redirect:o.rows[0]}:null}
 const L2=ROOMS.get(code);if(L2)return{r:L2};
 const st=g.rows[0].game_state||{},now=Date.now();let r=null;
 if(st.st==='play'&&Array.isArray(st.slots)&&st.slots.length){r={code,st:'play',ps:new Map(),dl:0,host:+st.host||0,goal:st.goal|0,prog:st.prog|0,sq:st.sq|0,meet:null,held:new Set(),sl:new Map(),nd:1,pt:0};
  for(const q of st.slots){const id=typeof q.id==='number'?q.id:String(q.id);r.sl.set(id,{id,n:q.n,c:q.c,bot:1,ws:null,role:q.role,alive:q.alive?1:0,bd:q.bd?1:0,vent:q.vent?1:0,em:q.em?1:0,tasks:q.tasks|0,cd:now+8000,rv:0,ut:0,x:q.x==null?undefined:q.x,y:q.y==null?undefined:q.y});if(typeof id==='number')PID=Math.max(PID,id)}}
 else if(st.st==='lobby'){r={code,st:'lobby',ps:new Map(),dl:now+FILL_MS,nd:1,pt:0};for(const q of(st.ps||[]))if(+q.id>0)PID=Math.max(PID,+q.id)}
 if(!r){await POOL.query(`UPDATE whos_sus.game_rooms SET status='ended',lease_until=NOW(),updated_at=NOW() WHERE room_id=$1 AND server_id=$2`,[code,SERVER_ID]).catch(()=>{});return null}
 ROOMS.set(code,r);console.log('Bilik diambil alih: '+code+' ('+r.st+')');return{r}}
// pemain v2 bawa token yang tidak dikenal server ini: cari di Neon lalu masuk kembali ke slot/lobby-nya
async function dbResume(ws,m){const tok=m.tok;if(!POOL||!/^[\w-]{8,40}$/.test(tok))return false;
 const q=await POOL.query('SELECT room_id,player_id FROM whos_sus.resume_sessions WHERE token_hash=$1 AND expires_at>NOW()',[sha(tok)]);
 if(!q.rowCount)return false;const code=q.rows[0].room_id,id=+q.rows[0].player_id;if(!Number.isFinite(id)||ws.dead||ws.r)return !!ws.dead;
 const a=await adopt(code);if(ws.dead)return true;if(!a)return false;
 if(a.redirect){wsSend(ws,{t:'redirect',url:a.redirect.server_url||'',status:a.redirect.status||'',wait:a.redirect.wait|0});try{ws.socket.end(Buffer.from([0x88,0]))}catch(e){}return true}
 const r=a.r;
 if(r.st==='play'&&r.sl){const x=r.sl.get(id);if(x&&x.bot&&!x.ws&&!r.ps.has(id)){resume(ws,{tok,r,id},x);return true}return false}
 if(r.st==='lobby'&&!r.ps.has(id)&&r.ps.size<MAXP){rejoinLobby(ws,r,id,tok,m);return true}
 return false}
// pastikan tipe kolom & indeks yang dibutuhkan ada (hanya schema whos_sus; aman dijalankan berulang, dikunci agar server tidak balapan)
async function ensureSchema(){const c=await POOL.connect();
 try{await c.query('BEGIN');await c.query('SELECT pg_advisory_xact_lock(727401)');
  await c.query(`DO $$ DECLARE r record; BEGIN FOR r IN SELECT conrelid::regclass AS t,conname FROM pg_constraint WHERE contype='f' AND conrelid IN (to_regclass('whos_sus.game_rooms'),to_regclass('whos_sus.room_players'),to_regclass('whos_sus.resume_sessions')) LOOP EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',r.t,r.conname); END LOOP; END $$`);
  const want={game_rooms:{room_id:'text',host_player_id:'text',server_id:'text',game_state:'jsonb'},room_players:{room_id:'text',player_id:'text',player_state:'jsonb'},resume_sessions:{token_hash:'text',player_id:'text',room_id:'text'}};
  const cur=await c.query(`SELECT table_name AS t,column_name AS c,data_type AS d FROM information_schema.columns WHERE table_schema='whos_sus' AND table_name=ANY($1::text[])`,[Object.keys(want)]);
  for(const x of cur.rows){const w=want[x.t]&&want[x.t][x.c];if(!w||x.d===w||(w==='text'&&x.d==='character varying'))continue;
   await c.query(`ALTER TABLE whos_sus.${x.t} ALTER COLUMN ${x.c} DROP DEFAULT`);
   await c.query(`ALTER TABLE whos_sus.${x.t} ALTER COLUMN ${x.c} TYPE ${w} USING ${x.c}::text${w==='jsonb'?'::jsonb':''}`);console.log('Schema fix: '+x.t+'.'+x.c+' '+x.d+' -> '+w)}
  await c.query('CREATE UNIQUE INDEX IF NOT EXISTS ux_whos_rooms_id ON whos_sus.game_rooms(room_id)');
  await c.query('CREATE UNIQUE INDEX IF NOT EXISTS ux_whos_roomplayers ON whos_sus.room_players(room_id,player_id)');
  await c.query('CREATE UNIQUE INDEX IF NOT EXISTS ux_whos_sessions ON whos_sus.resume_sessions(token_hash)');
  await c.query('COMMIT')}
 catch(e){try{await c.query('ROLLBACK')}catch(_){}console.error('ensureSchema:',e.message)}
 finally{c.release()}}
if(POOL){setInterval(persistAll,3000).unref();
 setInterval(()=>{if(DOWN)return;(async()=>{const old=`SELECT room_id FROM whos_sus.game_rooms WHERE (status='ended' AND updated_at<NOW()-INTERVAL '10 minutes') OR expires_at<NOW()`;
  await POOL.query(`DELETE FROM whos_sus.resume_sessions WHERE expires_at<NOW() OR room_id IN (${old})`);await POOL.query(`DELETE FROM whos_sus.room_players WHERE room_id IN (${old})`);await POOL.query(`DELETE FROM whos_sus.game_rooms WHERE room_id IN (${old})`)})().catch(e=>console.error('Cleanup:',e.message))},300000).unref()}

// ===== Parser frame WebSocket =====
const rl=ws=>{const now=Date.now();if(now-ws.mt>1000){ws.mt=now;ws.mc=0}if(++ws.mc>WS_MPS*4){abuse(ws.ip,5);ws.socket.destroy();return 2}return ws.mc>WS_MPS?1:0};
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
 if(!cfOk(q)||banned(ip))return bad('403 Forbidden');
 if(q.url.split('?')[0]!=='/ws'||!k||String(q.headers.upgrade).toLowerCase()!=='websocket'){abuse(ip);return bad('400 Bad Request')}
 if(ORIG&&!ORIG.includes(q.headers.origin)){abuse(ip);return bad('403 Forbidden')}
 if(WSN>=MAXWS){TH.full++;return bad('503 Service Unavailable')}
 if((IPC.get(ip)||0)>=(TH.mode==='attack'?3:WS_IP)){abuse(ip);return bad('503 Service Unavailable')}
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
  if(!r.ps.size){if(!(r.held&&r.held.size))closeRoom(r);continue}
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
