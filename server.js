// Server tanpa dependensi: node server.js  (butuh Node 18+)
// Konfigurasi lewat env / file .env (lihat .env.example)
const http=require('http'),fs=require('fs'),path=require('path');
try{for(const l of fs.readFileSync(path.join(__dirname,'.env'),'utf8').split(/\r?\n/)){const m=l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);if(m&&!(m[1] in process.env))process.env[m[1]]=m[2].replace(/^(['"])(.*)\1$/,'$2')}}catch(e){}
const PORT=process.env.PORT||3000,DB=path.join(__dirname,'data.json'),
E=process.env,KEY=E.AI_API_KEY||E.ANTHROPIC_API_KEY||'',
PROV=(E.AI_PROVIDER||(E.AI_BASE_URL?'openai':'anthropic')).toLowerCase(),
BASE=(E.AI_BASE_URL||(PROV==='anthropic'?'https://api.anthropic.com':'https://api.openai.com/v1')).replace(/\/+$/,''),
MD=E.AI_MODEL||E.ANTHROPIC_MODEL||(PROV==='anthropic'?'claude-sonnet-5-5':''),
MODELS={default:MD,quick:E.AI_MODEL_QUICK||E.ANTHROPIC_MODEL_QUICK||(PROV==='anthropic'?'claude-haiku-5-5':MD)},
READY=!!(KEY&&MODELS.default),
CAP=+process.env.AI_DAILY_LIMIT||5000,RPM=+process.env.AI_RPM||60;
let data={chat:[],profiles:{}};
try{data=Object.assign(data,JSON.parse(fs.readFileSync(DB,'utf8')))}catch(e){}
let dirty=false;setInterval(()=>{if(dirty){dirty=false;fs.writeFile(DB,JSON.stringify(data),()=>{})}},5000);
const hits=new Map();let day=new Date().toDateString(),used=0;
setInterval(()=>{const n=Date.now();for(const[k,a]of hits)if(!a.some(t=>n-t<60000))hits.delete(k)},60000);
const rateOk=ip=>{const n=Date.now(),a=(hits.get(ip)||[]).filter(t=>n-t<60000);if(a.length>=RPM)return false;a.push(n);hits.set(ip,a);return true};
const str=(v,n)=>String(v==null?'':v).slice(0,n),num=v=>Math.max(0,Math.min(1e9,+v||0));
const send=(r,c,o,t)=>{r.writeHead(c,{'content-type':t||'application/json','cache-control':'no-store'});r.end(typeof o==='string'||Buffer.isBuffer(o)?o:JSON.stringify(o))};
const body=(req,max=4096)=>new Promise((ok,no)=>{let b='';req.on('data',c=>{b+=c;if(b.length>max){no();req.destroy()}});req.on('end',()=>{try{ok(JSON.parse(b||'{}'))}catch(e){no()}});req.on('error',no)});
http.createServer(async(q,r)=>{try{const u=q.url.split('?')[0];
if(u==='/'||u==='/index.html')return send(r,200,fs.readFileSync(path.join(__dirname,'index.html')),'text/html;charset=utf-8');
if(u==='/api/ping')return send(r,200,{ok:1,ai:READY});
if(u==='/api/ai'&&q.method==='POST'){
 if(!READY)return send(r,503,{error:'AI_API_KEY / AI_MODEL belum diatur'});
 const ip=String(q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim();
 if(!rateOk(ip))return send(r,429,{error:'terlalu cepat'});
 const d=new Date().toDateString();if(d!==day){day=d;used=0}if(used>=CAP)return send(r,429,{error:'batas harian AI tercapai'});
 const b=await body(q,24000),p=str(b.prompt,20000);if(!p)return send(r,400,{});used++;
 const model=b.tier==='quick'?MODELS.quick:MODELS.default,A=PROV==='anthropic';
 const rs=await fetch(BASE+(A?'/v1/messages':'/chat/completions'),{method:'POST',signal:AbortSignal.timeout(40000),headers:A?{'content-type':'application/json','x-api-key':KEY,'anthropic-version':'2023-06-01'}:{'content-type':'application/json',authorization:'Bearer '+KEY},body:JSON.stringify(A?{model,max_tokens:1024,messages:[{role:'user',content:p}]}:{model,messages:[{role:'user',content:p}]})});
 if(!rs.ok){console.error('AI API',rs.status,(await rs.text()).slice(0,200));return send(r,502,{error:'AI gagal'})}
 const j=await rs.json();return send(r,200,{text:A?(j.content||[]).map(c=>c.text||'').join(''):str(j.choices&&j.choices[0]&&j.choices[0].message&&j.choices[0].message.content,8000)})}
if(u==='/api/chat'&&q.method==='GET')return send(r,200,data.chat.slice(-40).reverse().map((d,i)=>({id:String(i),d})));
if(u==='/api/chat'&&q.method==='POST'){const b=await body(q),x=str(b.x,60).trim(),uid=str(b.u,24).replace(/[^\w-]/g,'');if(!x)return send(r,400,{});data.chat.push({n:str(b.n,12),u:uid,x,t:Date.now()});if(data.chat.length>200)data.chat.splice(0,data.chat.length-200);dirty=true;return send(r,200,{ok:1})}
if(u==='/api/profiles'&&q.method==='GET')return send(r,200,Object.entries(data.profiles).sort((a,b)=>b[1].t-a[1].t).slice(0,40).map(([id,d])=>({id,d})));
const m=u.match(/^\/api\/profiles\/([\w-]{1,24})$/);
if(m&&q.method==='PUT'){const b=await body(q);data.profiles[m[1]]={name:str(b.name,12),lv:num(b.lv),xp:num(b.xp),nx:num(b.nx),mt:num(b.mt),wn:num(b.wn),kl:num(b.kl),rp:Math.min(num(b.rp),99999),age:num(b.age),reg:str(b.reg,20),bio:str(b.bio,40),show:b.show?1:0,sreg:b.sreg?1:0,t:Date.now()};dirty=true;return send(r,200,{ok:1})}
send(r,404,{})}catch(e){send(r,400,{})}}).listen(PORT,()=>console.log('Skeld server di http://localhost:'+PORT+' · AI: '+(READY?'aktif ['+PROV+'] '+MODELS.quick+' / '+MODELS.default:'MATI (set AI_API_KEY'+(PROV==='anthropic'?'':' dan AI_MODEL')+')')));
