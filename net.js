// net-v2.js — klien WebSocket protokol v2 untuk server Skeld v4 (browser & Node 22+)
// Fitur: posisi biner, sinkron jam dengan server, RTT, buffer interpolasi halus, auto-reconnect + resume slot.
//
// Pakai:
//   const net=new SkeldNet({url:'wss://domainmu/ws',name:'Budi',color:'#3b82f6',on:m=>{/* semua pesan JSON server: hi,lobby,start,role,ev,... */}});
//   net.connect();
//   net.sendPos(x,y,vx,vy,facing,phase);   // panggil ~30x/detik saat match
//   const p=net.getPos(id);                // {x,y,vx,vy,f,ph} terinterpolasi, atau null
//   net.rtt                                // ms (diukur klien), net.srvRtt = diukur server
//   net.send({t:'ev',k:'kill',v:targetId}) // event game tetap JSON seperti biasa
(function(g){
class SkeldNet{
 constructor(o){this.url=o.url;this.name=o.name||'Pemain';this.color=o.color||'#ff3b30';this.on=o.on||(()=>{});
  this.delay=o.delay==null?100:o.delay;   // ms buffer interpolasi (naikkan bila jaringan jitter)
  this.tok=null;this.id=null;this.off=0;this.rtt=0;this.srvRtt=0;this.buf=new Map();this.closed=false;this.tries=0;this.ws=null}
 connect(){const w=this.ws=new WebSocket(this.url);w.binaryType='arraybuffer';
  w.onopen=()=>{this.tries=0;w.send(JSON.stringify({t:'join',n:this.name,c:this.color,v:2,tok:this.tok||undefined}));
   clearInterval(this.pi);this._ping();this.pi=setInterval(()=>this._ping(),2000)};
  w.onmessage=e=>typeof e.data==='string'?this._json(JSON.parse(e.data)):this._bin(new DataView(e.data));
  w.onclose=()=>{clearInterval(this.pi);if(w!==this.ws||this.closed)return;this.on({t:'net',up:0});setTimeout(()=>{if(!this.closed)this.connect()},Math.min(3000,250*++this.tries))};
  w.onerror=()=>{}}
 close(){this.closed=true;clearInterval(this.pi);try{this.ws.close()}catch(e){}}
 _ping(){if(this.ws&&this.ws.readyState===1)this.ws.send(JSON.stringify({t:'ping',c:Date.now()}))}
 _json(m){
  if(m.t==='hi'){this.id=m.id;if(m.tok)this.tok=m.tok;if(m.st)this.off=m.st-Date.now()}
  else if(m.t==='pong'){const now=Date.now(),rt=now-m.c;this.rtt=this.rtt?this.rtt*0.7+rt*0.3:rt;this.srvRtt=m.r||0;
   const o=m.s+rt/2-now;this.off=this._o?this._o+(o-this._o)*0.2:o;this._o=this.off;return}
  else if(m.t==='start'||m.t==='end')this.buf.clear();
  this.on(m)}
 _bin(v){if(v.getUint8(0)!==1)return;const low=v.getUint32(1),srv=Date.now()+this.off,t=low+4294967296*Math.round((srv-low)/4294967296),n=v.getUint8(7);let o=8;
  for(let i=0;i<n;i++,o+=21){const id=v.getUint32(o);let a=this.buf.get(id);if(!a)this.buf.set(id,a=[]);
   a.push({t,x:v.getInt32(o+4),y:v.getInt32(o+8),vx:v.getInt16(o+12),vy:v.getInt16(o+14),f:v.getInt8(o+16),ph:v.getFloat32(o+17)});if(a.length>40)a.shift()}}
 send(o){if(this.ws&&this.ws.readyState===1)this.ws.send(JSON.stringify(o))}
 sendPos(x,y,vx,vy,f,ph){if(!this.ws||this.ws.readyState!==1)return;const b=new DataView(new ArrayBuffer(18));b.setUint8(0,1);b.setInt32(1,Math.round(x));b.setInt32(5,Math.round(y));
  b.setInt16(9,Math.max(-2000,Math.min(2000,Math.round(vx))));b.setInt16(11,Math.max(-2000,Math.min(2000,Math.round(vy))));b.setInt8(13,f===-1?-1:1);b.setFloat32(14,ph||0);this.ws.send(b.buffer)}
 // posisi terinterpolasi pada waktu render (sedikit di belakang waktu server agar mulus)
 getPos(id,now){const a=this.buf.get(id);if(!a||!a.length)return null;const rt=(now==null?Date.now():now)+this.off-this.delay;
  let i=a.length-1;while(i>0&&a[i-1].t>rt)i--;
  if(i===0||a[i].t<=rt){const s=a[i];if(a[i].t<=rt){const dt=Math.min(150,rt-s.t)/1000;return{x:s.x+s.vx*dt,y:s.y+s.vy*dt,vx:s.vx,vy:s.vy,f:s.f,ph:s.ph}}return{x:s.x,y:s.y,vx:s.vx,vy:s.vy,f:s.f,ph:s.ph}}
  const p=a[i-1],q=a[i],k=(rt-p.t)/Math.max(1,q.t-p.t);return{x:p.x+(q.x-p.x)*k,y:p.y+(q.y-p.y)*k,vx:q.vx,vy:q.vy,f:q.f,ph:p.ph+(q.ph-p.ph)*k}}
}
if(typeof module!=='undefined'&&module.exports)module.exports=SkeldNet;else g.SkeldNet=SkeldNet;
})(typeof window!=='undefined'?window:globalThis);
