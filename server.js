const fs=require('fs'),path=require('path'),crypto=require('crypto'),http=require('http');
const {spawn}=require('child_process');
const express=require('express');const {WebSocketServer}=require('ws');const AdmZip=require('adm-zip');
const ROOT=__dirname,DATA=process.env.DATA_DIR||path.join(ROOT,'data'),SRV=path.join(DATA,'servers');
fs.mkdirSync(SRV,{recursive:true});
const CFG_F=path.join(ROOT,'config.json');
const def={port:3000,telegramToken:"",adminTgIds:[],ownerTgId:"",maxServersPerUser:3,idleMinutes:5,bonusCoins:10,cloudflared:true,domain:""};
const cfg={...def,...(fs.existsSync(CFG_F)?JSON.parse(fs.readFileSync(CFG_F)):{})};
if(!fs.existsSync(CFG_F))fs.writeFileSync(CFG_F,JSON.stringify(cfg,null,2));
if(process.env.PORT)cfg.port=+process.env.PORT;
if(process.env.TG_TOKEN)cfg.telegramToken=process.env.TG_TOKEN;
if(process.env.ADMIN_TG_IDS)cfg.adminTgIds=process.env.ADMIN_TG_IDS.split(',').map(x=>x.trim()).filter(Boolean);
if(process.env.OWNER_TG_ID)cfg.ownerTgId=process.env.OWNER_TG_ID.trim();
if(process.env.IDLE_MINUTES)cfg.idleMinutes=+process.env.IDLE_MINUTES;
if(process.env.CLOUDFLARED==='0')cfg.cloudflared=false;
let publicUrl=process.env.PUBLIC_URL?process.env.PUBLIC_URL.replace(/\/$/,''):(cfg.domain||`http://localhost:${cfg.port}`);

// Paket toko (harga dalam koin). Gratis hanya sekali per akun.
const PLANS=[{ram:1024,label:'1 GB',price:0},{ram:2048,label:'2 GB',price:2},{ram:3072,label:'3 GB',price:4},{ram:4096,label:'4 GB',price:5},{ram:5120,label:'5 GB',price:7}];
const TYPES={
  node:{label:'Node.js',startup:'npm install && npm start'},
  python:{label:'Python',startup:'[ -f requirements.txt ] && pip install -r requirements.txt; python3 main.py'},
  web:{label:'Web statis',startup:''}
};
const typeOf=s=>(s&&TYPES[s.type])?s.type:'node';
const USER_CMDS=['ls','pwd','cat','cd','clear','npm','node','python3','pip','echo','tail','head','grep','mkdir','touch'];
const SHELL_RE=/^(pkg|apt|apt-get|npm|npx|yarn|pnpm|node|pip|pip3|python|python3|git|ls|cd|cat|cp|mv|rm|rmdir|mkdir|touch|nano|vim|echo|clear|pwd|kill|killall|ps|top|htop|curl|wget|unzip|zip|tar|chmod|chown|grep|find|head|tail|sed|awk|sh|bash|exit|df|du|free|ifconfig|ip|ping|env|export|source|which|history|sudo|gcc|make|java|nohup|screen|tmux|ssh|scp|rsync|man|help|date|uname|whoami|cloudflared|termux-\S+)(\s|$)/i;

// ---------- DB ----------
const DB_F=path.join(DATA,'db.json');
const db=fs.existsSync(DB_F)?JSON.parse(fs.readFileSync(DB_F)):{users:[],servers:[]};
const save=()=>fs.writeFileSync(DB_F,JSON.stringify(db,null,1));
const rnd=n=>crypto.randomBytes(n).toString('hex');
const hash=(p,s=rnd(8))=>s+':'+crypto.scryptSync(p,s,32).toString('hex');
const check=(p,h)=>{const [s]=h.split(':');return crypto.timingSafeEqual(Buffer.from(hash(p,s)),Buffer.from(h))};
function addUser(username,password,admin=false,tg=null){
  if(db.users.find(u=>u.username===username))throw new Error('Username sudah ada');
  const u={id:rnd(4),username,pass:hash(password),admin,tg,coins:0,bonusGiven:false};db.users.push(u);save();return u}
if(!db.users.length){const pw=rnd(5);addUser('admin',pw,true);
  console.log(`\n=== ADMIN DIBUAT ===\nUsername: admin\nPassword: ${pw}\n====================\n`)}
const isOwnerTg=id=>!!cfg.ownerTgId&&String(id)===String(cfg.ownerTgId);
const isAdminTg=id=>isOwnerTg(id)||cfg.adminTgIds.map(String).includes(String(id));
const tgUserOf=id=>db.users.find(u=>u.tg===String(id));

// ---------- Server manager ----------
const procs={};
const P=id=>procs[id]||(procs[id]={p:null,job:null,cwd:null,prompt:false,logs:[],status:'offline',clients:new Set(),last:null});
const strip=s=>s.replace(/\x1b\[[0-9;?]*[A-Za-z]/g,'');
const dirOf=id=>path.join(SRV,id);
const touch=s=>{if(s)s.lastActive=Date.now()};
function emit(id,m){const s=JSON.stringify(m);P(id).clients.forEach(c=>c.readyState===1&&c.send(s))}
function log(id,t){const o=P(id);t.split(/\r?\n/).forEach(l=>{if(l===''&&t!=='\n')return;o.logs.push(l);if(o.logs.length>600)o.logs.shift();emit(id,{t:'log',d:l})})}
function setStatus(id,s){P(id).status=s;emit(id,{t:'status',d:s})}
function nextPort(){let p=20000;for(const s of db.servers)if(s.port>=p)p=s.port+1;return p}
function loadEnv(dir){const f=path.join(dir,'.env'),o={};if(!fs.existsSync(f))return o;
  for(const l of fs.readFileSync(f,'utf8').split(/\r?\n/)){const m=l.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if(m)o[m[1]]=m[2].replace(/^(["'])(.*)\1$/,'$2')}
  return o}

function startSrv(id){
  const o=P(id),s=db.servers.find(x=>x.id===id);
  if(!s)return;
  touch(s);
  if(typeOf(s)==='web'){o.status='running';return}
  if(o.p)return;
  if(!s.port){s.port=nextPort();save()}
  log(id,'[panel] Server ditandai untuk mulai...');
  setStatus(id,'starting');
  const p=spawn('sh',['-c',s.startup||'echo "Tidak ada startup command"'],{cwd:dirOf(id),detached:true,
    env:{...process.env,PORT:String(s.port),...loadEnv(dirOf(id)),NODE_OPTIONS:`--max-old-space-size=${s.ram}`,SERVER_MEMORY:String(s.ram),SERVER_ID:id}});
  o.p=p;o.last=null;o.prompt=false;o.startedAt=Date.now();
  p.stdout.on('data',d=>{o.prompt=!/\n$/.test(d.toString());log(id,strip(d.toString()));setStatus(id,'running')});
  p.stderr.on('data',d=>{o.prompt=!/\n$/.test(d.toString());log(id,strip(d.toString()))});
  p.on('close',c=>{o.p=null;o.last=null;log(id,`[panel] Proses berhenti dengan kode ${c}`);setStatus(id,'offline')});
  p.on('error',e=>{o.p=null;o.last=null;log(id,'[panel] '+e.message);setStatus(id,'offline')});
}
function stopSrv(id,sig='SIGTERM'){const o=P(id);if(!o.p)return;
  try{process.kill(-o.p.pid,sig)}catch{try{o.p.kill(sig)}catch{}}
  if(sig==='SIGTERM'){setStatus(id,'stopping');setTimeout(()=>o.p&&stopSrv(id,'SIGKILL'),10000)}}

function createServer({name,owner,ram,startup,type,autostart,createdBy,free}){
  const t=TYPES[type]?type:'node';
  const id=rnd(4);const d=dirOf(id);fs.mkdirSync(d,{recursive:true});
  fs.writeFileSync(path.join(d,'.env'),'# Variabel environment server ini. Format: KEY=VALUE\n');
  if(t==='node'){
    fs.writeFileSync(path.join(d,'package.json'),JSON.stringify({name:'bot',version:'1.0.0',scripts:{start:'node index.js'},dependencies:{}},null,2));
    fs.writeFileSync(path.join(d,'index.js'),"console.log('Server Node.js siap! Upload file kamu di tab Files, lalu Restart.');setInterval(()=>{},1e5);\n");
  }else if(t==='python'){
    fs.writeFileSync(path.join(d,'requirements.txt'),'# Tulis dependency Python di sini, satu per baris\n');
    fs.writeFileSync(path.join(d,'main.py'),"import time\nprint('Server Python siap! Upload script kamu di tab Files, lalu Restart.')\nwhile True:\n    time.sleep(100)\n");
  }else{
    fs.writeFileSync(path.join(d,'index.html'),'<!DOCTYPE html><html lang="id"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Website</title></head><body style="font-family:sans-serif;text-align:center;padding:60px"><h1>Website kamu sudah online</h1><p>Upload file website kamu di tab Files.</p></body></html>\n');
  }
  const s={id,name:name||'Server',owner,type:t,ram:+ram||1024,port:nextPort(),
    startup:startup||TYPES[t].startup,autostart:autostart!==false,created:Date.now(),
    createdBy:createdBy||'',free:!!free,lastActive:Date.now()};
  db.servers.push(s);save();
  if(t==='web')P(id).status='running';
  return s}
function deleteServer(id){
  const o=P(id);if(o.job)try{process.kill(-o.job.pid,'SIGKILL')}catch{}
  stopSrv(id,'SIGKILL');fs.rmSync(dirOf(id),{recursive:true,force:true});
  db.servers=db.servers.filter(s=>s.id!==id);delete procs[id];save()}

// Pembelian dari toko web atau bot. Dipakai bersama supaya aturannya sama.
function purchase(user,{ram,type,name},via){
  const plan=PLANS.find(p=>p.ram===+ram);if(!plan)throw new Error('Paket tidak valid');
  const t=TYPES[type||'node']?(type||'node'):null;if(!t)throw new Error('Tipe tidak valid');
  const mine=db.servers.filter(s=>s.owner===user.id);
  if(!user.admin&&mine.length>=cfg.maxServersPerUser)throw new Error(`Batas ${cfg.maxServersPerUser} server per akun tercapai`);
  if(!user.admin&&plan.price===0&&mine.some(s=>s.free))throw new Error('Paket gratis hanya bisa dipakai sekali');
  if(!user.admin&&(user.coins||0)<plan.price)throw new Error(`Koin tidak cukup. Saldo ${user.coins||0}, butuh ${plan.price}`);
  if(!user.admin)user.coins=(user.coins||0)-plan.price;
  const s=createServer({name:name||`Server-${plan.label.replace(' ','')}`,owner:user.id,ram:plan.ram,type:t,createdBy:user.username,free:plan.price===0});
  save();
  notifyAdmins(`Server baru dibuat\nOleh: ${user.username}\nKode: ${s.id}\nTipe: ${TYPES[t].label}\nRAM: ${s.ram} MB\nLewat: ${via==='bot'?'Bot Telegram':'Web toko'}`);
  return s}

// ---------- Statistik CPU & RAM (baca /proc) ----------
const CLK=100;
function treeOf(root){const map={};
  try{for(const d of fs.readdirSync('/proc')){if(!/^\d+$/.test(d))continue;
    try{const st=fs.readFileSync('/proc/'+d+'/stat','utf8');const f=st.slice(st.lastIndexOf(')')+2).split(' ');
      map[d]={ppid:f[1],ut:+f[11],st:+f[12]}}catch{}}}catch{}
  const kids={};for(const k in map){(kids[map[k].ppid]=kids[map[k].ppid]||[]).push(k)}
  const out=[],q=[String(root)];
  while(q.length){const c=q.pop();if(map[c])out.push({pid:c,...map[c]});(kids[c]||[]).forEach(x=>q.push(x))}
  return out}
function stats(id){const o=P(id);if(!o.p)return{cpu:0,ram:0,uptime:0};
  let ticks=0,pages=0;
  for(const x of treeOf(o.p.pid)){ticks+=x.ut+x.st;
    try{pages+=+fs.readFileSync('/proc/'+x.pid+'/statm','utf8').split(' ')[1]}catch{}}
  const now=Date.now(),prev=o.last;o.last={t:now,ticks};
  let cpu=0;if(prev&&now>prev.t)cpu=((ticks-prev.ticks)/CLK)/((now-prev.t)/1000)*100;
  return{cpu:Math.max(0,+cpu.toFixed(1)),ram:Math.round(pages*4096/1048576),uptime:Date.now()-(o.startedAt||Date.now())}}
setInterval(()=>{for(const id in procs){const o=procs[id];if(!o.p||!o.clients.size)continue;
  const s=db.servers.find(x=>x.id===id);emit(id,{t:'stats',...stats(id),limit:s?s.ram:0})}},2000);

// ---------- Auto hapus server idle ----------
// Server yang mati dan tidak disentuh (buka panel, console, file, atau diakses lewat link) selama idleMinutes akan dihapus.
// Server yang sedang jalan tidak dihapus. Server milik admin juga tidak dihapus.
setInterval(()=>{
  const now=Date.now(),limit=(cfg.idleMinutes||0)*60000;
  if(limit>0){
    for(const s of [...db.servers]){
      const owner=db.users.find(u=>u.id===s.owner);
      if(owner&&owner.admin)continue;
      if(P(s.id).p)continue;
      if(now-(s.lastActive||s.created||now)<limit)continue;
      const name=s.name,code=s.id,who=owner?owner.username:'-';
      deleteServer(s.id);
      notifyAdmins(`Server dihapus otomatis (tidak dipakai ${cfg.idleMinutes} menit)\nNama: ${name}\nKode: ${code}\nMilik: ${who}`);
      if(owner&&owner.tg)tg('sendMessage',{chat_id:owner.tg,text:`Server ${name} (${code}) dihapus otomatis karena tidak dipakai ${cfg.idleMinutes} menit.`}).catch(()=>{});
    }}
  save()},30000);

// ---------- Console: perintah shell + input bot ----------
function runCmd(id,raw,isAdmin=false){
  const o=P(id);
  if(!o.cwd)o.cwd=dirOf(id);
  touch(db.servers.find(x=>x.id===id));
  const t0=String(raw).trim();
  if(o.job&&!t0.startsWith('$')&&!t0.startsWith('!')){
    log(id,'> '+t0);if(o.job.stdin.writable)o.job.stdin.write(t0+'\n');return}
  const forceShell=t0.startsWith('$'),forceBot=t0.startsWith('!');
  const body=(forceShell||forceBot)?t0.slice(1).trim():t0;
  const botUp=!!(o.p&&o.p.stdin.writable);
  const toBot=forceBot||(!forceShell&&botUp&&(o.prompt||!SHELL_RE.test(body)));
  if(toBot){
    if(!botUp){log(id,'[panel] Server belum berjalan. Tekan Start dulu.');return}
    o.prompt=false;log(id,'> '+body);o.p.stdin.write(body+'\n');return}
  if(!body)return;
  if(!isAdmin){
    const first=body.split(/\s+/)[0];
    if(!USER_CMDS.includes(first)){
      log(id,'[panel] Perintah "'+first+'" hanya untuk admin. Yang bisa kamu pakai: '+USER_CMDS.join(', ')+'. Input bot: teks biasa atau awalan !');return}
  }
  if(body==='clear'){o.logs.length=0;emit(id,{t:'clear'});return}
  if(body==='cd'||body.startsWith('cd ')){
    let arg=body.slice(2).trim();if(!arg||arg==='~')arg=process.env.HOME||'/';
    const target=path.resolve(o.cwd,arg.replace(/^~(?=\/|$)/,process.env.HOME||'/'));
    const inside=target===dirOf(id)||target.startsWith(dirOf(id)+path.sep);
    if(!isAdmin&&!inside){log(id,'cd: user hanya bisa berada di folder server');return}
    if(fs.existsSync(target)&&fs.statSync(target).isDirectory())o.cwd=target;
    else log(id,'cd: folder tidak ditemukan: '+arg);
    return}
  if(o.job){log(id,'[panel] Masih ada perintah berjalan. Tekan Ctrl+C dulu.');return}
  let c=body;
  if(/^pkg install\b/.test(c)&&!/(^|\s)-y(\s|$)/.test(c))c=c.replace(/^pkg install/,'pkg install -y');
  log(id,'$ '+body);
  const j=spawn('sh',['-c',c],{cwd:o.cwd,detached:true,stdio:['pipe','pipe','pipe'],
    env:{...process.env,TERM:'dumb',DEBIAN_FRONTEND:'noninteractive'}});
  o.job=j;
  const out=d=>log(id,strip(d.toString()));
  j.stdout.on('data',out);j.stderr.on('data',out);
  j.on('close',code=>{if(o.job===j)o.job=null;log(id,`[selesai dengan kode ${code}]`)});
  j.on('error',e=>{if(o.job===j)o.job=null;log(id,'[error] '+e.message)});
}
function stopCmd(id){const o=P(id);if(!o.job)return;
  try{process.kill(-o.job.pid,'SIGINT')}catch{try{o.job.kill('SIGINT')}catch{}}
  log(id,'^C');}

// ---------- HTTP ----------
const app=express();
const cookies=h=>Object.fromEntries((h||'').split(';').map(x=>x.trim().split('=')).filter(x=>x[0]).map(([k,...v])=>[k,v.join('=')]));
const sessions={};
const userOf=req=>{const t=cookies(req.headers.cookie).tok;const id=sessions[t];return db.users.find(u=>u.id===id)};
const auth=(req,res,next)=>{const u=userOf(req);if(!u)return res.status(401).json({error:'Belum login'});req.user=u;next()};
const adminOnly=(req,res,next)=>req.user.admin?next():res.status(403).json({error:'Khusus admin'});
const srvAccess=(req,res,next)=>{const s=db.servers.find(x=>x.id===req.params.id);
  if(!s||!(req.user.admin||s.owner===req.user.id))return res.status(404).json({error:'Server tidak ditemukan'});touch(s);req.srv=s;next()};
const safe=(id,p='/')=>{const b=dirOf(id),r=path.resolve(b,'.'+path.posix.join('/',p));if(r!==b&&!r.startsWith(b+path.sep))throw new Error('Path tidak valid');return r};
const wrap=f=>(req,res)=>{try{f(req,res)}catch(e){res.status(400).json({error:e.message})}};
const pub=s=>{const t=typeOf(s);return{...s,type:t,typeLabel:TYPES[t].label,status:P(s.id).status,
  address:`${publicUrl}/#/server/${s.id}`,
  siteUrl:t==='web'?`${publicUrl}/s/${s.id}/`:`${publicUrl}/p/${s.id}/`,
  ownerName:(db.users.find(u=>u.id===s.owner)||{}).username}};

// Website statis dan proxy aplikasi. Dipasang sebelum express.json.
app.use('/s/:id',(req,res,next)=>{const s=db.servers.find(x=>x.id===req.params.id);
  if(!s||typeOf(s)!=='web')return next();touch(s);
  express.static(dirOf(s.id))(req,res,next)});
app.use('/p/:id',(req,res)=>{const s=db.servers.find(x=>x.id===req.params.id);
  if(!s||typeOf(s)==='web')return res.status(404).send('Tidak ditemukan');
  touch(s);
  if(!P(s.id).p)return res.status(503).send('Server belum berjalan. Tekan Start di panel.');
  const pr=http.request({host:'127.0.0.1',port:s.port,method:req.method,path:req.url,headers:req.headers},up=>{
    res.writeHead(up.statusCode,up.headers);up.pipe(res)});
  pr.on('error',()=>{if(!res.headersSent)res.status(502).send('Aplikasi belum listen di port PORT')});
  req.pipe(pr)});

app.use(express.json());
app.get('/toko',(req,res)=>res.sendFile(path.join(ROOT,'public','toko.html')));

app.post('/api/login',wrap((req,res)=>{const {username,password}=req.body;
  const u=db.users.find(x=>x.username===username);
  if(!u||!check(password||'',u.pass))return res.status(401).json({error:'Username atau password salah'});
  let bonus=0;
  if(!u.bonusGiven){bonus=cfg.bonusCoins;u.coins=(u.coins||0)+bonus;u.bonusGiven=true;save()}
  const t=rnd(24);sessions[t]=u.id;res.setHeader('Set-Cookie',`tok=${t}; HttpOnly; Path=/; SameSite=Lax; Max-Age=604800`);
  res.json({ok:true,bonus})}));
app.post('/api/register',wrap((req,res)=>{const {username,password}=req.body;
  if(!username||!/^[A-Za-z0-9_]{3,20}$/.test(username))throw new Error('Username 3-20 karakter (huruf, angka, _)');
  if(!password||password.length<4)throw new Error('Password minimal 4 karakter');
  addUser(username,password);res.json({ok:true})}));
app.post('/api/logout',(req,res)=>{delete sessions[cookies(req.headers.cookie).tok];res.json({ok:true})});
app.get('/api/me',auth,(req,res)=>res.json({id:req.user.id,username:req.user.username,admin:req.user.admin,coins:req.user.coins||0,publicUrl,userCmds:USER_CMDS}));
app.put('/api/me/password',auth,wrap((req,res)=>{
  if(!check(req.body.current||'',req.user.pass))throw new Error('Password lama salah');
  if(!req.body.password||req.body.password.length<4)throw new Error('Password baru minimal 4 karakter');
  req.user.pass=hash(req.body.password);save();res.json({ok:true})}));
app.get('/api/types',auth,(req,res)=>res.json(Object.entries(TYPES).map(([k,v])=>({id:k,label:v.label}))));
app.get('/api/plans',auth,(req,res)=>res.json(PLANS));

// Toko: buat server dengan koin
app.post('/api/store/create',auth,wrap((req,res)=>{
  const s=purchase(req.user,{ram:req.body.ram,type:req.body.type,name:String(req.body.name||'').slice(0,40)},'web');
  res.json({...pub(s),coins:req.user.coins})}));
app.get('/api/store/servers',auth,(req,res)=>res.json(db.servers.filter(s=>req.user.admin||s.owner===req.user.id).map(pub)));
app.delete('/api/store/servers/:id',auth,srvAccess,wrap((req,res)=>{
  const name=req.srv.name;deleteServer(req.srv.id);
  if(req.user.admin)notifyAdmins(`Server ${name} dihapus oleh admin ${req.user.username}`);
  res.json({ok:true})}));

app.get('/api/servers',auth,(req,res)=>res.json(db.servers.filter(s=>req.user.admin||s.owner===req.user.id).map(pub)));
app.post('/api/servers',auth,adminOnly,wrap((req,res)=>{
  const o=db.users.find(u=>u.id===req.body.owner);if(!o)throw new Error('Owner tidak ada');
  res.json(pub(createServer({...req.body,createdBy:req.user.username})))}));
app.get('/api/servers/:id',auth,srvAccess,(req,res)=>res.json(pub(req.srv)));
app.delete('/api/servers/:id',auth,adminOnly,srvAccess,(req,res)=>{deleteServer(req.srv.id);res.json({ok:true})});
app.put('/api/servers/:id/startup',auth,srvAccess,(req,res)=>{
  if(req.body.startup!==undefined)req.srv.startup=String(req.body.startup).slice(0,500);
  if(req.body.autostart!==undefined)req.srv.autostart=!!req.body.autostart;
  save();res.json({ok:true})});
app.post('/api/servers/:id/power',auth,srvAccess,(req,res)=>{const id=req.srv.id,a=req.body.action;
  if(typeOf(req.srv)==='web')return res.json({ok:true});
  if(a==='start')startSrv(id);else if(a==='stop')stopSrv(id);else if(a==='kill')stopSrv(id,'SIGKILL');
  else if(a==='restart'){stopSrv(id);const t=setInterval(()=>{if(!P(id).p){clearInterval(t);startSrv(id)}},500)}
  res.json({ok:true})});

app.get('/api/users',auth,adminOnly,(req,res)=>res.json(db.users.map(u=>({id:u.id,username:u.username,admin:u.admin,tg:u.tg,coins:u.coins||0}))));
app.post('/api/users',auth,adminOnly,wrap((req,res)=>{const {username,password,admin}=req.body;
  if(!username||!password)throw new Error('Isi username & password');addUser(username,password,!!admin);res.json({ok:true})}));
app.put('/api/users/:id/password',auth,adminOnly,wrap((req,res)=>{
  const u=db.users.find(x=>x.id===req.params.id);if(!u)throw new Error('User tidak ada');
  if(!req.body.password||req.body.password.length<4)throw new Error('Password minimal 4 karakter');
  u.pass=hash(req.body.password);save();res.json({ok:true})}));
app.put('/api/users/:id/coins',auth,adminOnly,wrap((req,res)=>{
  const u=db.users.find(x=>x.id===req.params.id);if(!u)throw new Error('User tidak ada');
  const n=Math.trunc(+req.body.amount);if(!Number.isFinite(n))throw new Error('Jumlah tidak valid');
  u.coins=Math.max(0,(u.coins||0)+n);save();res.json({ok:true,coins:u.coins})}));
app.delete('/api/users/:id',auth,adminOnly,wrap((req,res)=>{
  if(req.params.id===req.user.id)throw new Error('Tidak bisa hapus diri sendiri');
  db.servers.filter(s=>s.owner===req.params.id).forEach(s=>deleteServer(s.id));
  db.users=db.users.filter(u=>u.id!==req.params.id);save();res.json({ok:true})}));

// files
const F='/api/servers/:id/files';
app.get(F,auth,srvAccess,wrap((req,res)=>{const d=safe(req.srv.id,req.query.path);
  res.json(fs.readdirSync(d,{withFileTypes:true}).map(e=>{const st=fs.statSync(path.join(d,e.name));
    return {name:e.name,dir:e.isDirectory(),size:st.size,mtime:st.mtimeMs}}).sort((a,b)=>b.dir-a.dir||a.name.localeCompare(b.name)))}));
app.get(F+'/read',auth,srvAccess,wrap((req,res)=>{const f=safe(req.srv.id,req.query.path);
  if(fs.statSync(f).size>5e6)throw new Error('File terlalu besar untuk diedit (>5MB). Gunakan Unduh.');res.type('text/plain').send(fs.readFileSync(f,'utf8'))}));
app.get(F+'/download',auth,srvAccess,wrap((req,res)=>res.download(safe(req.srv.id,req.query.path))));
app.put(F+'/write',auth,srvAccess,express.text({type:'*/*',limit:'20mb'}),wrap((req,res)=>{fs.writeFileSync(safe(req.srv.id,req.query.path),req.body||'');res.json({ok:true})}));
app.post(F+'/mkdir',auth,srvAccess,wrap((req,res)=>{fs.mkdirSync(safe(req.srv.id,req.body.path),{recursive:true});res.json({ok:true})}));
app.post(F+'/delete',auth,srvAccess,wrap((req,res)=>{for(const p of req.body.paths)fs.rmSync(safe(req.srv.id,p),{recursive:true,force:true});res.json({ok:true})}));
app.post(F+'/upload',auth,srvAccess,wrap((req,res)=>{
  const dir=safe(req.srv.id,req.query.path),f=path.join(dir,path.basename(req.query.name||'file'));
  const out=fs.createWriteStream(f);
  out.on('error',e=>{if(!res.headersSent)res.status(500).json({error:e.message})});
  out.on('finish',()=>res.json({ok:true}));
  req.pipe(out)}));
app.post(F+'/unzip',auth,srvAccess,wrap((req,res)=>{
  const zf=safe(req.srv.id,req.body.path);
  const dest=path.dirname(zf);
  const z=new AdmZip(zf);let n=0;
  for(const e of z.getEntries()){
    const t=path.resolve(dest,e.entryName);
    if(t!==dest&&!t.startsWith(dest+path.sep))throw new Error('Entri zip tidak aman: '+e.entryName);
    if(e.isDirectory){fs.mkdirSync(t,{recursive:true});continue}
    fs.mkdirSync(path.dirname(t),{recursive:true});
    fs.writeFileSync(t,e.getData());n++}
  res.json({ok:true,files:n})}));

app.use(express.static(path.join(ROOT,'public')));

// ---------- WebSocket console ----------
const server=http.createServer(app);
server.requestTimeout=0;server.headersTimeout=0;server.timeout=0;
const wss=new WebSocketServer({noServer:true});
server.on('upgrade',(req,sock,head)=>{
  const u=userOf(req),url=new URL(req.url,'http://x'),id=url.searchParams.get('id');
  const s=db.servers.find(x=>x.id===id);
  if(url.pathname!=='/ws'||!u||!s||!(u.admin||s.owner===u.id))return sock.destroy();
  wss.handleUpgrade(req,sock,head,ws=>{
    const o=P(id);o.clients.add(ws);touch(s);
    ws.send(JSON.stringify({t:'init',logs:o.logs,status:o.status,stats:stats(id),limit:s.ram,isAdmin:!!u.admin,userCmds:USER_CMDS}));
    ws.on('message',m=>{try{const j=JSON.parse(m);
      if(j.t==='cmd')runCmd(id,String(j.d),!!u.admin);
      else if(j.t==='ctrl')stopCmd(id)}catch{}});
    ws.on('close',()=>o.clients.delete(ws))})});

// ---------- Telegram bot ----------
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const tg=async(m,b)=>{const r=await fetch(`https://api.telegram.org/bot${cfg.telegramToken}/${m}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});return r.json()};
const H=s=>String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const kb=rows=>({inline_keyboard:rows});
const btn=(t,d)=>({text:t,callback_data:d});
const tgSend=(c,text,markup)=>tg('sendMessage',{chat_id:c,text,parse_mode:'HTML',reply_markup:markup});
function notifyAdmins(text){
  if(!cfg.telegramToken)return;
  const ids=new Set([...cfg.adminTgIds.map(String),String(cfg.ownerTgId||'')].filter(Boolean));
  ids.forEach(i=>tg('sendMessage',{chat_id:i,text}).catch(()=>{}))}
function ensureTgUser(from){
  let u=tgUserOf(from.id);if(u)return{u,pw:null,created:false};
  const pw=rnd(4);u=addUser('tg'+from.id,pw,false,String(from.id));
  u.coins=cfg.bonusCoins;u.bonusGiven=true;save();
  return{u,pw,created:true}}
function mainKb(c){
  const rows=[[btn('Buat Panel','plans'),btn('Koin Saya','saldo')],[btn('Panel Saya','mine')]];
  if(isAdminTg(c))rows.push([btn('Daftar Panel (Admin)','adm')]);
  if(isOwnerTg(c))rows.push([btn('Menu Owner','own')]);
  return kb(rows)}
function statusLine(s){return `${H(s.name)} | ${H(s.id)} | ${s.status==='running'||P(s.id).status==='running'?'ON':'OFF'} | ${s.ram}MB`}

async function tgMessage(m){
  const c=m.chat.id,text=(m.text||'').trim(),[cmd,...args]=text.split(/\s+/);
  const {u,pw,created}=ensureTgUser(m.from);
  if(cmd==='/start'||cmd==='/menu'){
    let t=`Halo <b>${H(u.username)}</b>\nSaldo koin: <b>${u.coins||0}</b>\n`;
    if(created)t+=`\nAkun kamu dibuat.\nUsername: <code>${H(u.username)}</code>\nPassword: <code>${pw}</code>\nBonus ${cfg.bonusCoins} koin sudah masuk.\n`;
    return tgSend(c,t+'\nPilih menu di bawah:',mainKb(c))}
  if(cmd==='/panel')return tgSend(c,`Panel: ${H(publicUrl)}\nToko: ${H(publicUrl)}/toko`);
  if(cmd==='/addadmin'){
    if(!isOwnerTg(c))return tgSend(c,'Khusus owner.');
    const id=args[0];if(!id)return tgSend(c,'Pakai: /addadmin ID_TELEGRAM');
    if(!cfg.adminTgIds.map(String).includes(id))cfg.adminTgIds.push(id);saveAdmins();
    return tgSend(c,`Admin ditambahkan: ${H(id)}`,mainKb(c))}
  if(cmd==='/koin'){
    if(!isAdminTg(c))return tgSend(c,'Khusus admin.');
    const [name,amt]=args;const t=db.users.find(x=>x.username===name);
    if(!t||!Number.isFinite(+amt))return tgSend(c,'Pakai: /koin USERNAME JUMLAH (bisa minus)');
    t.coins=Math.max(0,(t.coins||0)+Math.trunc(+amt));save();
    if(t.tg)tg('sendMessage',{chat_id:t.tg,text:`Saldo koin kamu sekarang ${t.coins}.`}).catch(()=>{});
    return tgSend(c,`Saldo ${H(t.username)} sekarang ${t.coins} koin`,mainKb(c))}
  if(cmd==='/del'){
    if(!isAdminTg(c))return;
    const s=db.servers.find(x=>x.id===args[0]);if(!s)return tgSend(c,'Server tidak ada.');
    deleteServer(s.id);return tgSend(c,'Dihapus '+H(s.id))}
  return tgSend(c,'Ketik /menu untuk membuka menu.',mainKb(c))}

async function tgCallback(q){
  const c=q.from.id,d=q.data||'',chat=q.message.chat.id;
  await tg('answerCallbackQuery',{callback_query_id:q.id}).catch(()=>{});
  const {u}=ensureTgUser(q.from);
  const send=(t,m)=>tgSend(chat,t,m);
  if(d==='menu')return send('Menu utama:',mainKb(c));
  if(d==='plans')return send(`Pilih paket (saldo: ${u.coins||0} koin):`,kb([...PLANS.map(p=>[btn(`${p.label} · ${p.price?p.price+' koin':'Gratis'}`,'buy:'+p.ram)]),[btn('Kembali','menu')]]));
  if(d==='saldo')return send(`Saldo koin: <b>${u.coins||0}</b>\nToko web: ${H(publicUrl)}/toko`,kb([[btn('Kembali','menu')]]));
  if(d.startsWith('buy:')){
    try{const s=purchase(u,{ram:+d.slice(4),type:'node'},'bot');
      return send(`Server dibuat.\nKode: <code>${H(s.id)}</code>\nRAM: ${s.ram} MB\nPanel: ${H(publicUrl)}/#/server/${H(s.id)}\nUsername: <code>${H(u.username)}</code>\nSisa koin: ${u.coins}\n\nServer yang mati dan tidak dipakai ${cfg.idleMinutes} menit akan dihapus otomatis.`,kb([[btn('Panel Saya','mine'),btn('Menu','menu')]]))}
    catch(e){return send('Gagal: '+H(e.message),kb([[btn('Pilih paket lain','plans')]]))}}
  if(d==='mine'){
    const list=db.servers.filter(s=>s.owner===u.id);
    const text=list.length?list.map(s=>statusLine(s)).join('\n'):'Kamu belum punya server.';
    return send('Panel saya (nama | kode | status | RAM):\n'+text,kb([...list.map(s=>[btn('Hapus '+s.name+' ('+s.id+')','del:'+s.id)]),[btn('Menu','menu')]]))}
  if(d==='adm'){
    if(!isAdminTg(c))return send('Khusus admin.');
    const list=db.servers;
    const text=list.length?list.map(s=>{const o=(db.users.find(x=>x.id===s.owner)||{}).username||'-';return statusLine(s)+' | milik '+H(o)}).join('\n'):'Belum ada server.';
    return send('Semua panel (ON/OFF):\n'+text,kb([...list.slice(0,20).map(s=>[btn('Hapus '+s.name+' ('+s.id+')','del:'+s.id)]),[btn('Menu','menu')]]))}
  if(d==='own'){
    if(!isOwnerTg(c))return send('Khusus owner.');
    const rows=cfg.adminTgIds.map(id=>[btn('Hapus admin '+id,'deladm:'+id)]);
    return send('Menu owner.\nAdmin: '+(cfg.adminTgIds.join(', ')||'-')+'\n\nTambah admin: /addadmin ID_TELEGRAM',kb([...rows,[btn('Menu','menu')]]))}
  if(d.startsWith('del:')){
    const s=db.servers.find(x=>x.id===d.slice(4));if(!s)return send('Server sudah tidak ada.');
    if(s.owner!==u.id&&!isAdminTg(c))return send('Kamu tidak bisa menghapus server ini.');
    const owner=(db.users.find(x=>x.id===s.owner)||{}).username||'-';const name=s.name,code=s.id;
    deleteServer(s.id);
    if(s.owner!==u.id)notifyAdmins(`Server ${name} (${code}) milik ${owner} dihapus oleh ${u.username}`);
    return send(`Server ${H(name)} dihapus.`,mainKb(c))}
  if(d.startsWith('deladm:')){
    if(!isOwnerTg(c))return send('Khusus owner.');
    cfg.adminTgIds=cfg.adminTgIds.filter(x=>String(x)!==d.slice(7));saveAdmins();
    return send('Admin dihapus.',mainKb(c))}
}
function saveAdmins(){
  let c={};try{c=JSON.parse(fs.readFileSync(CFG_F))}catch{}
  c.adminTgIds=cfg.adminTgIds;fs.writeFileSync(CFG_F,JSON.stringify(c,null,2))}
async function tgHandle(u){
  if(u.message&&u.message.text)return tgMessage(u.message);
  if(u.callback_query)return tgCallback(u.callback_query)}
async function tgPoll(){let off=0;console.log('[telegram] bot aktif');
  for(;;){try{const r=await tg('getUpdates',{offset:off,timeout:30,allowed_updates:['message','callback_query']});
    for(const u of r.result||[]){off=u.update_id+1;tgHandle(u).catch(e=>console.log('[tg]',e.message))}}catch{await sleep(5000)}}}

// ---------- Cloudflared ----------
function startTunnel(){
  const p=spawn('cloudflared',['tunnel','--url',`http://localhost:${cfg.port}`,'--no-autoupdate']);
  p.on('error',()=>console.log('[cloudflared] belum terpasang. Jalankan: pkg install cloudflared'));
  const onD=d=>{const m=d.toString().match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/);
    if(m&&publicUrl!==m[0]){publicUrl=m[0];console.log('[cloudflared] URL publik:',publicUrl);notifyAdmins('Panel online: '+publicUrl)}};
  p.stdout.on('data',onD);p.stderr.on('data',onD);
  p.on('close',()=>setTimeout(startTunnel,5000));
}

server.listen(cfg.port,()=>{
  console.log(`Panel jalan di http://localhost:${cfg.port}`);
  for(const s of db.servers){
    if(typeOf(s)==='web')P(s.id).status='running';
    else if(s.autostart!==false)startSrv(s.id);
  }
  if(cfg.telegramToken)tgPoll();
  if(cfg.cloudflared&&!cfg.domain)startTunnel();
});
process.on('SIGINT',()=>{db.servers.forEach(s=>stopSrv(s.id,'SIGKILL'));process.exit()});
