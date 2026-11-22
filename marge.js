const fs=require('fs'),path=require('path'),crypto=require('crypto');
const [,,SRC,DST]=process.argv;
if(!SRC||!DST){console.log('Pakai: node merge.js <data sumber> <data tujuan>');process.exit(1)}
const sdbFile=path.join(SRC,'db.json');
if(!fs.existsSync(sdbFile)){console.log('db.json sumber tidak ditemukan di',SRC);process.exit(1)}
const sdb=JSON.parse(fs.readFileSync(sdbFile));
fs.mkdirSync(path.join(DST,'servers'),{recursive:true});
const tf=path.join(DST,'db.json');
const tdb=fs.existsSync(tf)?JSON.parse(fs.readFileSync(tf)):{users:[],servers:[]};
const rnd=()=>crypto.randomBytes(4).toString('hex');
const uMap={};

for(const u of sdb.users){
  const ex=tdb.users.find(x=>x.username===u.username);
  if(ex){uMap[u.id]=ex.id;console.log('User sudah ada, dipakai ulang:',u.username);continue}
  const nu={...u,id:tdb.users.some(x=>x.id===u.id)?rnd():u.id};
  if(nu.tg&&tdb.users.some(x=>x.tg===nu.tg))nu.tg=null;
  tdb.users.push(nu);uMap[u.id]=nu.id;console.log('User ditambahkan:',u.username);
}

let nextPort=Math.max(20000,...tdb.servers.map(s=>(s.port||0)+1));
for(const s of sdb.servers){
  const id=tdb.servers.some(x=>x.id===s.id)?rnd():s.id;
  const ns={...s,id,owner:uMap[s.owner]||s.owner,port:nextPort++};
  const from=path.join(SRC,'servers',s.id),to=path.join(DST,'servers',id);
  if(fs.existsSync(from))fs.cpSync(from,to,{recursive:true});
  else fs.mkdirSync(to,{recursive:true});
  tdb.servers.push(ns);console.log('Server ditambahkan:',s.name,'-> kode',id);
}

fs.writeFileSync(tf,JSON.stringify(tdb,null,1));
console.log('Selesai. Total user:',tdb.users.length,'| total server:',tdb.servers.length);
