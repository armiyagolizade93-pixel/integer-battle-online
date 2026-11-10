const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');
const os = require('os');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: true, credentials: true } });
const PORT = process.env.PORT || 3000;
const rooms = new Map();
const tokenRoom = new Map();
const tokenRole = new Map();
const graceTimers = new Map();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }));
app.get('/lan-info', (_req, res) => {
  const addresses = [];
  for (const list of Object.values(os.networkInterfaces())) for (const item of (list || [])) {
    if (item.family === 'IPv4' && !item.internal && /^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)/.test(item.address)) {
      addresses.push({ ip: item.address, url: `http://${item.address}:${PORT}` });
    }
  }
  res.json({ addresses });
});

function makeCode(){ let code; do { code=String(crypto.randomInt(100000,1000000)); } while(rooms.has(code)); return code; }
function emitRoom(room, event, data){ io.to(room.code).emit(event, data); }
function clearGrace(token){ const t=graceTimers.get(token); if(t) clearTimeout(t); graceTimers.delete(token); }
function reattach(socket, token){
  const code=tokenRoom.get(token), role=tokenRole.get(token), room=rooms.get(code);
  if(!room || !role) return false;
  clearGrace(token); socket.join(code); room[role]=socket.id; room[role+'Token']=token;
  socket.data.token=token; socket.data.room=code; socket.data.role=role;
  socket.emit('room-rejoined',{code,role,state:room.state||null});
  emitRoom(room,'peer-reconnected',{role});
  return true;
}
io.on('connection', socket => {
  const token=String(socket.handshake.auth?.token || '');
  if(token && reattach(socket,token)) return;

  socket.on('create-room', ({name}={}) => {
    if(!token) return socket.emit('room-error',{message:'شناسهٔ اتصال معتبر نیست.'});
    if(socket.data.room) return;
    const code=makeCode(), room={code,host:socket.id,guest:null,hostToken:token,guestToken:null,hostName:String(name||'بازیکن آبی').slice(0,20),guestName:'بازیکن قرمز',state:null};
    rooms.set(code,room); tokenRoom.set(token,code); tokenRole.set(token,'host');
    socket.join(code); socket.data={...socket.data,token,room:code,role:'host'};
    socket.emit('room-created',{code});
  });
  socket.on('join-room', ({code,name}={}) => {
    code=String(code||'').trim(); const room=rooms.get(code);
    if(!room) return socket.emit('room-error',{message:'این اتاق پیدا نشد یا بسته شده است.'});
    if(room.guest && io.sockets.sockets.has(room.guest)) return socket.emit('room-error',{message:'این اتاق دو بازیکن دارد.'});
    if(!token) return socket.emit('room-error',{message:'شناسهٔ اتصال معتبر نیست.'});
    room.guest=socket.id; room.guestToken=token; room.guestName=String(name||'بازیکن قرمز').slice(0,20);
    tokenRoom.set(token,code); tokenRole.set(token,'guest');
    socket.join(code); socket.data={...socket.data,token,room:code,role:'guest'};
    socket.emit('room-joined',{code});
    io.to(room.host).emit('guest-joined',{name:room.guestName});
    if(room.state) socket.emit('host-state',{room:code,state:room.state});
  });
  socket.on('host-state', ({room:code,state}={}) => {
    const room=rooms.get(code); if(!room || socket.data.role!=='host' || socket.data.room!==code) return;
    room.state=state; socket.to(code).emit('host-state',{room:code,state});
  });
  socket.on('remote-answer', ({room:code,index}={}) => {
    const room=rooms.get(code); if(!room || socket.data.role!=='guest' || socket.data.room!==code) return;
    if(!Number.isInteger(index) || index<0 || index>3) return;
    io.to(room.host).emit('remote-answer',{room:code,index});
  });
  socket.on('game-ended', ({room:code}={}) => { const r=rooms.get(code); if(r && socket.data.role==='host') emitRoom(r,'game-ended',{}); });
  socket.on('host-left-game', ({room:code}={}) => { const r=rooms.get(code); if(r && socket.data.role==='host') emitRoom(r,'host-left-game',{}); });
  socket.on('disconnect', () => {
    const {token:t,room:code,role}=socket.data||{}; if(!t || !code || !role) return;
    const r=rooms.get(code); if(!r) return;
    if(r[role]===socket.id) r[role]=null;
    emitRoom(r,'peer-disconnected',{role});
    const timer=setTimeout(()=>{
      const still=rooms.get(code); if(!still) return;
      if(!still.host || !still.guest){
        if(role==='host' && !still.host){ if(still.guest) io.to(still.guest).emit('room-closed',{}); rooms.delete(code); }
        else if(role==='guest' && !still.guest){ still.guestToken=null; tokenRoom.delete(t); tokenRole.delete(t); }
      }
      graceTimers.delete(t);
    },60000);
    graceTimers.set(t,timer);
  });
});
server.listen(PORT,()=>console.log(`Integer Battle online server listening on ${PORT}`));
