require('dotenv').config();

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const dgram = require('dgram');
const cors = require('cors');
const multer = require('multer');
const { createClient } = require('@supabase/supabase-js');
const { randomUUID } = require('crypto');
const os = require('os');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

app.use(cors());
app.use(express.json());

// ==================================================
// SUPABASE SETUP
// ==================================================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 } // 50 MB
});

const PORT = Number(process.env.PORT) || 3000;
const DISCOVERY_PORT = Number(process.env.DISCOVERY_PORT) || 41234;
const BUCKET = 'files';
let lastLoggedIncomingId = null;

// ==================================================
// HELPERS
// ==================================================
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  const candidates = [];
  for (const name of Object.keys(interfaces)) {
    for (const network of interfaces[name]) {
      if (network.family === 'IPv4' && !network.internal) {
        if (name.startsWith('docker') || name.startsWith('veth') || name.startsWith('br-')) continue;
        candidates.push(network.address);
      }
    }
  }
  return candidates[0] || '127.0.0.1';
}

function getDeviceInfo() {
  let type = 'Computer';
  if (process.platform === 'darwin') type = 'Mac';
  else if (process.platform === 'win32') type = 'Windows PC';
  else if (process.platform === 'linux') type = 'Linux PC';
  return { name: os.hostname(), type: type };
}

// ==================================================
// FILE UPLOAD ROUTES (From File-Transfer-Backend)
// ==================================================
app.post('/upload', upload.single('file'), async (req, res) => {
    try {
      const file = req.file;
      if (!file) return res.status(400).json({ error: 'No file uploaded' });

      const fileId = randomUUID();
      const storagePath = `${fileId}-${file.originalname}`;

      const { error: uploadError } = await supabase.storage.from(BUCKET).upload(storagePath, file.buffer, {
          contentType: file.mimetype || 'application/octet-stream', upsert: false
      });

      if (uploadError) return res.status(500).json({ error: 'Storage upload failed', details: uploadError.message });

      const { data, error: dbError } = await supabase.from('file_links').insert({
            file_name: file.originalname, storage_path: storagePath, consumed: false
      }).select().single();

      if (dbError) {
        await supabase.storage.from(BUCKET).remove([storagePath]);
        return res.status(500).json({ error: 'Database save failed', details: dbError.message });
      }

      // Use the Render URL if available, otherwise local IP
      const baseUrl = process.env.RENDER_EXTERNAL_URL || `http://${getLocalIP()}:${PORT}`;
      const downloadUrl = `${baseUrl}/download/${data.id}`;

      res.json({ success: true, id: data.id, fileName: file.originalname, link: downloadUrl, downloadUrl: downloadUrl });
    } catch (err) {
      res.status(500).json({ error: 'Upload failed', details: err.message });
    }
});

app.get('/incoming', async (req, res) => {
  try {
    const { data, error } = await supabase.from('file_links').select('id, file_name, storage_path').eq('consumed', false).order('id', { ascending: false }).limit(1).maybeSingle();
    if (error || !data) return res.json({ available: false });

    const baseUrl = process.env.RENDER_EXTERNAL_URL || `http://${getLocalIP()}:${PORT}`;
    const downloadUrl = `${baseUrl}/download/${data.id}`;
    res.json({ available: true, id: data.id, fileName: data.file_name, downloadUrl: downloadUrl });
  } catch (err) {
    res.status(500).json({ available: false, error: 'Could not check incoming files' });
  }
});

app.get('/download/:id', async (req, res) => {
  try {
    const { data: record, error } = await supabase.from('file_links').select('*').eq('id', req.params.id).single();
    if (error || !record) return res.status(404).send('File not found');

    const { data: signedData, error: signedError } = await supabase.storage.from(BUCKET).createSignedUrl(record.storage_path, 60 * 60);
    if (signedError || !signedData) {
      const { data: publicUrlData } = supabase.storage.from(BUCKET).getPublicUrl(record.storage_path);
      if (!publicUrlData?.publicUrl) return res.status(404).json({ error: 'Storage object not found' });
      return res.redirect(publicUrlData.publicUrl);
    }
    
    await supabase.from('file_links').update({ consumed: true }).eq('id', record.id);
    res.redirect(signedData.signedUrl);
  } catch (err) { res.status(500).json({ error: 'Download failed' }); }
});

app.post('/dismiss/:id', async (req, res) => {
  try {
    await supabase.from('file_links').update({ consumed: true }).eq('id', req.params.id);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: 'Dismiss failed' }); }
});


// ==================================================
// SOCKET.IO / WEBRTC ROUTING (From Signaling Server)
// ==================================================
const rooms = {};

function isValidRoomId(roomId) {
    return typeof roomId === "string" && roomId.trim().length > 0 && roomId.length <= 100 && !["__proto__", "constructor", "prototype"].includes(roomId);
}
function getRoom(roomId) { return isValidRoomId(roomId) ? rooms[roomId] || null : null; }

app.get("/", (req, res) => {
    res.json({ success: true, message: 'SmartLink Combined Backend is running!', ip: getLocalIP(), port: PORT });
});

io.on("connection", (socket) => {
    socket.on("create-room", (roomId) => {
        if (!isValidRoomId(roomId)) return socket.emit("room-error", "Invalid room ID");
        roomId = roomId.trim();
        if (rooms[roomId]) return socket.emit("room-error", "Room already exists");
        rooms[roomId] = { host: socket.id, viewers: [] };
        socket.join(roomId);
        socket.emit("room-created", { roomId });
    });

    socket.on("request-join", ({ roomId } = {}) => {
        const room = getRoom(roomId);
        if (!room) return socket.emit("room-error", "Room not found");
        if (room.host === socket.id) return socket.emit("room-error", "You are the host");
        if (room.viewers.includes(socket.id)) return socket.emit("join-approved");
        io.to(room.host).emit("join-request", { viewerId: socket.id, socketId: socket.id });
        socket.emit("join-pending");
    });

    socket.on("approve-viewer", ({ roomId, viewerSocketId } = {}) => {
        const room = getRoom(roomId);
        if (!room || room.host !== socket.id || typeof viewerSocketId !== "string") return;
        if (!room.viewers.includes(viewerSocketId)) room.viewers.push(viewerSocketId);
        const viewerSocket = io.sockets.sockets.get(viewerSocketId);
        if (viewerSocket) { viewerSocket.join(roomId); viewerSocket.emit("join-approved"); }
    });

    socket.on("reject-viewer", ({ roomId, viewerSocketId } = {}) => {
        const room = getRoom(roomId);
        if (!room || room.host !== socket.id) return;
        const viewerSocket = io.sockets.sockets.get(viewerSocketId);
        if (viewerSocket) viewerSocket.emit("join-rejected");
    });

    socket.on("webrtc-offer", (data = {}) => {
        const room = getRoom(data.roomId);
        if (!room || room.host !== socket.id || !room.viewers.includes(data.to)) return;
        io.to(data.to).emit("webrtc-offer", { roomId: data.roomId, sdp: data.sdp });
    });

    socket.on("webrtc-answer", (data = {}) => {
        const room = getRoom(data.roomId);
        if (!room || !room.viewers.includes(socket.id)) return;
        io.to(room.host).emit("webrtc-answer", { roomId: data.roomId, from: socket.id, sdp: data.sdp });
    });

    socket.on("webrtc-ice", (data = {}) => {
        const room = getRoom(data.roomId);
        if (!room || typeof data.candidate !== "string") return;
        const candidate = { roomId: data.roomId, sdpMid: data.sdpMid, sdpMLineIndex: data.sdpMLineIndex, candidate: data.candidate };
        if (room.host === socket.id) room.viewers.forEach(v => io.to(v).emit("webrtc-ice", candidate));
        else if (room.viewers.includes(socket.id)) io.to(room.host).emit("webrtc-ice", { ...candidate, from: socket.id });
    });

    socket.on("send-chat", ({ roomId, message, sender } = {}) => {
        const room = getRoom(roomId);
        if (!room || (socket.id !== room.host && !room.viewers.includes(socket.id)) || typeof message !== "string") return;
        socket.to(roomId).emit("receive-chat", { message, sender });
    });

    socket.on("sync-clipboard", ({ roomId, text } = {}) => {
        const room = getRoom(roomId);
        if (!room || (socket.id !== room.host && !room.viewers.includes(socket.id)) || typeof text !== "string") return;
        socket.to(roomId).emit("receive-clipboard", { text });
    });

    socket.on("disconnect", () => {
        for (const roomId in rooms) {
            const room = rooms[roomId];
            if (room.host === socket.id) {
                room.viewers.forEach(v => io.to(v).emit("room-closed"));
                delete rooms[roomId];
            } else {
                room.viewers = room.viewers.filter(id => id !== socket.id);
            }
        }
    });
});

// ==================================================
// UDP DISCOVERY (For local LAN)
// ==================================================
const discoveryServer = dgram.createSocket('udp4');
discoveryServer.on('message', (message, remote) => {
  if (message.toString().trim() !== 'SMARTLINK_DISCOVER') return;
  const device = getDeviceInfo();
  const response = JSON.stringify({ name: device.name, type: device.type, ip: getLocalIP(), port: PORT });
  discoveryServer.send(Buffer.from(response), remote.port, remote.address);
});
discoveryServer.bind(DISCOVERY_PORT, '0.0.0.0');

// ==================================================
// START SERVER
// ==================================================
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Combined Server running on port ${PORT}`);
});