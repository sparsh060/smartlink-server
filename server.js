
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const rooms = {};

// Server health check
app.get("/", (req, res) => {
    res.send("SmartLink Signaling Server is running!");
});

io.on("connection", (socket) => {

    console.log("Connected:", socket.id);

    // =====================================
    // 1. CREATE ROOM
    // =====================================

    socket.on("create-room", (roomId) => {

        if (typeof roomId !== "string" || !roomId.trim()) return;

        roomId = roomId.trim();

        // Prevent another host from overwriting an active room
        if (rooms[roomId]) {
            socket.emit("room-error", "Room already exists");
            return;
        }

        rooms[roomId] = {
            host: socket.id,
            viewers: []
        };

        socket.join(roomId);

        console.log("Room created:", roomId);
    });


    // =====================================
    // 2. VIEWER REQUEST TO JOIN
    // =====================================

    socket.on("request-join", ({ roomId, viewerId }) => {

        const room = rooms[roomId];

        if (!room) {
            socket.emit("room-error", "Room not found");
            return;
        }

        io.to(room.host).emit("join-request", {
            viewerId: socket.id,
            socketId: socket.id
        });

        console.log("Join request:", socket.id, "Room:", roomId);
    });


    // =====================================
    // 3. HOST APPROVES VIEWER
    // =====================================

    socket.on("approve-viewer", ({ roomId, viewerSocketId }) => {

        const room = rooms[roomId];

        if (!room || room.host !== socket.id) return;

        if (!room.viewers.includes(viewerSocketId)) {
            room.viewers.push(viewerSocketId);
        }

        const viewerSocket = io.sockets.sockets.get(viewerSocketId);

        if (viewerSocket) {
            viewerSocket.join(roomId);
            viewerSocket.emit("join-approved");
        }

        console.log("Viewer approved:", viewerSocketId);
    });


    // =====================================
    // 4. WEBRTC OFFER
    // =====================================

    socket.on("webrtc-offer", (data) => {

        const room = rooms[data.roomId];

        if (!room || room.host !== socket.id) return;

        if (!room.viewers.includes(data.to)) return;

        io.to(data.to).emit("webrtc-offer", {
            roomId: data.roomId,
            sdp: data.sdp
        });

        console.log("WebRTC offer sent to:", data.to);
    });


    // =====================================
    // 5. WEBRTC ANSWER
    // =====================================

    socket.on("webrtc-answer", (data) => {

        const room = rooms[data.roomId];

        if (!room || !room.viewers.includes(socket.id)) return;

        io.to(room.host).emit("webrtc-answer", {
            roomId: data.roomId,
            sdp: data.sdp
        });

        console.log("WebRTC answer received");
    });


    // =====================================
    // 6. WEBRTC ICE CANDIDATES
    // =====================================

    socket.on("webrtc-ice", (data) => {

        const room = rooms[data.roomId];

        if (!room) return;

        const candidate = {
            roomId: data.roomId,
            sdpMid: data.sdpMid,
            sdpMLineIndex: data.sdpMLineIndex,
            candidate: data.candidate
        };

        // Host sends ICE to all approved viewers
        if (room.host === socket.id) {

            room.viewers.forEach((viewerId) => {
                io.to(viewerId).emit("webrtc-ice", candidate);
            });

        }

        // Viewer sends ICE to Host
        else if (room.viewers.includes(socket.id)) {

            io.to(room.host).emit("webrtc-ice", candidate);

        }

    });


    // =====================================
    // 7. CHAT SYSTEM
    // =====================================

    socket.on("send-chat", ({ roomId, message, sender }) => {

        const room = rooms[roomId];

        if (!room) return;

        if (socket.id !== room.host &&
            !room.viewers.includes(socket.id)) return;

        if (typeof message !== "string" || !message.trim()) return;

        socket.to(roomId).emit("receive-chat", {
            message,
            sender
        });

        console.log("Chat:", sender, message);
    });


    // =====================================
    // 8. CLIPBOARD SHARING
    // =====================================

    socket.on("sync-clipboard", ({ roomId, text }) => {

        const room = rooms[roomId];

        if (!room) return;

        if (socket.id !== room.host &&
            !room.viewers.includes(socket.id)) return;

        if (typeof text !== "string") return;

        socket.to(roomId).emit("receive-clipboard", {
            text
        });

        console.log("Clipboard synced:", roomId);
    });


    // =====================================
    // 9. DISCONNECT CLEANUP
    // =====================================

    socket.on("disconnect", () => {

        console.log("Disconnected:", socket.id);

        for (const roomId in rooms) {

            const room = rooms[roomId];

            // Host disconnected
            if (room.host === socket.id) {

                room.viewers.forEach((viewerId) => {
                    io.to(viewerId).emit("room-closed");
                });

                delete rooms[roomId];

                console.log("Room destroyed:", roomId);

            }

            // Viewer disconnected
            else {

                room.viewers = room.viewers.filter(
                    id => id !== socket.id
                );

            }

        }

    });

});


// =====================================
// 10. START SERVER
// =====================================

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
    console.log("--------------------------------");
    console.log("SmartLink Signaling Server");
    console.log("Port:", PORT);
    console.log("Status: Running");
    console.log("--------------------------------");
});
