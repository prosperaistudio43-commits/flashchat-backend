const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*", 
        methods: ["GET", "POST"]
    }
});

const activeUsers = new Map(); // Tracks socket.id -> { number, username, joinedAt }
const activeChats = new Map(); // Tracks rooms -> { messages, createdAt }

function generateUniqueNumber() {
    let num;
    const existingNumbers = Array.from(activeUsers.values()).map(u => u.number);
    do {
        num = Math.floor(1000 + Math.random() * 9000).toString();
    } while (existingNumbers.includes(num));
    return num;
}

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Register user with a username
    socket.on('register_user', ({ username }) => {
        const userNumber = generateUniqueNumber();
        activeUsers.set(socket.id, { number: userNumber, username: username, joinedAt: Date.now() });
        
        // Send registration details back to client
        socket.emit('assigned_credentials', { number: userNumber, username });
    });

    // Handle pairing request via unique Number
    socket.on('connect_to_peer', ({ peerNumber }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) return;

        const peerEntry = Array.from(activeUsers.entries())
            .find(([_, data]) => data.number === peerNumber);

        if (!peerEntry) {
            socket.emit('error_message', { message: 'Number not found or user offline.' });
            return;
        }

        const peerSocketId = peerEntry[0];
        const peerData = peerEntry[1];
        
        // Unique room identifier sorted by their unique numbers
        const roomName = [currentUser.number, peerData.number].sort().join('_');
        const targetSocket = io.sockets.sockets.get(peerSocketId);
        
        if (targetSocket) {
            socket.join(roomName); 
            targetSocket.join(roomName); 

            // Exchange structural metadata profiles
            socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username });
            targetSocket.emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username });
        }

        if (!activeChats.has(roomName)) {
            activeChats.set(roomName, { messages: [], createdAt: Date.now() });
        }
    });

    socket.on('send_message', ({ roomName, message }) => {
        const currentUser = activeUsers.get(socket.id);
        if (currentUser && activeChats.has(roomName)) {
            const msgData = { 
                roomName: roomName, 
                sender: currentUser.number, 
                text: message, 
                timestamp: Date.now() 
            };
            activeChats.get(roomName).messages.push(msgData);
            io.to(roomName).emit('receive_message', msgData);
        }
    });

    socket.on('disconnect', () => {
        activeUsers.delete(socket.id);
    });
});

// 30-Minute Expiration Core Loop
setInterval(() => {
    const now = Date.now();
    const THIRTY_MINUTES = 30 * 60 * 1000;
    for (const [roomName, chatData] of activeChats.entries()) {
        if (now - chatData.createdAt > THIRTY_MINUTES) {
            io.to(roomName).emit('chat_expired', { message: 'This ephemeral session has expired.' });
            activeChats.delete(roomName);
        }
    }
}, 60000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Engine live on port ${PORT}`));
