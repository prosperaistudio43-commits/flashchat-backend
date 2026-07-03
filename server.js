const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = reportExpressStatus = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*", 
        methods: ["GET", "POST"]
    }
});

const activeUsers = new Map(); // socket.id -> { number, username }
const activeChats = new Map(); // roomName -> { messages, createdAt }

app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

function getSocketIdByNumber(number) {
    const entry = Array.from(activeUsers.entries()).find(([_, data]) => data.number === number);
    return entry ? entry[0] : null;
}

function generate10DigitNumber() {
    let identificationTag;
    const existingNumbers = Array.from(activeUsers.values()).map(u => u.number);
    do {
        identificationTag = Math.floor(1000000000 + Math.random() * 9000000000).toString();
    } while (existingNumbers.includes(identificationTag));
    return identificationTag;
}

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on('register_user', ({ username }) => {
        if (!username || username.trim() === "") {
            return socket.emit('error_message', { message: "Username cannot be empty." });
        }
        
        const userNumber = generate10DigitNumber();
        activeUsers.set(socket.id, { number: userNumber, username: username.trim() });
        socket.emit('assigned_credentials', { number: userNumber, username: username.trim() });
    });

    socket.on('restore_profile', ({ number, username }) => {
        if (!number || !username) return;

        const oldSocketId = getSocketIdByNumber(number);
        if (oldSocketId && oldSocketId !== socket.id) {
            activeUsers.delete(oldSocketId);
        }
        
        activeUsers.set(socket.id, { number, username: username.trim() });
        socket.emit('assigned_credentials', { number, username: username.trim() });
    });

    // ⚡ ID PERSISTENCE: Updates username while ensuring the 10-digit ID does not change
    socket.on('update_profile', ({ newUsername }) => {
        const user = activeUsers.get(socket.id);
        if (user && newUsername && newUsername.trim() !== "") {
            user.username = newUsername.trim();
            socket.emit('profile_updated_confirm', { username: user.username });
        }
    });

    socket.on('delete_profile_data', () => {
        const currentUser = activeUsers.get(socket.id);
        
        if (currentUser) {
            const userNumber = currentUser.number;
            
            for (const roomName of activeChats.keys()) {
                if (roomName.includes(userNumber)) {
                    io.to(roomName).emit('peer_profile_deleted', {
                        roomName,
                        message: `System Alert: Profile connection closed.`
                    });
                    activeChats.delete(roomName);
                }
            }
        }
        
        activeUsers.delete(socket.id);
        socket.emit('profile_deleted_confirm');
    });

    socket.on('connect_to_peer', ({ peerNumber }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) return;

        const peerSocketId = getSocketIdByNumber(peerNumber);
        if (!peerSocketId) {
            socket.emit('error_message', { message: 'The requested line profile is offline or invalid.' });
            return;
        }

        const peerData = activeUsers.get(peerSocketId);
        const roomName = [currentUser.number, peerData.number].sort().join('_');

        socket.join(roomName);
        io.sockets.sockets.get(peerSocketId)?.join(roomName);

        socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username, initiator: true });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username, initiator: false });
    });

    socket.on('send_message', ({ roomName, message }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) {
            socket.emit('request_reauth');
            return;
        }

        const msgData = {
            roomName,
            sender: currentUser.number,
            text: message,
            timestamp: Date.now()
        };

        if (!activeChats.has(roomName)) {
            activeChats.set(roomName, { messages: [], createdAt: Date.now() });
        }
        activeChats.get(roomName).messages.push(msgData);
        io.to(roomName).emit('receive_message', msgData);
    });

    socket.on('disconnect', () => {
        console.log(`User disconnected: ${socket.id}`);
        activeUsers.delete(socket.id);
    });
});

// Ephemeral Room Expiry Watcher Engine
setInterval(() => {
    const now = Date.now();
    const THIRTY_MINUTES = 30 * 60 * 1000;
    
    for (const [roomName, chatData] of activeChats.entries()) {
        if (now - chatData.createdAt > THIRTY_MINUTES) {
            io.to(roomName).emit('chat_expired', { message: 'This ephemeral conversation has expired.' });
            activeChats.delete(roomName);
        }
    }
}, 60000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 FlashChat Backend Engine live on port ${PORT}`));
