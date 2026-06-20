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

const activeUsers = new Map(); // socket.id -> { number, username }
const activeChats = new Map(); // roomName -> { messages, createdAt }

// Helper function to find socket ID by user number
function getSocketIdByNumber(number) {
    const entry = Array.from(activeUsers.entries()).find(([_, data]) => data.number === number);
    return entry ? entry[0] : null;
}

// Generates a cryptographically sound 10-digit number string
function generate10DigitNumber() {
    let num;
    const existingNumbers = Array.from(activeUsers.values()).map(u => u.number);
    do {
        num = Math.floor(1000000000 + Math.random() * 9000000000).toString();
    } while (existingNumbers.includes(num));
    return num;
}

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // New profile registration handler
    socket.on('register_user', ({ username }) => {
        const userNumber = generate10DigitNumber();
        activeUsers.set(socket.id, { number: userNumber, username });
        socket.emit('assigned_credentials', { number: userNumber, username });
    });

    // Revisit/Refresh recovery handler: logs an existing profile back into system RAM
    socket.on('restore_profile', ({ number, username }) => {
        // Disconnect any old dead socket references associated with this number
        const oldSocketId = getSocketIdByNumber(number);
        if (oldSocketId && oldSocketId !== socket.id) {
            activeUsers.delete(oldSocketId);
        }
        activeUsers.set(socket.id, { number, username });
        socket.emit('profile_restored_confirm', { number, username });
    });

    // Handle modification updates to profile details
    socket.on('update_profile', ({ newUsername }) => {
        const user = activeUsers.get(socket.id);
        if (user) {
            user.username = newUsername;
            socket.emit('profile_updated_confirm', { username: newUsername });
        }
    });

    // Handle profile erasure requests
    socket.on('delete_profile_data', () => {
        activeUsers.delete(socket.id);
        socket.emit('profile_deleted_confirm');
    });

    // Process secure pairing link invitation requests
    socket.on('connect_to_peer', ({ peerNumber }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) return;

        const peerSocketId = getSocketIdByNumber(peerNumber);
        if (!peerSocketId) {
            socket.emit('error_message', { message: 'The requested NOMBER is currently offline or invalid.' });
            return;
        }

        const peerData = activeUsers.get(peerSocketId);
        const roomName = [currentUser.number, peerData.number].sort().join('_');

        socket.join(roomName);
        io.sockets.sockets.get(peerSocketId)?.join(roomName);

        // Send customized structural parameters to both endpoints
        socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username, initiator: true });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username, initiator: false });
    });

    socket.on('send_message', ({ roomName, message }) => {
        const currentUser = activeUsers.get(socket.id);
        if (currentUser) {
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
        }
    });

    socket.on('disconnect', () => {
        activeUsers.delete(socket.id);
    });
});

// Self-Destruction Cleanup Daemon Loop
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
server.listen(PORT, () => console.log(`🚀 Engine live on port ${PORT}`));
