const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Enable Cross-Origin Resource Sharing for all origins
const io = new Server(server, {
    cors: {
        origin: "*", 
        methods: ["GET", "POST"]
    }
});

const activeUsers = new Map(); // socket.id -> { number, username }
const activeChats = new Map(); // roomName -> { messages, createdAt }

// ⚡ WAKE UP GATEWAY: Crucial for waking up Render's free tier immediately on frontend load
app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

// Helper utility to match an active 10-digit NOMBER profile with its active live socket instance
function getSocketIdByNumber(number) {
    const entry = Array.from(activeUsers.entries()).find(([_, data]) => data.number === number);
    return entry ? entry[0] : null;
}

// Generates an isolated, non-colliding random 10-digit identifier string
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

    // Registration Handler: Triggered when user clicks "Generate NOMBER Profile"
    socket.on('register_user', ({ username }) => {
        if (!username || username.trim() === "") {
            return socket.emit('error_message', { message: "Username cannot be empty." });
        }
        
        const userNumber = generate10DigitNumber();
        activeUsers.set(socket.id, { number: userNumber, username: username.trim() });
        
        // Emits 'assigned_credentials' to match the frontend app.js event targets perfectly
        socket.emit('assigned_credentials', { number: userNumber, username: username.trim() });
    });

    // Session Recovery Hook: Re-binds identity parameters cleanly when client refreshes or changes network lines
    socket.on('restore_profile', ({ number, username }) => {
        if (!number || !username) return;

        // Garbage collection: If this profile number is still tied to a dead socket, drop it first
        const oldSocketId = getSocketIdByNumber(number);
        if (oldSocketId && oldSocketId !== socket.id) {
            activeUsers.delete(oldSocketId);
        }
        
        activeUsers.set(socket.id, { number, username: username.trim() });
        socket.emit('assigned_credentials', { number, username: username.trim() });
    });

    // Profile Customization Hook: Updates username while preserving the persistent numeric ID
    socket.on('update_profile', ({ newUsername }) => {
        const user = activeUsers.get(socket.id);
        if (user && newUsername && newUsername.trim() !== "") {
            user.username = newUsername.trim();
            socket.emit('profile_updated_confirm', { username: user.username });
        }
    });

    // Explicit Deletion Hook: Completely unbinds records from server state
    socket.on('delete_profile_data', () => {
        activeUsers.delete(socket.id);
        socket.emit('profile_deleted_confirm');
    });

    // Peer Line Sync Handler: Subscribes both independent channels to an isolated room
    socket.on('connect_to_peer', ({ peerNumber }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) return;

        const peerSocketId = getSocketIdByNumber(peerNumber);
        if (!peerSocketId) {
            socket.emit('error_message', { message: 'The requested NOMBER is currently offline or invalid.' });
            return;
        }

        const peerData = activeUsers.get(peerSocketId);
        // Create an ordered, standardized room format (e.g. "1234567890_0987654321")
        const roomName = [currentUser.number, peerData.number].sort().join('_');

        socket.join(roomName);
        io.sockets.sockets.get(peerSocketId)?.join(roomName);

        // Echo responses back to build localized state profiles on the frontend layout
        socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username, initiator: true });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username, initiator: false });
    });

    // Message Broadcasting Engine
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
        
        // Deliver message structure instantly to everyone currently listening inside the room
        io.to(roomName).emit('receive_message', msgData);
    });

    // ⚡ FIXED GARBAGE COLLECTION: Removes the transient map hook immediately on connection drop
    socket.on('disconnect', () => {
        console.log(`User disconnected: ${socket.id}`);
        activeUsers.delete(socket.id);
    });
});

// Ephemeral Sweeper Engine: Tracks room expiration intervals to delete historical message payloads
setInterval(() => {
    const now = Date.now();
    const THIRTY_MINUTES = 30 * 60 * 1000;
    
    for (const [roomName, chatData] of activeChats.entries()) {
        if (now - chatData.createdAt > THIRTY_MINUTES) {
            io.to(roomName).emit('chat_expired', { message: 'This ephemeral conversation has expired.' });
            activeChats.delete(roomName);
        }
    }
}, 60000); // Evaluates state conditions every 60 seconds

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 FlashChat Backend Engine live on port ${PORT}`));
