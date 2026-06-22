const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// 1. FIXED CORS DOMAIN POLICY
const ALLOWED_ORIGINS = [
    "https://strong-crepe-37984f.netlify.app", // Clean production link matching browser headers
    "http://localhost:5500",                   // Local Live Server frontend mapping
    "http://127.0.0.1:5500",                   // Alternate local standard IP mapping
    "http://localhost:3000",                   // Catch-all for alternate local frameworks
    "http://127.0.0.1:3000"
];

const io = new Server(server, {
    cors: {
        origin: (origin, callback) => {
            if (!origin) return callback(null, true);
            
            const isAllowed = ALLOWED_ORIGINS.includes(origin) || 
                              origin.startsWith("http://localhost:") || 
                              origin.startsWith("http://127.0.0.1:");

            if (isAllowed) {
                callback(null, true); 
            } else {
                console.warn(`Rejected Origin Attempt: ${origin}`);
                callback(new Error("Security Policy Enforcement: Unauthorized Origin Blocked"));
            }
        },
        methods: ["GET", "POST"],
        credentials: true
    }
});

const activeUsers = new Map(); // socket.id -> { number, username }
const activeChats = new Map(); // roomName -> { messages, createdAt }

function getSocketIdByNumber(number) {
    const entry = Array.from(activeUsers.entries()).find(([_, data]) => data.number === number);
    return entry ? entry[0] : null;
}

function generate10DigitNumber() {
    let num;
    const existingNumbers = Array.from(activeUsers.values()).map(u => u.number);
    do {
        num = Math.floor(1000000000 + Math.random() * 9000000000).toString();
    } while (existingNumbers.includes(num));
    return num;
}

io.on('connection', (socket) => {
    // 2. HANDSHAKE FAIL-SAFE CHECK
    const clientOrigin = socket.handshake.headers.origin;
    if (clientOrigin) {
        const isAllowed = ALLOWED_ORIGINS.includes(clientOrigin) || 
                          clientOrigin.startsWith("http://localhost:") || 
                          clientOrigin.startsWith("http://127.0.0.1:");
                          
        if (!isAllowed) {
            console.warn(`🚨 Security Breach Attempt: Connection terminated from unauthorized origin: ${clientOrigin}`);
            socket.disconnect(true);
            return;
        }
    }

    console.log(`User connected successfully: ${socket.id}`);

    // 3. PACKET-LEVEL EVENT RATE LIMITING
    socket.actionCount = 0;
    socket.lastResetTime = Date.now();

    socket.use(([event, ...args], next) => {
        const now = Date.now();
        if (now - socket.lastResetTime > 10000) {
            socket.actionCount = 0;
            socket.lastResetTime = now;
        }

        socket.actionCount++;

        if (socket.actionCount > 25) {
            console.warn(`🚨 Rate limit exceeded by socket: ${socket.id}. Forced disconnect executed.`);
            socket.emit('error_message', { message: 'Rate limit exceeded. Connection throttled.' });
            socket.disconnect(true);
            return;
        }
        next();
    });

    // Registration handler (Only path where a new NOMBER is ever generated)
    socket.on('register_user', ({ username }) => {
        if (!username || username.trim().length === 0) return;
        
        const userNumber = generate10DigitNumber();
        activeUsers.set(socket.id, { number: userNumber, username: username.trim() });
        socket.emit('assigned_credentials', { number: userNumber, username: username.trim() });
    });

    // Revisit profile recovery handler
    socket.on('restore_profile', ({ number, username }) => {
        if (!number || !username) return;

        const oldSocketId = getSocketIdByNumber(number);
        // If they are logging in on a new socket tab, wipe the stale mapping link
        if (oldSocketId && oldSocketId !== socket.id) {
            activeUsers.delete(oldSocketId);
        }
        
        // Save state cleanly bound strictly to the current active socket id
        activeUsers.set(socket.id, { number: number.toString(), username: username.trim() });
        socket.emit('profile_restored_confirm', { number: number.toString(), username: username.trim() });
    });

    // FIXED PROFILE UPDATE HANDLER (Explicitly preserves existing NOMBER tracking string)
    socket.on('update_profile', ({ newUsername }) => {
        if (!newUsername || newUsername.trim().length === 0) return;

        const currentUser = activeUsers.get(socket.id);
        if (currentUser) {
            // Keep the exact original persistent number, only alter the display name payload property
            const preservedNumber = currentUser.number;
            currentUser.username = newUsername.trim();

            // Explicitly force state mapping confirmation down pipeline
            socket.emit('profile_updated_confirm', { 
                number: preservedNumber, 
                username: currentUser.username 
            });
        } else {
            // If socket lost state in memory, tell client to reauth without breaking variables
            socket.emit('request_reauth');
        }
    });

    socket.on('delete_profile_data', () => {
        activeUsers.delete(socket.id);
        socket.emit('profile_deleted_confirm');
    });

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

        socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username, initiator: true });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username, initiator: false });
    });

    socket.on('send_message', ({ roomName, message }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) {
            socket.emit('request_reauth');
            return;
        }

        if (!message || message.trim().length === 0 || !roomName) return;

        const verifiedPeers = roomName.split('_');
        if (!verifiedPeers.includes(currentUser.number)) {
            socket.emit('error_message', { message: 'Unauthorized pipeline. Access denied to target channel room.' });
            return;
        }

        const msgData = {
            roomName,
            sender: currentUser.number,
            text: message.trim(),
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
    });
});

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
