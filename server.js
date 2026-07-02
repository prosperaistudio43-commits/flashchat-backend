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

// Expose a quick HTTP endpoint to wake up / ping the server
app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

// Dual-mapping to handle dynamic WebSockets reliably
const usersByNumber = new Map(); // number -> { username, socketId }
const numberBySocket = new Map(); // socketId -> number
const activeChats = new Map();    // roomName -> { messages, createdAt }

function generate10DigitNumber() {
    let num;
    do {
        num = Math.floor(1000000000 + Math.random() * 9000000000).toString();
    } while (usersByNumber.has(num)); // Secure check against persistent store
    return num;
}

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Registration handler
    socket.on('register_user', ({ username }) => {
        if (!username || username.trim() === "") {
            return socket.emit('error_message', { message: "Invalid username input." });
        }
        
        const userNumber = generate10DigitNumber();
        
        // Save bindings
        usersByNumber.set(userNumber, { username: username.trim(), socketId: socket.id });
        numberBySocket.set(socket.id, userNumber);

        socket.emit('profile_generated', { 
            number: userNumber, 
            username: username.trim() 
        });
    });

    // Persistent Check Auth / Session Recovery
    socket.on('check_auth', ({ number, username }) => {
        if (!number || !username) {
            return socket.emit('auth_failed');
        }

        // Re-bind the new socket ID to the persistent identity
        usersByNumber.set(number, { username: username, socketId: socket.id });
        numberBySocket.set(socket.id, number);

        socket.emit('auth_verified', { number, username });

        // Auto-restore room subscriptions for this new socket
        for (const roomName of activeChats.keys()) {
            if (roomName.includes(number)) {
                socket.join(roomName);
            }
        }
    });

    // Profile updates keep the numeric ID persistent
    socket.on('update_profile', ({ newUsername }) => {
        const userNumber = numberBySocket.get(socket.id);
        if (!userNumber || !newUsername) return;

        usersByNumber.set(userNumber, { username: newUsername.trim(), socketId: socket.id });
        socket.emit('profile_updated_confirm', { username: newUsername.trim() });
    });

    // Clean up profile data explicitly
    socket.on('delete_profile_data', () => {
        const userNumber = numberBySocket.get(socket.id);
        if (userNumber) {
            usersByNumber.delete(userNumber);
        }
        numberBySocket.delete(socket.id);
        socket.emit('profile_deleted_confirm');
    });

    // Connection matching handler
    socket.on('connect_peer', ({ peerNumber }) => {
        const myNumber = numberBySocket.get(socket.id);
        if (!myNumber) {
            return socket.emit('error_message', { message: "Session expired. Reauthenticating..." });
        }

        const peerData = usersByNumber.get(peerNumber);
        if (!peerData) {
            return socket.emit('error_message', { message: "Target profile number could not be found." });
        }

        const currentUser = usersByNumber.get(myNumber);
        const roomName = [myNumber, peerNumber].sort().join('-');

        socket.join(roomName);
        
        // Get peer's active socket connection
        const peerSocketId = peerData.socketId;
        const peerSocket = io.sockets.sockets.get(peerSocketId);
        
        if (peerSocket) {
            peerSocket.join(roomName);
        }

        // Sync old chat logs if room history exists
        const structuralHistory = activeChats.has(roomName) ? activeChats.get(roomName).messages : [];
        
        socket.emit('peer_connected', { roomName, peerNumber, peerUsername: peerData.username, initiator: true, history: structuralHistory });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: myNumber, peerUsername: currentUser.username, initiator: false, history: structuralHistory });
    });

    socket.on('send_message', ({ roomName, message }) => {
        const userNumber = numberBySocket.get(socket.id);
        if (!userNumber) {
            return socket.emit('request_reauth');
        }

        const msgData = {
            roomName,
            sender: userNumber,
            text: message,
            timestamp: Date.now()
        };

        if (!activeChats.has(roomName)) {
            activeChats.set(roomName, { messages: [], createdAt: Date.now() });
        }
        activeChats.get(roomName).messages.push(msgData);
        io.to(roomName).emit('receive_message', msgData);
    });

    // Garbage collection on connection loss
    socket.on('disconnect', () => {
        console.log(`User disconnected: ${socket.id}`);
        const userNumber = numberBySocket.get(socket.id);
        
        // Remove transient socket mapping, keep persistent user registry intact
        if (userNumber) {
            const userData = usersByNumber.get(userNumber);
            if (userData && userData.socketId === socket.id) {
                // Set socketId to null indicating they are offline but registered
                userData.socketId = null; 
            }
        }
        numberBySocket.delete(socket.id);
    });
});

// Periodic Sweeper Routine (Stops memory leaks)
setInterval(() => {
    const now = Date.now();
    const THIRTY_MINUTES = 30 * 60 * 1000;
    for (const [roomName, chatData] of activeChats.entries()) {
        if (now - chatData.createdAt > THIRTY_MINUTES) {
            io.to(roomName).emit('room_expired');
            activeChats.delete(roomName);
        }
    }
}, 60000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`⚡ Backend Core running smoothly on port ${PORT}`);
});
