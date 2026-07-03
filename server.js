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
const userRooms = new Map();   // number -> Set of roomNames this user belongs to

function getSocketIdByNumber(number) {
    const entry = Array.from(activeUsers.entries()).find(([_, data]) => data.number === number);
    return entry ? entry[0] : null;
}

// Room names are deterministically "<lowerNumber>_<higherNumber>" (see connect_to_peer),
// so the other participant's number can always be derived from the room name itself.
function getPeerNumberInRoom(roomName, myNumber) {
    return roomName.split('_').find(n => n !== myNumber);
}

function addUserRoom(number, roomName) {
    if (!userRooms.has(number)) userRooms.set(number, new Set());
    userRooms.get(number).add(roomName);
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
    console.log(`User connected: ${socket.id}`);

    // Registration handler
    socket.on('register_user', ({ username }) => {
        const userNumber = generate10DigitNumber();
        activeUsers.set(socket.id, { number: userNumber, username });
        socket.emit('assigned_credentials', { number: userNumber, username });
    });

    // Revisit profile recovery handler
    socket.on('restore_profile', ({ number, username }) => {
        const oldSocketId = getSocketIdByNumber(number);
        if (oldSocketId && oldSocketId !== socket.id) {
            activeUsers.delete(oldSocketId);
        }
        activeUsers.set(socket.id, { number, username });

        // ⚡ CRITICAL FIX: rooms are tied to the socket connection, not the user's
        // number. A reconnect gets a brand-new socket.id that isn't a member of
        // any Socket.IO room yet, even though the client still thinks it's in
        // the conversation. Without rejoining here, io.to(roomName).emit(...)
        // silently stops reaching this user after any disconnect/reconnect.
        const rooms = userRooms.get(number);
        if (rooms) {
            rooms.forEach(roomName => socket.join(roomName));
        }

        socket.emit('profile_restored_confirm', { number, username });
    });

    socket.on('update_profile', ({ newUsername }) => {
        const user = activeUsers.get(socket.id);
        if (user) {
            user.username = newUsername;
            socket.emit('profile_updated_confirm', { username: newUsername });
        }
    });

    socket.on('delete_profile_data', () => {
        const user = activeUsers.get(socket.id);
        if (user) {
            const rooms = userRooms.get(user.number);
            if (rooms) {
                rooms.forEach(roomName => {
                    const peerNumber = getPeerNumberInRoom(roomName, user.number);
                    const peerSocketId = getSocketIdByNumber(peerNumber);
                    if (peerSocketId) {
                        io.to(peerSocketId).emit('peer_account_deleted', { peerNumber: user.number });
                    }
                    activeChats.delete(roomName);
                });
            }
            userRooms.delete(user.number);
        }
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
        addUserRoom(currentUser.number, roomName);
        addUserRoom(peerData.number, roomName);

        socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username, initiator: true });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username, initiator: false });
    });

    // Best-effort room cleanup when a user deletes a chat on their end.
    socket.on('leave_room', ({ roomName }) => {
        const user = activeUsers.get(socket.id);
        socket.leave(roomName);
        if (user) {
            userRooms.get(user.number)?.delete(roomName);
        }
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
