const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Enable CORS so your Netlify frontend can talk to this backend safely
const io = new Server(server, {
    cors: {
        origin: "*", 
        methods: ["GET", "POST"]
    }
});

const activeUsers = new Map(); // Tracks socket connections and tags
const activeChats = new Map(); // Tracks rooms and message history

// Generate a random unique 4-digit numeric tag
function generateUniqueTag() {
    let tag;
    const existingTags = Array.from(activeUsers.values()).map(u => u.tag);
    do {
        tag = Math.floor(1000 + Math.random() * 9000).toString();
    } while (existingTags.includes(tag));
    return tag;
}

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Assign unique tag immediately on connection
    const userTag = generateUniqueTag();
    activeUsers.set(socket.id, { tag: userTag, joinedAt: Date.now() });
    socket.emit('assigned_tag', { tag: userTag });

    // Handle peer-to-peer pairing request
    socket.on('connect_to_peer', ({ peerTag }) => {
        const peerSocketEntry = Array.from(activeUsers.entries())
            .find(([_, data]) => data.tag === peerTag);

        if (!peerSocketEntry) {
            socket.emit('error_message', { message: 'Tag not found or user has disconnected.' });
            return;
        }

        const peerSocketId = peerSocketEntry[0];
        const roomName = [userTag, peerTag].sort().join('_');
        const targetSocket = io.sockets.sockets.get(peerSocketId);
        
        if (targetSocket) {
            // Force BOTH sockets to enter the room channel immediately
            socket.join(roomName); 
            targetSocket.join(roomName); 

            // Alert each user specifically with their peer's tag
            socket.emit('peer_connected', { roomName, peerTag: peerTag });
            targetSocket.emit('peer_connected', { roomName, peerTag: userTag });
        } else {
            socket.emit('error_message', { message: 'Failed to establish secure room tunnel.' });
        }

        if (!activeChats.has(roomName)) {
            activeChats.set(roomName, { messages: [], createdAt: Date.now() });
        }
    });

    // Handle incoming real-time messages
    socket.on('send_message', ({ roomName, message }) => {
        const chat = activeChats.get(roomName);
        if (chat) {
            // ✅ FIX: Include the exact roomName in the message object
            const msgData = { 
                roomName: roomName, 
                sender: userTag, 
                text: message, 
                timestamp: Date.now() 
            };
            chat.messages.push(msgData);
            
            // Broadcast message back to the active room channel
            io.to(roomName).emit('receive_message', msgData);
        }
    });

    // Clean up connections when user leaves or refreshes tabs
    socket.on('disconnect', () => {
        console.log(`User disconnected: ${socket.id}`);
        activeUsers.delete(socket.id);
    });
}); // <--- THIS WAS THE MISSING CLOSING PROPERTY CAUSING THE CRASH!

// --- THE 30-MINUTE SELF-DESTRUCT MECHANISM ---
setInterval(() => {
    const now = Date.now();
    const THIRTY_MINUTES = 30 * 60 * 1000;

    for (const [roomName, chatData] of activeChats.entries()) {
        if (now - chatData.createdAt > THIRTY_MINUTES) {
            console.log(`⏱️ Wiping expired chat room: ${roomName}`);
            io.to(roomName).emit('chat_expired', { message: 'This chat session has expired after 30 minutes.' });
            activeChats.delete(roomName);
        }
    }
}, 60000); // Runs once every minute

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Server listening on port ${PORT}`);
});