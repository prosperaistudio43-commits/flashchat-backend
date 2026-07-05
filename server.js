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
    // NOTE: default maxHttpBufferSize is ~1MB. Avatars and image messages must
    // stay compressed client-side (see compressImageToDataUrl in app.js) or
    // the socket connection for that user gets silently dropped mid-send.
});

const activeUsers = new Map(); // socket.id -> { number, username, avatar }
const activeChats = new Map(); // roomName ("num_num") -> { messages, createdAt }
const userRooms = new Map();   // number -> Set of 1:1 roomNames this user belongs to

// ==========================================================================
// 👥 GROUP CHAT STATE
// Group rooms are namespaced with a "grp_" prefix so their ids can never
// collide with a 1:1 room name (which is always "<lowerNumber>_<higherNumber>").
// That prefix is also how send_message/leave_room tell the two apart without
// needing a separate set of client-facing events.
// ==========================================================================
const groups = new Map();      // groupId -> { groupId, groupName, members: Set(number), memberInfo: Map(number -> {username, avatar}), createdAt, messages }
const userGroups = new Map();  // number -> Set of groupIds this user belongs to

const GROUP_MAX_MEMBERS = 100;
const GROUP_LIFETIME_MS = 24 * 60 * 60 * 1000; // 24 hours
const DIRECT_CHAT_LIFETIME_MS = 30 * 60 * 1000; // unchanged, existing behavior

const GROUP_ADJECTIVES = ['Wild', 'Neon', 'Electric', 'Midnight', 'Savage', 'Golden', 'Silent', 'Cosmic', 'Rebel', 'Phantom', 'Feral', 'Rogue', 'Chaotic', 'Velvet', 'Crimson', 'Static'];
const GROUP_NOUNS = ['Foxes', 'Wolves', 'Vibes', 'Circle', 'Squad', 'Tribe', 'Legends', 'Coven', 'Crew', 'Society', 'Collective', 'Gang', 'Syndicate', 'Council', 'Order', 'Frequency'];

function generateGroupName() {
    const adj = GROUP_ADJECTIVES[Math.floor(Math.random() * GROUP_ADJECTIVES.length)];
    const noun = GROUP_NOUNS[Math.floor(Math.random() * GROUP_NOUNS.length)];
    return `${adj} ${noun}`;
}

function generateGroupId() {
    let id;
    do {
        id = 'grp_' + Math.random().toString(36).slice(2, 10);
    } while (groups.has(id));
    return id;
}

function addUserGroup(number, groupId) {
    if (!userGroups.has(number)) userGroups.set(number, new Set());
    userGroups.get(number).add(groupId);
}

function serializeGroupMembers(group) {
    return Array.from(group.memberInfo.entries()).map(([number, info]) => ({
        number,
        username: info.username,
        avatar: info.avatar || null
    }));
}

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
        activeUsers.set(socket.id, { number: userNumber, username, avatar: null });
        socket.emit('assigned_credentials', { number: userNumber, username });
    });

    // Revisit profile recovery handler
    socket.on('restore_profile', ({ number, username, avatar }) => {
        const oldSocketId = getSocketIdByNumber(number);
        const priorAvatar = oldSocketId ? activeUsers.get(oldSocketId)?.avatar : null;
        if (oldSocketId && oldSocketId !== socket.id) {
            activeUsers.delete(oldSocketId);
        }
        activeUsers.set(socket.id, { number, username, avatar: avatar || priorAvatar || null });

        // ⚡ CRITICAL FIX: rooms are tied to the socket connection, not the user's
        // number. A reconnect gets a brand-new socket.id that isn't a member of
        // any Socket.IO room yet, even though the client still thinks it's in
        // the conversation. Without rejoining here, io.to(roomName).emit(...)
        // silently stops reaching this user after any disconnect/reconnect.
        const rooms = userRooms.get(number);
        if (rooms) {
            rooms.forEach(roomName => socket.join(roomName));
        }

        // Same fix applies to group rooms — rejoin every group this number
        // still belongs to so group broadcasts keep reaching this socket.
        const gIds = userGroups.get(number);
        if (gIds) {
            gIds.forEach(groupId => {
                const group = groups.get(groupId);
                if (!group) return;
                socket.join(groupId);
                // keep group roster's username/avatar fresh across reconnects
                if (group.memberInfo.has(number)) {
                    group.memberInfo.set(number, { username, avatar: avatar || priorAvatar || null });
                }
            });
        }

        socket.emit('profile_restored_confirm', { number, username });
    });

    socket.on('update_profile', ({ newUsername }) => {
        const user = activeUsers.get(socket.id);
        if (user) {
            user.username = newUsername;
            const gIds = userGroups.get(user.number);
            if (gIds) {
                gIds.forEach(groupId => {
                    const group = groups.get(groupId);
                    if (group && group.memberInfo.has(user.number)) {
                        group.memberInfo.get(user.number).username = newUsername;
                    }
                });
            }
            socket.emit('profile_updated_confirm', { username: newUsername });
        }
    });

    // ⚡ AVATAR: stored as a compressed base64 data URL on the in-memory user
    // record, mirrored into any group rosters, and pushed out to everyone the
    // user currently shares a 1:1 or group room with, so their picture updates
    // live for people already chatting with them.
    socket.on('update_avatar', ({ avatarData }) => {
        const user = activeUsers.get(socket.id);
        if (!user) return;
        user.avatar = avatarData || null;

        const gIds = userGroups.get(user.number);
        if (gIds) {
            gIds.forEach(groupId => {
                const group = groups.get(groupId);
                if (group && group.memberInfo.has(user.number)) {
                    group.memberInfo.get(user.number).avatar = user.avatar;
                }
                socket.to(groupId).emit('peer_avatar_updated', { number: user.number, avatar: user.avatar, groupId });
            });
        }

        const rooms = userRooms.get(user.number);
        if (rooms) {
            rooms.forEach(roomName => {
                const peerNumber = getPeerNumberInRoom(roomName, user.number);
                const peerSocketId = getSocketIdByNumber(peerNumber);
                if (peerSocketId) io.to(peerSocketId).emit('peer_avatar_updated', { number: user.number, avatar: user.avatar });
            });
        }

        socket.emit('avatar_updated_confirm', { avatar: user.avatar });
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

            // Leave every group but don't delete the group itself — the other
            // members' chat should survive this user quitting the app.
            const gIds = userGroups.get(user.number);
            if (gIds) {
                gIds.forEach(groupId => {
                    const group = groups.get(groupId);
                    if (!group) return;
                    group.members.delete(user.number);
                    group.memberInfo.delete(user.number);
                    io.to(groupId).emit('group_member_left', { groupId, number: user.number, username: user.username });
                    if (group.members.size === 0) groups.delete(groupId);
                });
            }
            userGroups.delete(user.number);
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

        socket.emit('peer_connected', { roomName, peerNumber: peerData.number, peerUsername: peerData.username, peerAvatar: peerData.avatar || null, initiator: true });
        io.to(peerSocketId).emit('peer_connected', { roomName, peerNumber: currentUser.number, peerUsername: currentUser.username, peerAvatar: currentUser.avatar || null, initiator: false });
    });

    // ==========================================================================
    // 👥 GROUP CHAT EVENTS
    // ==========================================================================

    socket.on('create_group', () => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) return;

        const groupId = generateGroupId();
        const groupName = generateGroupName();
        const group = {
            groupId,
            groupName,
            members: new Set([currentUser.number]),
            memberInfo: new Map([[currentUser.number, { username: currentUser.username, avatar: currentUser.avatar || null }]]),
            createdAt: Date.now(),
            messages: []
        };
        groups.set(groupId, group);
        addUserGroup(currentUser.number, groupId);
        socket.join(groupId);

        socket.emit('group_created', {
            groupId,
            groupName,
            createdAt: group.createdAt,
            members: serializeGroupMembers(group),
            messages: []
        });
    });

    socket.on('join_group', ({ groupId }) => {
        const currentUser = activeUsers.get(socket.id);
        if (!currentUser) return;

        const group = groups.get(groupId);
        if (!group) {
            socket.emit('error_message', { message: 'That group link has expired or no longer exists.' });
            return;
        }

        const alreadyMember = group.members.has(currentUser.number);
        if (!alreadyMember && group.members.size >= GROUP_MAX_MEMBERS) {
            socket.emit('error_message', { message: `This group is full (${GROUP_MAX_MEMBERS}/${GROUP_MAX_MEMBERS} members).` });
            return;
        }

        group.members.add(currentUser.number);
        group.memberInfo.set(currentUser.number, { username: currentUser.username, avatar: currentUser.avatar || null });
        addUserGroup(currentUser.number, groupId);
        socket.join(groupId);

        socket.emit('group_joined', {
            groupId,
            groupName: group.groupName,
            createdAt: group.createdAt,
            members: serializeGroupMembers(group),
            messages: group.messages
        });

        if (!alreadyMember) {
            const joinSystemMsg = {
                roomName: groupId,
                system: true,
                text: `${currentUser.username} in the 4kin chat`,
                timestamp: Date.now()
            };
            group.messages.push(joinSystemMsg);
            socket.to(groupId).emit('group_member_joined', {
                groupId,
                number: currentUser.number,
                username: currentUser.username,
                avatar: currentUser.avatar || null,
                systemMessage: joinSystemMsg
            });
        }
    });

    // Best-effort room cleanup when a user deletes a chat, or leaves a group, on their end.
    socket.on('leave_room', ({ roomName }) => {
        const user = activeUsers.get(socket.id);
        socket.leave(roomName);
        if (!user) return;

        if (groups.has(roomName)) {
            const group = groups.get(roomName);
            group.members.delete(user.number);
            group.memberInfo.delete(user.number);
            userGroups.get(user.number)?.delete(roomName);
            if (group.members.size === 0) {
                groups.delete(roomName);
            } else {
                io.to(roomName).emit('group_member_left', { groupId: roomName, number: user.number, username: user.username });
            }
            return;
        }

        userRooms.get(user.number)?.delete(roomName);
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
            senderUsername: currentUser.username,
            senderAvatar: currentUser.avatar || null,
            text: message,
            timestamp: Date.now()
        };

        // Groups use the "grp_" prefix, so this check alone is enough to route
        // between group storage/broadcast and the existing 1:1 path below.
        if (groups.has(roomName)) {
            const group = groups.get(roomName);
            if (!group.members.has(currentUser.number)) return; // not (or no longer) a member
            group.messages.push(msgData);
            io.to(roomName).emit('receive_message', msgData);
            return;
        }

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

    for (const [roomName, chatData] of activeChats.entries()) {
        if (now - chatData.createdAt > DIRECT_CHAT_LIFETIME_MS) {
            io.to(roomName).emit('chat_expired', { message: 'This ephemeral conversation has expired.' });
            activeChats.delete(roomName);
        }
    }

    // Groups auto-delete 24 hours after creation, regardless of activity —
    // matches the "everything disappears" promise the rest of the app makes.
    for (const [groupId, group] of groups.entries()) {
        if (now - group.createdAt > GROUP_LIFETIME_MS) {
            io.to(groupId).emit('group_expired', { groupId, message: 'This group chat has expired after 24 hours.' });
            group.members.forEach(number => userGroups.get(number)?.delete(groupId));
            groups.delete(groupId);
        }
    }
}, 60000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Engine live on port ${PORT}`));
