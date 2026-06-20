const BACKEND_URL = "https://flashchat-backend-1.onrender.com";
const socket = io(BACKEND_URL, { autoConnect: false });

let myNumber = localStorage.getItem('flash_my_number') || null;
let myUsername = localStorage.getItem('flash_my_username') || null;
let activeRoomId = null;

let roomsData = JSON.parse(localStorage.getItem('flash_rooms_v2')) || {};

// DOM Selectors
const introScreen = document.getElementById('intro-screen');
const dashboard = document.getElementById('dashboard');
const usernameInput = document.getElementById('username-input');
const joinBtn = document.getElementById('join-btn');
const displayMyName = document.getElementById('display-my-name');
const displayMyNumber = document.getElementById('display-my-number');
const peerInput = document.getElementById('peer-number-input');
const connectBtn = document.getElementById('connect-btn');
const chatListContainer = document.getElementById('chat-list');
const sidebarPanel = document.getElementById('sidebar-panel');
const chatPanel = document.getElementById('chat-panel');
const blankState = document.getElementById('blank-state');
const activeChatContainer = document.getElementById('active-chat-container');
const chatWithName = document.getElementById('chat-with-name');
const chatWithNumber = document.getElementById('chat-with-number');
const messagesContainer = document.getElementById('chat-messages');
const messageInput = document.getElementById('message-input');
const sendBtn = document.getElementById('send-btn');
const backToListBtn = document.getElementById('back-to-list-btn');
const attachBtn = document.getElementById('attach-btn');
const imageInputFile = document.getElementById('image-input-file');

// Settings Selectors
const settingsModal = document.getElementById('settings-modal');
const openSettingsBtn = document.getElementById('open-settings-btn');
const closeSettingsBtn = document.getElementById('close-settings-btn');
const settingsUsernameInput = document.getElementById('settings-username-input');
const saveProfileBtn = document.getElementById('save-profile-btn');
const deleteProfileBtn = document.getElementById('delete-profile-btn');

// FIX: Streamlined connection keeper that auto-manages the handshake seamlessly
function ensureServerConnection() {
    if (!socket.connected) {
        socket.connect();
    }
}

// Auto-restore login on connection drops or background wakes
socket.on('connect', () => {
    if (myNumber && myUsername) {
        socket.emit('restore_profile', { number: myNumber, username: myUsername });
        
        // Re-sync rooms with backend after reconnection
        for (const roomId in roomsData) {
            socket.emit('connect_to_peer', { peerNumber: roomsData[roomId].peerNumber });
        }
    }
});

// Force re-auth if server requests it dynamically
socket.on('request_reauth', () => {
    if (myNumber && myUsername) {
        socket.emit('restore_profile', { number: myNumber, username: myUsername });
    }
});

// Initial Page Load Handler Check
window.addEventListener('DOMContentLoaded', () => {
    if (myNumber && myUsername) {
        displayMyName.textContent = myUsername;
        displayMyNumber.textContent = `NOMBER: ${myNumber}`;
        introScreen.classList.add('hidden');
        dashboard.classList.remove('hidden');
        renderSidebarList();
        
        // Open connection pipeline immediately
        ensureServerConnection();
    }
});

joinBtn.addEventListener('click', () => {
    const name = usernameInput.value.trim();
    if (name.length > 0) {
        ensureServerConnection();
        socket.emit('register_user', { username: name });
    }
});

socket.on('assigned_credentials', ({ number, username }) => {
    myNumber = number;
    myUsername = username;
    localStorage.setItem('flash_my_number', number);
    localStorage.setItem('flash_my_username', username);
    
    displayMyName.textContent = username;
    displayMyNumber.textContent = `NOMBER: ${number}`;
    
    introScreen.classList.add('hidden');
    dashboard.classList.remove('hidden');
    renderSidebarList();
});

connectBtn.addEventListener('click', () => {
    const targetNum = peerInput.value.trim();
    if (targetNum.length === 10 && targetNum !== myNumber) {
        ensureServerConnection();
        socket.emit('connect_to_peer', { peerNumber: targetNum });
        peerInput.value = "";
    }
});

socket.on('peer_connected', ({ roomName, peerNumber, peerUsername, initiator }) => {
    if (!roomsData[roomName]) {
        roomsData[roomName] = {
            peerUsername: peerUsername,
            peerNumber: peerNumber,
            messages: [],
            expired: false,
            showInvitationText: !initiator 
        };
        saveToStorage();
    }
    renderSidebarList();
    if (initiator) switchActiveChat(roomName);
});

function saveToStorage() {
    localStorage.setItem('flash_rooms_v2', JSON.stringify(roomsData));
}

function renderSidebarList() {
    chatListContainer.innerHTML = "";
    for (const [roomId, data] of Object.entries(roomsData)) {
        const itemDiv = document.createElement('div');
        itemDiv.classList.add('chat-user-item');
        if (roomId === activeRoomId) itemDiv.classList.add('active');
        
        const lastMsg = data.messages[data.messages.length - 1];
        let preview = "";
        
        if (data.expired) {
            preview = "Expired";
        } else if (data.showInvitationText) {
            preview = `📩 ${data.peerUsername} wants to chat you`;
        } else if (lastMsg) {
            preview = lastMsg.text.startsWith('{"type":"image"') ? "📷 Photo Attachment" : lastMsg.text;
        } else {
            preview = "Connected securely. Send a message!";
        }

        itemDiv.innerHTML = `
            <div class="title">${data.peerUsername}</div>
            <div class="preview">${preview}</div>
        `;
        itemDiv.addEventListener('click', () => {
            data.showInvitationText = false;
            saveToStorage();
            switchActiveChat(roomId);
        });
        chatListContainer.appendChild(itemDiv);
    }
}

function switchActiveChat(roomId) {
    activeRoomId = roomId;
    const room = roomsData[roomId];

    sidebarPanel.classList.add('mobile-hidden');
    chatPanel.classList.remove('mobile-hidden');
    blankState.classList.add('hidden');
    activeChatContainer.classList.remove('hidden');
    
    chatWithName.textContent = room.peerUsername;
    chatWithNumber.textContent = `NOMBER: ${room.peerNumber}`;
    
    messageInput.disabled = room.expired;
    sendBtn.disabled = room.expired;
    attachBtn.disabled = room.expired;

    renderActiveMessages();
    renderSidebarList();
}

backToListBtn.addEventListener('click', () => {
    chatPanel.classList.add('mobile-hidden');
    sidebarPanel.classList.remove('mobile-hidden');
    activeRoomId = null;
    renderSidebarList();
});

function renderActiveMessages() {
    messagesContainer.innerHTML = "";
    if (!activeRoomId) return;

    const room = roomsData[activeRoomId];
    room.messages.forEach(msgData => {
        const msgDiv = document.createElement('div');
        msgDiv.classList.add('msg');
        
        if (msgData.isSystem) {
            msgDiv.classList.add('system');
            msgDiv.textContent = msgData.text;
        } else {
            msgDiv.classList.add(msgData.sender === myNumber ? 'me' : 'peer');
            
            if (msgData.text.startsWith('{"type":"image"')) {
                try {
                    const parsedImage = JSON.parse(msgData.text);
                    const imgElement = document.createElement('img');
                    imgElement.src = parsedImage.data;
                    imgElement.style.maxWidth = '100%';
                    imgElement.style.borderRadius = '8px';
                    msgDiv.appendChild(imgElement);
                } catch(e) {
                    msgDiv.textContent = msgData.text;
                }
            } else {
                msgDiv.textContent = msgData.text;
            }
        }
        messagesContainer.appendChild(msgDiv);
    });
    messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

sendBtn.addEventListener('click', dispatchMessage);
messageInput.addEventListener('keypress', (e) => { if(e.key === 'Enter') dispatchMessage(); });

// FIX: Removed the callback bottleneck so message transmissions fire instantaneously
function dispatchMessage() {
    const text = messageInput.value.trim();
    if (text && activeRoomId) {
        ensureServerConnection();
        socket.emit('send_message', { roomName: activeRoomId, message: text });
        messageInput.value = "";
    }
}

socket.on('receive_message', (msgData) => {
    const matchingRoomId = msgData.roomName;
    if (roomsData[matchingRoomId]) {
        roomsData[matchingRoomId].messages.push(msgData);
        saveToStorage();
        if (matchingRoomId === activeRoomId) renderActiveMessages();
        renderSidebarList();
    }
});

attachBtn.addEventListener('click', () => imageInputFile.click());
imageInputFile.addEventListener('change', () => {
    const file = imageInputFile.files[0];
    if (file && activeRoomId) {
        const reader = new FileReader();
        reader.onloadend = () => {
            ensureServerConnection();
            socket.emit('send_message', { 
                roomName: activeRoomId, 
                message: JSON.stringify({ type: 'image', data: reader.result }) 
            });
        };
        reader.readAsDataURL(file);
        imageInputFile.value = "";
    }
});

socket.on('chat_expired', ({ message }) => {
    for (const roomId in roomsData) {
        if (!roomsData[roomId].expired) {
            roomsData[roomId].expired = true;
            roomsData[roomId].messages.push({ text: message, isSystem: true });
            saveToStorage();
            if (roomId === activeRoomId) switchActiveChat(roomId);
            break;
        }
    }
    renderSidebarList();
});

// Settings Events Handles
openSettingsBtn.addEventListener('click', () => {
    settingsUsernameInput.value = myUsername;
    settingsModal.classList.remove('hidden');
});

closeSettingsBtn.addEventListener('click', () => settingsModal.classList.add('hidden'));

saveProfileBtn.addEventListener('click', () => {
    const updatedName = settingsUsernameInput.value.trim();
    if (updatedName.length > 0) {
        ensureServerConnection();
        socket.emit('update_profile', { newUsername: updatedName });
    }
});

socket.on('profile_updated_confirm', ({ username }) => {
    myUsername = username;
    localStorage.setItem('flash_my_username', username);
    displayMyName.textContent = username;
    settingsModal.classList.add('hidden');
    alert("Profile configurations updated successfully.");
});

deleteProfileBtn.addEventListener('click', () => {
    if (confirm("Are you absolutely sure you want to permanently erase your profile identity and all cached chat logs?")) {
        ensureServerConnection();
        socket.emit('delete_profile_data');
    }
});

socket.on('profile_deleted_confirm', () => {
    localStorage.clear();
    sessionStorage.clear();
    myNumber = null;
    myUsername = null;
    activeRoomId = null;
    roomsData = {};
    alert("Your profile and records have been permanently cleared.");
    window.location.reload();
});

socket.on('error_message', ({ message }) => alert(message));
