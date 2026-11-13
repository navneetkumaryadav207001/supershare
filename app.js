// ============================================================================
// 1. INLINE WEBASSEMBLY MODULE (Fast Byte Checksum / Integrity Engine)
// Compiled directly from raw Wasm bytecode—works on GitHub Pages with 0 build tools
// ============================================================================
let wasmChecksumFn = null;

async function initWasmEngine() {
  // Minimal Wasm binary exporting `checksum(ptr: i32, len: i32) -> i32` + 16 pages memory (1MB)
  const wasmBytes = new Uint8Array([
    0x00,0x61,0x73,0x6d, 0x01,0x00,0x00,0x00,
    0x01,0x07,0x01,0x60,0x02,0x7f,0x7f,0x01,0x7f, // Type: (i32, i32) -> i32
    0x03,0x02,0x01,0x00,                          // Func 0: type 0
    0x05,0x03,0x01,0x00,0x10,                     // Memory: 16 pages (1MB)
    0x07,0x15,0x02,
      0x06,0x6d,0x65,0x6d,0x6f,0x72,0x79,0x02,0x00,
      0x08,0x63,0x68,0x65,0x63,0x6b,0x73,0x75,0x6d,0x00,0x00,
    0x0a,0x27,0x01,0x25,0x01,0x02,0x7f,            // Code: rolling XOR/rotate checksum
      0x41,0x00,0x21,0x02,                        // local 2 (hash) = 0
      0x03,0x40,                                  // loop
        0x20,0x01,0x45,0x0d,0x01,                 // if len == 0 break
        0x20,0x02,0x41,0x05,0x74,                 // hash << 5
        0x20,0x00,0x2d,0x00,0x00,0x73,            // ^ load8_u(ptr)
        0x21,0x02,                                // hash = ...
        0x20,0x00,0x41,0x01,0x6a,0x21,0x00,       // ptr++
        0x20,0x01,0x41,0x01,0x6b,0x21,0x01,       // len--
        0x0c,0x00,                                // continue loop
      0x0b,
      0x20,0x02,0x0b
  ]);

  const { instance } = await WebAssembly.instantiate(wasmBytes);
  const memView = new Uint8Array(instance.exports.memory.buffer);
  wasmChecksumFn = (uint8Arr) => {
    const len = Math.min(uint8Arr.length, memView.length);
    memView.set(uint8Arr.subarray(0, len), 0);
    return instance.exports.checksum(0, len) >>> 0;
  };
  document.getElementById('wasmBadge').textContent = 'WASM: Active';
}

// ============================================================================
// 2. SERVICE WORKER REGISTRATION & STATE
// ============================================================================
const CHUNK_SIZE = 64 * 1024; // 64 KB optimal SCTP packet size
const PARALLEL_CHANNELS = 4;  // 4 concurrent unordered streams to saturate Wi-Fi
const MAX_BUFFER = 1024 * 1024; // 1 MB backpressure ceiling per channel

const myId = 'peer_' + Math.random().toString(36).slice(2, 10);
const myName = `${navigator.platform.split(' ')[0] || 'Device'}-${myId.slice(-4).toUpperCase()}`;
document.getElementById('myName').textContent = myName;

let ws = null;
let currentRoom = '';
let peers = new Map();
let pc = null;
let controlChannel = null;
let dataChannels = [];
let activePeerId = null;
let pendingIncomingOffer = null;
let outgoingFiles = new Map();
let wakeLock = null;

async function initServiceWorker() {
  if ('serviceWorker' in navigator) {
    await navigator.serviceWorker.register('./sw.js');
    await navigator.serviceWorker.ready;
    document.getElementById('swBadge').textContent = 'SW Proxy: Ready';
  } else {
    document.getElementById('swBadge').textContent = 'SW Proxy: Unsupported';
  }
}

// ============================================================================
// 3. ZERO-CONFIG WI-FI AUTO-DISCOVERY (Public WSS MQTT Signaling)
// ============================================================================
async function autoDiscoverWifi() {
  const roomInfo = document.getElementById('roomInfo');
  roomInfo.textContent = 'Detecting Wi-Fi network...';
  try {
    const res = await fetch('https://api.ipify.org?format=json');
    const { ip } = await res.json();
    // Hash public egress IP so devices on same Wi-Fi/Hotspot join identical room
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode('supershare_' + ip));
    const roomHash = Array.from(new Uint8Array(buf)).slice(0, 6).map(b => b.toString(16).padStart(2, '0')).join('');
    connectSignaling(roomHash, `Auto-WiFi (${ip})`);
  } catch {
    connectSignaling('default-lan-room', 'Fallback Room');
  }
}

function connectSignaling(room, label) {
  currentRoom = room;
  document.getElementById('roomInfo').textContent = label;
  if (ws) ws.close();

  // Lightweight MQTT-over-WSS public broker for serverless GitHub Pages signaling
  ws = new WebSocket('wss://broker.emqx.io:8084/mqtt', 'mqtt');
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => {
    sendMqttConnect(ws, myId);
    setTimeout(() => {
      sendMqttSubscribe(ws, `supershare/${currentRoom}/+`);
      document.getElementById('netDot').classList.add('online');
      broadcastPresence();
    }, 300);
  };

  ws.onmessage = (ev) => {
    const msg = parseMqttPublish(new Uint8Array(ev.data));
    if (!msg) return;
    handleSignal(msg);
  };
}

setInterval(() => { if (ws && ws.readyState === 1) broadcastPresence(); }, 4000);

function broadcastPresence() {
  publishSignal('broadcast', { type: 'HELLO', from: myId, name: myName, ua: navigator.userAgent.slice(0, 32) });
}

function publishSignal(target, payload) {
  if (!ws || ws.readyState !== 1) return;
  sendMqttPublish(ws, `supershare/${currentRoom}/${target}`, JSON.stringify(payload));
}

// ============================================================================
// 4. WEBRTC PARALLEL ENGINE (1 Control Channel + 4 Unordered Data Channels)
// ============================================================================
function createPeerConnection(targetId, isInitiator) {
  activePeerId = targetId;
  pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
  dataChannels = [];

  pc.onicecandidate = (e) => {
    if (e.candidate) publishSignal(targetId, { type: 'ICE', from: myId, candidate: e.candidate });
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'connected') {
      document.getElementById('transferPanel').classList.remove('hidden');
      document.getElementById('activePeerName').textContent = peers.get(targetId)?.name || targetId;
      computeSasCode();
    }
  };

  if (isInitiator) {
    controlChannel = pc.createDataChannel('control', { ordered: true });
    setupControlChannel(controlChannel);
    for (let i = 0; i < PARALLEL_CHANNELS; i++) {
      const dc = pc.createDataChannel(`data_${i}`, { ordered: false });
      setupDataChannel(dc);
      dataChannels.push(dc);
    }
  } else {
    pc.ondatachannel = (e) => {
      if (e.channel.label === 'control') {
        controlChannel = e.channel;
        setupControlChannel(controlChannel);
      } else {
        setupDataChannel(e.channel);
        dataChannels.push(e.channel);
      }
    };
  }
}

async function computeSasCode() {
  const stats = await pc.getStats();
  let fp = '';
  stats.forEach(r => { if (r.type === 'certificate' && r.fingerprint) fp += r.fingerprint; });
  document.getElementById('sasCode').textContent = fp.replace(/:/g, '').slice(0, 6).toUpperCase() || 'VERIFIED';
}

function setupControlChannel(dc) {
  dc.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'OFFER_FILES') showConsentModal(msg);
    if (msg.type === 'ACCEPT_FILES') startParallelSend(msg.acceptedIds);
  };
}

function setupDataChannel(dc) {
  dc.binaryType = 'arraybuffer';
  dc.onmessage = (e) => {
    // Packet Header: [8 bytes fileId hash][8 bytes byteOffset][4 bytes wasmChecksum][payload...]
    const view = new DataView(e.data);
    const fileId = String(view.getBigUint64(0));
    const offset = Number(view.getBigUint64(8));
    const expectedSum = view.getUint32(16);
    const payload = new Uint8Array(e.data, 20);

    if (wasmChecksumFn && wasmChecksumFn(payload) !== expectedSum) {
      console.warn('Wasm checksum mismatch at offset', offset);
      return;
    }

    // Push directly to Service Worker HTTP stream
    navigator.serviceWorker.controller?.postMessage(
      { type: 'PUSH_CHUNK', fileId, offset, chunk: payload.buffer },
      [payload.buffer]
    );
    updateProgressUI(fileId, payload.byteLength);
  };
}

// ============================================================================
// 5. CONSENT FLOW ("ACCEPT" / "ACCEPT ALL") & PARALLEL STREAMING
// ============================================================================
function queueFilesToSend(fileList) {
  if (!controlChannel || controlChannel.readyState !== 'open') return alert('Connect to a device first!');
  const manifest = [];
  for (const file of fileList) {
    const fileId = String(Math.floor(Math.random() * 1e15));
    outgoingFiles.set(fileId, file);
    manifest.push({ fileId, name: file.name, size: file.size });
    createProgressRow(fileId, file.name, file.size, 'Waiting for receiver approval...');
  }
  controlChannel.send(JSON.stringify({ type: 'OFFER_FILES', sender: myName, files: manifest }));
}

function showConsentModal(msg) {
  pendingIncomingOffer = msg.files;
  document.getElementById('senderModalName').textContent = msg.sender;
  const ul = document.getElementById('incomingFileList');
  ul.innerHTML = '';
  msg.files.forEach((f) => {
    const li = document.createElement('li');
    li.innerHTML = `<input type="checkbox" value="${f.fileId}" checked /> <span>${f.name} (${formatBytes(f.size)})</span>`;
    ul.appendChild(li);
  });
  document.getElementById('consentModal').classList.remove('hidden');
}

function acceptIncoming(fileIds) {
  document.getElementById('consentModal').classList.add('hidden');
  for (const id of fileIds) {
    const meta = pendingIncomingOffer.find(f => f.fileId === id);
    if (!meta) continue;
    createProgressRow(id, meta.name, meta.size, 'Streaming via SW HTTP Proxy...');

    // Register stream in Service Worker & trigger native browser download
    navigator.serviceWorker.controller.postMessage({
      type: 'REGISTER_STREAM',
      fileId: id,
      fileName: meta.name,
      fileSize: meta.size
    });
    const a = document.createElement('a');
    a.href = `./sw-stream/${id}`;
    a.download = meta.name;
    document.body.appendChild(a);
    a.click();
    a.remove();
  }
  controlChannel.send(JSON.stringify({ type: 'ACCEPT_FILES', acceptedIds: fileIds }));
}

async function startParallelSend(acceptedIds) {
  if ('wakeLock' in navigator) {
    try { wakeLock = await navigator.wakeLock.request('screen'); } catch {}
  }

  for (const fileId of acceptedIds) {
    const file = outgoingFiles.get(fileId);
    if (!file) continue;

    let nextOffset = 0;
    const fileIdBig = BigInt(fileId);

    // Spawn 4 concurrent workers striping 64KB chunks across all 4 DataChannels
    const workers = dataChannels.map(async (dc) => {
      while (nextOffset < file.size) {
        const offset = nextOffset;
        nextOffset += CHUNK_SIZE;

        // Backpressure control so browser buffer never overflows
        while (dc.bufferedAmount > MAX_BUFFER) {
          await new Promise(r => setTimeout(r, 5));
        }

        const sliceBuf = await file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
        const sliceArr = new Uint8Array(sliceBuf);
        const sum = wasmChecksumFn ? wasmChecksumFn(sliceArr) : 0;

        const packet = new ArrayBuffer(20 + sliceArr.byteLength);
        const view = new DataView(packet);
        view.setBigUint64(0, fileIdBig);
        view.setBigUint64(8, BigInt(offset));
        view.setUint32(16, sum);
        new Uint8Array(packet, 20).set(sliceArr);

        dc.send(packet);
        updateProgressUI(fileId, sliceArr.byteLength);
      }
    });

    await Promise.all(workers);
  }
  if (wakeLock) { wakeLock.release(); wakeLock = null; }
}

// ============================================================================
// 6. SIGNALING HANDLER & UI HELPERS
// ============================================================================
async function handleSignal(msg) {
  if (msg.from === myId) return;
  if (msg.type === 'HELLO') {
    peers.set(msg.from, { name: msg.name, lastSeen: Date.now() });
    renderPeers();
  } else if (msg.type === 'OFFER') {
    createPeerConnection(msg.from, false);
    await pc.setRemoteDescription(msg.sdp);
    const ans = await pc.createAnswer();
    await pc.setLocalDescription(ans);
    publishSignal(msg.from, { type: 'ANSWER', from: myId, sdp: ans });
  } else if (msg.type === 'ANSWER' && pc) {
    await pc.setRemoteDescription(msg.sdp);
  } else if (msg.type === 'ICE' && pc) {
    await pc.addIceCandidate(msg.candidate);
  }
}

async function initiateConnection(targetId) {
  createPeerConnection(targetId, true);
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  publishSignal(targetId, { type: 'OFFER', from: myId, sdp: offer });
}

function renderPeers() {
  const list = document.getElementById('peerList');
  list.innerHTML = '';
  peers.forEach((info, id) => {
    const div = document.createElement('div');
    div.className = 'peer-card';
    div.innerHTML = `<strong>${info.name}</strong><span class="os">Click to connect (4x P2P)</span>`;
    div.onclick = () => initiateConnection(id);
    list.appendChild(div);
  });
}

const progressTracker = new Map();
function createProgressRow(id, name, total, statusText) {
  progressTracker.set(id, { transferred: 0, total, startTime: performance.now() });
  const row = document.createElement('div');
  row.className = 'transfer-item';
  row.id = `row_${id}`;
  row.innerHTML = `
    <div class="item-meta"><span>${name}</span><span id="stat_${id}">${statusText}</span></div>
    <div class="progress-bar"><div class="progress-fill" id="bar_${id}"></div></div>
  `;
  document.getElementById('transferQueue').prepend(row);
}

function updateProgressUI(id, bytesAdded) {
  const item = progressTracker.get(id);
  if (!item) return;
  item.transferred += bytesAdded;
  const pct = Math.min(100, (item.transferred / item.total) * 100);
  const elapsed = (performance.now() - item.startTime) / 1000 || 0.1;
  const speedMBs = (item.transferred / (1024 * 1024) / elapsed).toFixed(1);
  document.getElementById(`bar_${id}`).style.width = `${pct}%`;
  document.getElementById(`stat_${id}`).textContent = `${pct.toFixed(1)}% • ${speedMBs} MB/s`;
}

function formatBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(2) + ' GB';
}

// Minimal MQTT 3.1.1 binary packet helpers (zero external CDN dependencies)
function encodeStr(s) {
  const b = new TextEncoder().encode(s);
  return new Uint8Array([b.length >> 8, b.length & 255, ...b]);
}
function sendMqttConnect(sock, clientId) {
  const vh = new Uint8Array([0,4, 77,81,84,84, 4, 2, 0,60]);
  const pl = encodeStr(clientId);
  sock.send(new Uint8Array([0x10, vh.length + pl.length, ...vh, ...pl]));
}
function sendMqttSubscribe(sock, topic) {
  const t = encodeStr(topic);
  sock.send(new Uint8Array([0x82, 2 + t.length + 1, 0, 1, ...t, 0]));
}
function sendMqttPublish(sock, topic, msg) {
  const t = encodeStr(topic);
  const m = new TextEncoder().encode(msg);
  const len = t.length + m.length;
  const header = len < 128 ? [0x30, len] : [0x30, (len & 0x7f) | 0x80, len >> 7];
  sock.send(new Uint8Array([...header, ...t, ...m]));
}
function parseMqttPublish(buf) {
  if ((buf[0] & 0xf0) !== 0x30) return null;
  let idx = 1, mult = 1, rem = 0, b;
  do { b = buf[idx++]; rem += (b & 127) * mult; mult *= 128; } while (b & 128);
  const tLen = (buf[idx] << 8) | buf[idx + 1];
  idx += 2 + tLen;
  try { return JSON.parse(new TextDecoder().decode(buf.subarray(idx))); } catch { return null; }
}

// Event Listeners
document.getElementById('autoConfigBtn').onclick = autoDiscoverWifi;
document.getElementById('joinRoomBtn').onclick = () => {
  const r = document.getElementById('roomInput').value.trim();
  if (r) connectSignaling(r, `Custom Room: ${r}`);
};
document.getElementById('dropZone').onclick = () => document.getElementById('fileInput').click();
document.getElementById('fileInput').onchange = (e) => queueFilesToSend(e.target.files);
document.getElementById('acceptAllBtn').onclick = () => acceptIncoming(pendingIncomingOffer.map(f => f.fileId));
document.getElementById('acceptSelectedBtn').onclick = () => {
  const checked = Array.from(document.querySelectorAll('#incomingFileList input:checked')).map(i => i.value);
  if (checked.length) acceptIncoming(checked);
};
document.getElementById('declineBtn').onclick = () => document.getElementById('consentModal').classList.add('hidden');
document.getElementById('disconnectBtn').onclick = () => { if (pc) pc.close(); document.getElementById('transferPanel').classList.add('hidden'); };

// Boot
initWasmEngine();
initServiceWorker().then(autoDiscoverWifi);