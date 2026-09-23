/* ============================================================
   Aurora P2P Chat — app.js
   ============================================================ */
const DEBUG = true;
const log = (...a) => DEBUG && console.log('%c[Aurora]','color:#22d3ee;font-weight:bold',...a);
const logErr = (...a) => console.error('%c[ERR]','color:#f43f5e;font-weight:bold',...a);
window.addEventListener('error', e => logErr('Global:', e.message));

const APP_ID = 'aurora-p2p-v9';
const CHUNK = 16 * 1024;
const MAX_FILES = 30;

let joinRoom = null;
async function loadTrystero(){
  const urls = [
    'https://esm.sh/trystero@0.21.5/mqtt',
    'https://cdn.jsdelivr.net/npm/trystero@0.21.5/mqtt/+esm',
    'https://esm.sh/trystero@0.21.5/nostr',
    'https://esm.sh/trystero@0.21.5'
  ];
  for(const url of urls){
    try{
      log('Try:', url);
      const mod = await import(/* @vite-ignore */ url);
      if(mod.joinRoom){ joinRoom = mod.joinRoom; log('✅ Loaded:', url); return url; }
    }catch(e){ logErr('Fail:', url, e.message); }
  }
  throw new Error('Все источники Trystero недоступны');
}

let room = null;
let sendText, sendFileMeta, sendFileChunk, sendFileEnd, sendTyping, sendName, sendStream, sendCallSignal, sendKick, sendDelete, sendRole;
let onText, onFileMeta, onFileChunk, onFileEnd, onTyping, onName, onStream, onCallSignal, onKick, onDelete, onRole;
let cryptoKey = null;
let mediaRecorder = null;
let audioChunks = [];
let lastTyping = 0;
const receivingFiles = new Map();
let activeScanner = null;
let pickedFiles = [];
let currentRoomId = null;
let currentMenuMsgId = null;
let myJoinedAt = null;

const peers = new Map();
const blocked = new Set();
const peerJoinedAt = new Map();
let ownerId = null;
const adminIds = new Set();

const callState = { active:false, incoming:false, outgoing:false, isVideo:false, localStream:null, remoteStream:null, remotePeerId:null, startTime:null, timerInterval:null, micEnabled:true, camEnabled:true };

const settings = { nickname:'Аноним', password:'', typing:true, enterToSend:true, theme:'dark' };

const $ = id => document.getElementById(id);
const messages = $('messages');
const toast = $('toast');
const msgMenu = $('msgMenu');
const peerMenu = $('peerMenu');

function showToast(t,d=2400){toast.textContent=t;toast.classList.add('show');clearTimeout(toast._t);toast._t=setTimeout(()=>toast.classList.remove('show'),d)}
function fmtSize(b){if(b<1024)return b+' B';if(b<1048576)return (b/1024).toFixed(1)+' KB';if(b<1073741824)return (b/1048576).toFixed(1)+' MB';return (b/1073741824).toFixed(2)+' GB'}
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function initials(n){if(!n)return '?';return n.trim().split(/\s+/).map(w=>w[0]).join('').slice(0,2).toUpperCase()||'?'}
function genId(){return Date.now().toString(36)+'-'+Math.random().toString(36).slice(2,8)}
function colorFor(p){const c=['#22d3ee','#a78bfa','#f472b6','#34d399','#fbbf24','#fb7185','#38bdf8','#4ade80'];let h=0;for(let i=0;i<p.length;i++)h=(h*31+p.charCodeAt(i))|0;return c[Math.abs(h)%c.length]}
function peerName(id){return peers.get(id)?.name||('Аноним-'+String(id).slice(0,4))}

function myRole(){if(!room)return 'member';if(ownerId===room.selfId)return 'owner';if(adminIds.has(room.selfId))return 'admin';return 'member'}
function peerRole(id){if(id===ownerId)return 'owner';if(adminIds.has(id))return 'admin';return 'member'}
function roleLabel(r){return r==='owner'?'👑 Владелец':r==='admin'?'🛡 Админ':'Участник'}
function roleEmoji(r){return r==='owner'?'👑':r==='admin'?'🛡':''}
function canKick(id){if(!room||id===room.selfId)return false;const me=myRole(),them=peerRole(id);if(me==='owner')return true;if(me==='admin'&&them==='member')return true;return false}
function canPromote(id){return myRole()==='owner'&&peerRole(id)==='member'&&id!==room.selfId}
function canDemote(id){return myRole()==='owner'&&peerRole(id)==='admin'}
function canTransfer(id){return myRole()==='owner'&&id!==room.selfId&&peers.has(id)}

function updateMyRoleUI(){const el=$('myRole');if(el)el.textContent=roleEmoji(myRole());renderPeers()}
function setStatus(t,on){$('statusText').textContent=t;$('statusDot').classList.toggle('online',on);$('peerSub').textContent=on?(peers.size>0?peers.size+' в сети':'Вы в комнате'):(room?'Ожидание':'Подключитесь')}
function setConn(t){$('connIndicator').style.display='block';$('connText').textContent=t}

function addMsg(text,type='them',opts={}){
  const w=document.createElement('div');w.className='msg '+type;
  if(opts.msgId)w.dataset.msgid=opts.msgId;
  if(opts.sender&&type==='them'){const s=document.createElement('div');s.className='msg-sender';s.innerHTML=esc(opts.sender)+(opts.badge?' '+opts.badge:'');if(opts.color)s.style.color=opts.color;w.appendChild(s)}
  const sp=document.createElement('span');sp.textContent=text;w.appendChild(sp);
  if(opts.enc){const b=document.createElement('span');b.className='encrypted-badge';b.textContent='🔒';w.appendChild(b)}
  messages.appendChild(w);messages.scrollTop=messages.scrollHeight;return w;
}
function updateProfile(){$('myName').innerHTML=esc(settings.nickname)+' <span id="myRole">'+roleEmoji(myRole())+'</span>';$('myAvatar').textContent=initials(settings.nickname)}
function save(){localStorage.setItem('aurora',JSON.stringify(settings))}
function load(){try{const r=localStorage.getItem('aurora');if(r)Object.assign(settings,JSON.parse(r))}catch(e){}}
function applyTheme(){document.body.classList.toggle('light',settings.theme==='light');$('themeBtn').textContent=settings.theme==='light'?'☀️':'🌙'}

/* ============================================================
   МЕНЮ (☰) — с кнопкой выхода
   ============================================================ */
(function initMenu(){
  const menuBtn = $('menuBtn');
  if(!menuBtn) return;

  const qm = document.createElement('div');
  qm.id = 'quickMenu';
  qm.style.cssText = 'position:fixed;top:64px;left:12px;background:var(--bg2);border:1px solid var(--border2);border-radius:16px;padding:8px;min-width:230px;z-index:2000;box-shadow:0 12px 40px rgba(0,0,0,.7);display:none;backdrop-filter:blur(24px)';
  qm.innerHTML = `
    <button id="qmLeave" style="display:flex;align-items:center;gap:10px;width:100%;padding:13px 16px;border:none;background:transparent;color:#f43f5e;font-size:14.5px;font-family:inherit;cursor:pointer;border-radius:10px;text-align:left;font-weight:700">🚪 Покинуть комнату</button>
    <div style="height:1px;background:var(--border);margin:6px 4px"></div>
    <button id="qmClear" style="display:flex;align-items:center;gap:10px;width:100%;padding:13px 16px;border:none;background:transparent;color:var(--text);font-size:14.5px;font-family:inherit;cursor:pointer;border-radius:10px;text-align:left;font-weight:500">🗑 Очистить чат</button>
    <button id="qmTheme" style="display:flex;align-items:center;gap:10px;width:100%;padding:13px 16px;border:none;background:transparent;color:var(--text);font-size:14.5px;font-family:inherit;cursor:pointer;border-radius:10px;text-align:left;font-weight:500">🌙 Сменить тему</button>
    <div style="height:1px;background:var(--border);margin:6px 4px"></div>
    <button id="qmClose" style="display:flex;align-items:center;gap:10px;width:100%;padding:13px 16px;border:none;background:transparent;color:var(--text2);font-size:14.5px;font-family:inherit;cursor:pointer;border-radius:10px;text-align:left;font-weight:500">✕ Закрыть меню</button>
  `;
  document.body.appendChild(qm);

  const closeMenu = () => { qm.style.display = 'none'; };
  const toggleMenu = (e) => {
    if(e) e.stopPropagation();
    const isOpen = qm.style.display === 'block';
    qm.style.display = isOpen ? 'none' : 'block';
    // Открываем сайдбар на мобильных тоже
    const sb = $('sidebar');
    if(sb && !isOpen) sb.classList.add('open');
  };

  menuBtn.onclick = toggleMenu;
  document.addEventListener('click', (e) => {
    if(qm.style.display === 'block' && !qm.contains(e.target) && e.target !== menuBtn) closeMenu();
  });

  $('qmLeave').onclick = () => {
    closeMenu();
    const sb = $('sidebar'); if(sb) sb.classList.remove('open');
    if(!confirm('Покинуть комнату? Вы отключитесь от собеседников, чат закроется.')) return;
    localStorage.removeItem('aurora_room');
    location.href = location.origin + location.pathname;
  };
  $('qmClear').onclick = () => {
    closeMenu();
    if(!confirm('Очистить всю переписку в этом чате?')) return;
    if(messages) messages.innerHTML = '';
    if(typeof showToast === 'function') showToast('🗑 Чат очищен');
  };
  $('qmTheme').onclick = () => {
    closeMenu();
    settings.theme = settings.theme === 'light' ? 'dark' : 'light';
    applyTheme(); save();
  };
  $('qmClose').onclick = closeMenu;
})();

/* ============================================================
   ОБРАБОТЧИКИ ОСНОВНОГО UI
   ============================================================ */
$('themeBtn').onclick = () => { settings.theme = settings.theme==='light'?'dark':'light'; applyTheme(); save(); };
if($('menuBtnOld')) $('menuBtnOld').onclick = () => $('sidebar').classList.toggle('open');

$('settingsBtn').onclick = () => {
  $('nicknameInput').value = settings.nickname;
  $('passwordInput').value = settings.password;
  $('typingSwitch').classList.toggle('on', settings.typing);
  $('enterSwitch').classList.toggle('on', settings.enterToSend);
  $('settingsBg').classList.add('active');
};
$('settingsCloseBtn').onclick = () => $('settingsBg').classList.remove('active');
$('settingsSaveBtn').onclick = async () => {
  settings.nickname = $('nicknameInput').value.trim() || 'Аноним';
  settings.password = $('passwordInput').value;
  settings.typing = $('typingSwitch').classList.contains('on');
  settings.enterToSend = $('enterSwitch').classList.contains('on');
  save(); updateProfile();
  if(currentRoomId) await initCrypto(currentRoomId);
  if(room) sendName(settings.nickname);
  showToast('✅ Сохранено');
  $('settingsBg').classList.remove('active');
};
$('typingSwitch').onclick = function(){ this.classList.toggle('on'); };
$('enterSwitch').onclick = function(){ this.classList.toggle('on'); };

/* ============================================================
   КРИПТО
   ============================================================ */
async function initCrypto(roomId){
  const secret = settings.password ? `p2p:${settings.password}:${roomId}` : (roomId ? `p2p:${roomId}` : null);
  if(!secret){ cryptoKey = null; return; }
  try{
    const enc = new TextEncoder();
    const km = await crypto.subtle.importKey('raw', enc.encode(secret), {name:'PBKDF2'}, false, ['deriveKey']);
    cryptoKey = await crypto.subtle.deriveKey(
      {name:'PBKDF2', salt: enc.encode('aurora-salt'), iterations:150000, hash:'SHA-256'},
      km, {name:'AES-GCM', length:256}, false, ['encrypt','decrypt']
    );
    log('🔐 Key ready');
  }catch(e){ logErr('Crypto:', e); cryptoKey = null; }
}
async function encText(t){
  if(!cryptoKey) return {plain:t};
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const c = await crypto.subtle.encrypt({name:'AES-GCM',iv}, cryptoKey, new TextEncoder().encode(t));
  return {iv: Array.from(iv), enc: Array.from(new Uint8Array(c))};
}
async function decText(p){
  if(p.plain !== undefined) return p.plain;
  if(!cryptoKey) throw new Error('NO_KEY');
  const r = await crypto.subtle.decrypt({name:'AES-GCM', iv: new Uint8Array(p.iv)}, cryptoKey, new Uint8Array(p.enc));
  return new TextDecoder().decode(r);
}
async function encBuf(b){
  if(!cryptoKey) return b;
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const c = await crypto.subtle.encrypt({name:'AES-GCM', iv}, cryptoKey, b);
  const o = new Uint8Array(12 + c.byteLength);
  o.set(iv,0); o.set(new Uint8Array(c),12);
  return o.buffer;
}
async function decBuf(b){
  if(!cryptoKey) return b;
  const v = new Uint8Array(b);
  return await crypto.subtle.decrypt({name:'AES-GCM', iv: v.slice(0,12)}, cryptoKey, v.slice(12));
}

/* ============================================================
   QR
   ============================================================ */
function showQR(id, text){
  const c = $(id);
  c.innerHTML = ''; c.classList.add('active');
  new QRCode(c, { text, width:180, height:180, colorDark:'#0a0e1a', colorLight:'#fff', correctLevel: QRCode.CorrectLevel.L });
}
function startScan(box, video, onResult){
  const b = $(box), v = $(video);
  b.classList.add('active');
  navigator.mediaDevices.getUserMedia({video:{facingMode:'environment', width:{ideal:640}}}).then(stream => {
    v.srcObject = stream; v.play();
    const cv = document.createElement('canvas');
    const ctx = cv.getContext('2d', {willReadFrequently:true});
    let run = true;
    activeScanner = {stop:()=>{ run=false; stream.getTracks().forEach(t=>t.stop()); b.classList.remove('active'); }};
    function scan(){
      if(!run) return;
      if(v.readyState === v.HAVE_ENOUGH_DATA){
        cv.width = v.videoWidth; cv.height = v.videoHeight;
        ctx.drawImage(v, 0, 0);
        try{
          const img = ctx.getImageData(0, 0, cv.width, cv.height);
          const code = jsQR(img.data, cv.width, cv.height);
          if(code && code.data){ activeScanner.stop(); onResult(code.data); return; }
        }catch(e){}
      }
      requestAnimationFrame(scan);
    }
    scan();
  }).catch(()=>{ showToast('Нет доступа к камере'); b.classList.remove('active'); });
}
$('scanInviteClose').onclick = () => activeScanner && activeScanner.stop();

/* ============================================================
   КОМНАТА
   ============================================================ */
function getRoomFromUrl(){ return new URLSearchParams(location.hash.slice(1)).get('room'); }
function updateInvite(id){
  const url = `${location.origin}${location.pathname}#room=${id}`;
  $('inviteLinkText').textContent = url;
  $('inviteCard').style.display = 'block';
  return url;
}
$('copyLinkBtn').onclick = async () => {
  const url = updateInvite(currentRoomId || $('roomInput').value);
  try{ await navigator.clipboard.writeText(url); showToast('📋 Скопировано'); }catch(e){ showToast('Не удалось'); }
};
$('qrLinkBtn').onclick = () => {
  const url = updateInvite(currentRoomId || $('roomInput').value);
  const b = $('qrLinkBox');
  if(b.classList.contains('active')) b.classList.remove('active');
  else showQR('qrLinkBox', url);
};
$('scanInviteBtn').onclick = () => {
  startScan('scanInviteBox','scanInviteVideo',(data)=>{
    try{
      const p = new URLSearchParams(new URL(data).hash.slice(1));
      const r = p.get('room');
      if(r){ $('roomInput').value = r; showToast('✅ Распознано'); joinRoomById(r); }
    }catch(e){ showToast('Неверный QR'); }
  });
};
$('generateRoomBtn').onclick = () => {
  const w = ['alpha','bravo','delta','echo','fox','galaxy','hero','ice','jungle','kilo','lion','moon','nova','ocean','panda','quest','river','star','tiger','venus','wolf','zulu'];
  $('roomInput').value = `${w[Math.floor(Math.random()*w.length)]}-${w[Math.floor(Math.random()*w.length)]}-${Math.floor(Math.random()*9000)+1000}`;
};
$('joinRoomBtn').onclick = () => {
  const raw = $('roomInput').value.trim().toLowerCase();
  const r = raw.replace(/[^a-z0-9\-_]/g, '');
  if(!r) return showToast('Введите название');
  if(r !== raw) $('roomInput').value = r;
  localStorage.setItem('aurora_room', r);
  joinRoomById(r);
};
$('leaveRoomBtn').onclick = () => {
  if(!confirm('Покинуть комнату?')) return;
  localStorage.removeItem('aurora_room');
  location.href = location.origin + location.pathname;
};
$('kickedOkBtn').onclick = () => {
  localStorage.removeItem('aurora_room');
  location.href = location.origin + location.pathname;
};

/* ============================================================
   РОЛЕВЫЕ ДЕЙСТВИЯ
   ============================================================ */
function promotePeer(id){
  if(!canPromote(id)) return;
  adminIds.add(id);
  sendRole({type:'promote', target:id});
  addMsg(`🛡 ${peerName(id)} теперь админ`, 'system');
  renderPeers();
}
function demotePeer(id){
  if(!canDemote(id)) return;
  adminIds.delete(id);
  sendRole({type:'demote', target:id});
  addMsg(`👤 ${peerName(id)} лишён прав`, 'system');
  renderPeers();
}
function transferOwner(id){
  if(!canTransfer(id)) return;
  if(!confirm(`Передать владение "${peerName(id)}"?`)) return;
  const old = ownerId;
  ownerId = id;
  adminIds.add(old);
  sendRole({type:'transfer', target:id, old});
  addMsg(`👑 Владение передано: ${peerName(id)}`, 'system');
  updateMyRoleUI();
}
function kickPeer(id){
  if(!canKick(id)) return showToast('Нет прав');
  const name = peerName(id);
  if(!confirm(`Исключить "${name}"?`)) return;
  const data = {target:id, by:settings.nickname};
  try{ sendKick(data, id); }catch(e){}
  try{ sendKick(data); }catch(e){}
  blocked.add(id);
  peers.delete(id);
  adminIds.delete(id);
  peerJoinedAt.delete(id);
  renderPeers();
  addMsg(`🚫 ${name} исключён`, 'system');
  if(callState.active && callState.remotePeerId === id) endCall(false);
}

function syncOwner(){
  if(!room) return;
  const list = [[room.selfId, myJoinedAt||Date.now()], ...[...peers.keys()].map(id => [id, peerJoinedAt.get(id)||Date.now()])];
  list.sort((a,b) => a[1]-b[1] || (a[0]<b[0]?-1:1));
  const computed = list[0][0];
  if(computed !== ownerId){
    const old = ownerId;
    ownerId = computed;
    updateMyRoleUI();
    if(ownerId === room.selfId && old !== room.selfId){
      addMsg('👑 Вы стали владельцем', 'system');
      broadcastRole();
    }
    updatePeerHeader();
  }
}
function broadcastRole(){
  try{ sendRole({type:'sync', ownerId, admins:[...adminIds]}); }catch(e){}
}

/* ============================================================
   ПОДКЛЮЧЕНИЕ
   ============================================================ */
async function joinRoomById(roomId){
  if(room){ location.reload(); return; }
  currentRoomId = roomId;
  myJoinedAt = Date.now();
  setStatus('Подключение...', false);
  setConn('⏳ Загрузка библиотеки...');
  $('joinRoomBtn').disabled = true;
  $('leaveRoomBtn').style.display = 'flex';

  try{
    setConn('⏳ Загрузка Trystero...');
    await loadTrystero();
    setConn('⏳ Создание ключа...');
    await initCrypto(roomId);
    setConn('⏳ Подключение к сети...');

    room = joinRoom({appId: APP_ID}, roomId);
    room._roomId = roomId;
    location.hash = `room=${roomId}`;

    [sendText, onText]             = room.makeAction('t');
    [sendFileMeta, onFileMeta]     = room.makeAction('fm');
    [sendFileChunk, onFileChunk]   = room.makeAction('fc');
    [sendFileEnd, onFileEnd]       = room.makeAction('fe');
    [sendTyping, onTyping]         = room.makeAction('typ');
    [sendName, onName]             = room.makeAction('n');
    [sendStream, onStream]         = room.makeAction('s');
    [sendCallSignal, onCallSignal] = room.makeAction('call');
    [sendKick, onKick]             = room.makeAction('kick');
    [sendDelete, onDelete]         = room.makeAction('del');
    [sendRole, onRole]             = room.makeAction('role');

    onText(async (p, pid) => {
      if(blocked.has(pid)) return;
      try{
        const t = await decText(p);
        addMsg(t, 'them', {msgId:p.msgId, sender:peerName(pid), color:colorFor(pid), badge:roleEmoji(peerRole(pid)), enc:!!cryptoKey});
      }catch(e){ addMsg('⚠️ Не расшифровано', 'them', {sender:peerName(pid)}); }
    });
    onFileMeta((m, pid) => { if(blocked.has(pid)) return; receivingFiles.set(m.id, {...m, chunks:[], received:0, senderId:pid}); });
    onFileChunk((c, pid, m) => {
      if(blocked.has(pid)) return;
      const f = receivingFiles.get(m?.id); if(!f) return;
      f.chunks.push(c); f.received += c.byteLength;
    });
    onFileEnd(async (m, pid) => {
      if(blocked.has(pid)) return;
      const f = receivingFiles.get(m.id); if(!f) return;
      try{
        let ch = f.chunks;
        if(f.encrypted && cryptoKey){
          const o = [];
          for(const c of ch){ try{ o.push(await decBuf(c)); }catch(e){ o.push(c); } }
          ch = o;
        }
        const blob = new Blob(ch, {type: f.type || 'application/octet-stream'});
        const url = URL.createObjectURL(blob);
        renderFile(f.name, f.size, url, 'them', f.encrypted, f.senderId, m.id);
      }catch(e){ showToast('Ошибка расшифровки'); }
      receivingFiles.delete(m.id);
    });
    onTyping((isT, pid) => {
      if(blocked.has(pid) || !settings.typing) return;
      const el = $('typingBar');
      if(isT){
        $('typingText').textContent = peerName(pid)+' печатает...';
        el.classList.add('active');
        clearTimeout(el._t);
        el._t = setTimeout(()=>el.classList.remove('active'), 2200);
      }
    });
    onName((n, pid) => {
      if(blocked.has(pid)) return;
      if(peers.has(pid)) peers.get(pid).name = n;
      else peers.set(pid, {name:n, color:colorFor(pid)});
      if(!peerJoinedAt.has(pid)) peerJoinedAt.set(pid, Date.now());
      renderPeers(); updatePeerHeader();
    });
    onStream((s, pid) => {
      if(blocked.has(pid)) return;
      if(callState.active && callState.remotePeerId && callState.remotePeerId !== pid) return;
      callState.remoteStream = s;
      $('remoteVideo').srcObject = s;
      $('callMain').classList.toggle('audio-only', s.getVideoTracks().length === 0);
    });
    onCallSignal((sig, pid) => {
      if(blocked.has(pid)) return;
      callState.remotePeerId = pid;
      handleCall(sig, pid);
    });
    onKick((data, from) => {
      if(data.target === room.selfId){
        $('kickedText').textContent = `${data.by||'Участник'} исключил вас.`;
        $('kickedOverlay').classList.add('active');
        if(callState.active) endCall(false);
        return;
      }
      if(blocked.has(data.target)) return;
      const name = peerName(data.target);
      blocked.add(data.target);
      peers.delete(data.target);
      adminIds.delete(data.target);
      peerJoinedAt.delete(data.target);
      renderPeers();
      addMsg(`🚫 ${name} исключён`, 'system');
      if(callState.active && callState.remotePeerId === data.target) endCall(false);
      syncOwner();
    });
    onDelete((data, pid) => {
      if(blocked.has(pid)) return;
      if(data.msgId) removeMsg(data.msgId);
    });
    onRole((data, from) => {
      if(blocked.has(from)) return;
      if(data.type === 'sync'){
        if(data.ownerId) ownerId = data.ownerId;
        if(Array.isArray(data.admins)){ adminIds.clear(); data.admins.forEach(id=>adminIds.add(id)); }
        updateMyRoleUI(); updatePeerHeader();
      } else if(data.type === 'promote' && from === ownerId){
        adminIds.add(data.target);
        if(data.target === room.selfId) addMsg('🛡 Вы назначены админом','system');
        renderPeers();
      } else if(data.type === 'demote' && from === ownerId){
        adminIds.delete(data.target);
        if(data.target === room.selfId) addMsg('👤 Вы лишены прав','system');
        renderPeers();
      } else if(data.type === 'transfer' && from === ownerId){
        if(data.old) adminIds.add(data.old);
        ownerId = data.target;
        adminIds.delete(ownerId);
        if(ownerId === room.selfId) addMsg('👑 Вы стали владельцем','system');
        updateMyRoleUI(); updatePeerHeader();
      }
    });

    room.onPeerJoin(pid => {
      if(blocked.has(pid)) return;
      if(!peers.has(pid)) peers.set(pid, {name:'Аноним-'+String(pid).slice(0,4), color:colorFor(pid)});
      if(!peerJoinedAt.has(pid)) peerJoinedAt.set(pid, Date.now());
      renderPeers(); updatePeerHeader();
      const ann = () => {
        try{ sendName(settings.nickname, pid); }catch(e){}
        try{ sendRole({type:'sync', ownerId, admins:[...adminIds]}, pid); }catch(e){}
      };
      ann(); setTimeout(ann, 500); setTimeout(ann, 1500);
      addMsg(`👤 ${peerName(pid)} присоединился`, 'system');
      $('emptyState').style.display = 'none';
      $('messages').style.display = 'flex';
      $('inputBar').style.display = 'flex';
      $('headerActions').style.display = 'flex';
      setStatus('В сети', true);
      setConn(`✅ Подключено · ${peers.size}`);
      updatePeerHeader();
      setTimeout(syncOwner, 800);
      setTimeout(syncOwner, 2000);
    });
    room.onPeerLeave(pid => {
      const name = peerName(pid);
      const wasOwner = pid === ownerId;
      peers.delete(pid);
      adminIds.delete(pid);
      peerJoinedAt.delete(pid);
      renderPeers();
      addMsg(`👋 ${name} вышел`, 'system');
      if(wasOwner){
        const rem = [...peers.keys()];
        const next = rem.find(id => adminIds.has(id)) || rem[0];
        if(next){
          ownerId = next;
          adminIds.delete(next);
          if(next === room.selfId){ addMsg('👑 Вы стали владельцем','system'); setTimeout(broadcastRole, 400); }
          else addMsg(`👑 ${peerName(next)} теперь владелец`, 'system');
        } else ownerId = null;
      }
      if(peers.size === 0){ setStatus('Ожидание', false); setConn('⏳ Ожидание'); }
      else { setStatus('В сети', true); setConn(`✅ Подключено · ${peers.size}`); }
      updateMyRoleUI(); updatePeerHeader();
      if(callState.active && callState.remotePeerId === pid) endCall(false);
    });

    updateInvite(roomId);
    setStatus('Ожидание', false);
    setConn('⏳ Ожидание участников');
    $('emptyState').style.display = 'none';
    $('messages').style.display = 'flex';
    $('inputBar').style.display = 'flex';
    $('headerActions').style.display = 'flex';
    updatePeerHeader();
    showToast('✅ Комната создана');
    $('sidebar').classList.remove('open');

    setTimeout(() => {
      if(!ownerId && peers.size === 0){
        ownerId = room.selfId;
        updateMyRoleUI();
        broadcastRole();
        addMsg('👑 Вы владелец комнаты', 'system');
      } else if(!ownerId) syncOwner();
    }, 2000);

  }catch(e){
    logErr('Join:', e);
    showToast('Ошибка: ' + e.message);
    setConn('❌ ' + e.message);
    $('joinRoomBtn').disabled = false;
  }
}

/* ============================================================
   HEADER / PEERS
   ============================================================ */
function updatePeerHeader(){
  if(!room) return;
  if(peers.size === 0){
    $('peerName').textContent = 'Комната: ' + currentRoomId;
    $('peerSub').textContent = 'Ожидание участников';
  } else if(peers.size === 1){
    const [pid] = [...peers.keys()];
    const r = peerRole(pid);
    $('peerName').innerHTML = esc(peerName(pid)) + ' ' + roleEmoji(r);
    $('peerSub').textContent = roleLabel(r) + ' · 🔒';
  } else {
    $('peerName').textContent = 'Комната: ' + currentRoomId;
    $('peerSub').textContent = peers.size + ' участников · 🔒';
  }
}
function renderPeers(){
  const list = $('peersList');
  list.innerHTML = '';
  $('peersCount').textContent = peers.size;
  $('peersEmpty').style.display = peers.size === 0 ? 'block' : 'none';
  [...peers.entries()].forEach(([id, info]) => {
    const role = peerRole(id);
    const item = document.createElement('div');
    item.className = 'peer-item' + (role === 'owner' ? ' is-owner' : role === 'admin' ? ' is-admin' : '');
    item.innerHTML = `<div class="peer-avatar" style="background:${info.color}">${esc(initials(info.name))}</div><div class="peer-name-sm">${esc(info.name)}${roleEmoji(role)?' <span>'+roleEmoji(role)+'</span>':''}</div><div class="peer-actions"><button class="call-audio" data-p="${esc(id)}" data-t="audio">📞</button><button class="call-video" data-p="${esc(id)}" data-t="video">📹</button><button class="manage" data-p="${esc(id)}">⋯</button></div>`;
    list.appendChild(item);
  });
  list.querySelectorAll('.peer-actions button').forEach(b => {
    b.onclick = e => {
      e.stopPropagation();
      const id = b.dataset.p;
      if(b.classList.contains('manage')) openPeerMenu(b, id);
      else startCall(id, b.dataset.t === 'video');
    };
  });
}

function openPeerMenu(anchor, id){
  const role = peerRole(id);
  const can = canKick(id) || canPromote(id) || canDemote(id) || canTransfer(id);
  if(!can){ showToast(roleLabel(role)); return; }
  let html = `<button disabled>${esc(peerName(id))} · ${roleLabel(role)}</button>`;
  if(canPromote(id)) html += `<button class="gold" data-a="promote">🛡 Назначить админом</button>`;
  if(canDemote(id)) html += `<button data-a="demote">👤 Снять админа</button>`;
  if(canTransfer(id)) html += `<button class="gold" data-a="transfer">👑 Передать владение</button>`;
  if(canKick(id)) html += `<button class="danger" data-a="kick">🚫 Исключить</button>`;
  peerMenu.innerHTML = html;
  peerMenu.classList.add('active');
  const r = anchor.getBoundingClientRect();
  const mr = peerMenu.getBoundingClientRect();
  peerMenu.style.left = Math.max(10, r.right - mr.width) + 'px';
  peerMenu.style.top = (r.bottom + 6 + mr.height > window.innerHeight ? r.top - mr.height - 6 : r.bottom + 6) + 'px';
  peerMenu.querySelectorAll('button[data-a]').forEach(b => {
    b.onclick = () => {
      const a = b.dataset.a;
      peerMenu.classList.remove('active');
      if(a === 'promote') promotePeer(id);
      else if(a === 'demote') demotePeer(id);
      else if(a === 'transfer') transferOwner(id);
      else if(a === 'kick') kickPeer(id);
    };
  });
}
document.addEventListener('click', e => {
  if(!peerMenu.contains(e.target)) peerMenu.classList.remove('active');
  if(!msgMenu.contains(e.target)) msgMenu.classList.remove('active');
});

/* ============================================================
   СООБЩЕНИЯ
   ============================================================ */
async function sendMsg(){
  const i = $('msgInput');
  const t = i.value.trim();
  if(!t || !room) return;
  const id = genId();
  const payload = {...(await encText(t)), msgId:id};
  sendText(payload);
  addMsg(t, 'me', {msgId:id, enc:!!cryptoKey});
  i.value = '';
  i.style.height = 'auto';
  sendTyping(false);
}
$('sendBtn').onclick = sendMsg;
$('msgInput').onkeydown = e => {
  if(e.key === 'Enter' && !e.shiftKey && settings.enterToSend){ e.preventDefault(); sendMsg(); }
};
$('msgInput').addEventListener('input', function(){
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 140) + 'px';
  if(!room || !settings.typing) return;
  const now = Date.now();
  if(now - lastTyping > 900){ sendTyping(true); lastTyping = now; }
});

async function sendFile(file){
  if(!room || peers.size === 0) return showToast('Нет участников');
  const id = genId();
  const enc = !!cryptoKey;
  sendFileMeta({id, name:file.name, size:file.size, type:file.type, encrypted:enc});
  const buf = await file.arrayBuffer();
  const total = buf.byteLength;
  let sent = 0;
  const el = document.createElement('div');
  el.className = 'msg me';
  el.dataset.msgid = id;
  el.innerHTML = `<div class="file-card"><div class="file-thumb">📄</div><div class="file-details"><div class="file-title">${esc(file.name)}</div><div class="file-meta">${fmtSize(file.size)} · отправка${enc?' 🔐':''}</div></div></div><div class="progress-track"><div class="progress-fill"></div></div>`;
  messages.appendChild(el);
  messages.scrollTop = messages.scrollHeight;
  const fill = el.querySelector('.progress-fill');
  const info = el.querySelector('.file-meta');
  const next = async () => {
    if(sent >= total){
      sendFileEnd({id}, null);
      const url = URL.createObjectURL(file);
      el.remove();
      renderFile(file.name, file.size, url, 'me', enc, null, id);
      return;
    }
    const slice = buf.slice(sent, sent + CHUNK);
    let chunk = slice;
    if(enc){ try{ chunk = await encBuf(slice); }catch(e){ chunk = slice; } }
    try{ sendFileChunk(chunk, null, {id}); }catch(e){}
    sent += slice.byteLength;
    const p = Math.round(sent/total*100);
    fill.style.width = p + '%';
    info.textContent = `${fmtSize(file.size)} · ${p}%${enc?' 🔐':''}`;
    setTimeout(next, 0);
  };
  next();
}
function renderFile(name, size, url, side, enc, senderId, msgId){
  const d = document.createElement('div');
  d.className = 'msg ' + side;
  if(msgId) d.dataset.msgid = msgId;
  if(side === 'them' && senderId){
    const s = document.createElement('div');
    s.className = 'msg-sender';
    s.innerHTML = esc(peerName(senderId)) + (roleEmoji(peerRole(senderId)) ? ' '+roleEmoji(peerRole(senderId)) : '');
    s.style.color = colorFor(senderId);
    d.appendChild(s);
  }
  const isImg = /\.(png|jpe?g|gif|webp|svg|bmp)$/i.test(name);
  const isVid = /\.(mp4|webm|ogg|mov|mkv)$/i.test(name);
  const isAud = /\.(mp3|wav|ogg|m4a|flac|aac)$/i.test(name);
  let prev = '';
  if(isImg) prev = `<img src="${url}">`;
  else if(isVid) prev = `<video src="${url}" controls></video>`;
  else if(isAud) prev = `<audio src="${url}" controls></audio>`;
  const ic = isImg ? '🖼️' : isVid ? '🎬' : isAud ? '🎵' : '📄';
  d.insertAdjacentHTML('beforeend', `<div class="file-card"><div class="file-thumb">${ic}</div><div class="file-details"><div class="file-title">${esc(name)}</div><div class="file-meta">${fmtSize(size)}${enc?' · 🔒':''}</div></div><a class="file-action" href="${url}" download="${esc(name)}">⬇</a></div>${prev}`);
  messages.appendChild(d);
  messages.scrollTop = messages.scrollHeight;
}

function openMsgMenu(x, y, id){
  currentMenuMsgId = id;
  msgMenu.classList.add('active');
  const r = msgMenu.getBoundingClientRect();
  msgMenu.style.left = Math.min(x, innerWidth - r.width - 10) + 'px';
  msgMenu.style.top = Math.min(y, innerHeight - r.height - 10) + 'px';
}
document.addEventListener('contextmenu', e => {
  const m = e.target.closest('.msg.me');
  if(!m || !m.dataset.msgid) return;
  e.preventDefault();
  openMsgMenu(e.clientX, e.clientY, m.dataset.msgid);
});
let longTap = null;
document.addEventListener('touchstart', e => {
  const m = e.target.closest('.msg.me');
  if(!m || !m.dataset.msgid) return;
  longTap = setTimeout(() => openMsgMenu(e.touches[0].clientX, e.touches[0].clientY, m.dataset.msgid), 550);
}, {passive:true});
document.addEventListener('touchend', () => clearTimeout(longTap));
$('msgMenuDelete').onclick = () => {
  if(!currentMenuMsgId) return;
  const id = currentMenuMsgId;
  msgMenu.classList.remove('active');
  try{ sendDelete({msgId:id}); }catch(e){}
  removeMsg(id);
  showToast('🗑 Удалено');
};
$('msgMenuCopy').onclick = () => {
  const el = document.querySelector(`.msg[data-msgid="${currentMenuMsgId}"]`);
  msgMenu.classList.remove('active');
  if(!el) return;
  const s = [...el.querySelectorAll('span')].find(x => !x.classList.contains('encrypted-badge'));
  if(s){ navigator.clipboard.writeText(s.textContent); showToast('📋 Скопировано'); }
};
function removeMsg(id){
  const el = document.querySelector(`.msg[data-msgid="${id}"]`);
  if(el) el.remove();
}

/* ============================================================
   PICKER
   ============================================================ */
const pickerBg = $('pickerBg');
const filesGrid = $('filesGrid');
const gridSection = $('gridSection');
$('fileBtn').onclick = () => openPicker('files');
$('galleryBtn').onclick = () => openPicker('gallery');
$('pickerClose').onclick = closePicker;
$('pickerCancel').onclick = closePicker;
pickerBg.onclick = e => { if(e.target === pickerBg) closePicker(); };
$('dropZone').onclick = () => {
  const i = document.createElement('input');
  i.type = 'file'; i.multiple = true;
  i.onchange = e => addPick([...e.target.files]);
  i.click();
};
function openPicker(mode){
  pickedFiles = [];
  renderPicks();
  pickerBg.classList.add('active');
  $('pickerSub').textContent = peers.size === 0 ? 'Нет участников' : peers.size === 1 ? 'Файл получит собеседник' : `Файл получат все (${peers.size})`;
  if(mode === 'gallery'){
    const i = document.createElement('input');
    i.type = 'file'; i.accept = 'image/*,video/*'; i.multiple = true;
    i.onchange = e => addPick([...e.target.files]);
    i.click();
  }
}
function closePicker(){ pickerBg.classList.remove('active'); pickedFiles = []; }
function addPick(files){
  for(const f of files){
    if(pickedFiles.length >= MAX_FILES){ showToast('Максимум ' + MAX_FILES); break; }
    pickedFiles.push(f);
  }
  renderPicks();
}
function renderPicks(){
  filesGrid.innerHTML = '';
  if(!pickedFiles.length){
    gridSection.style.display = 'none';
    $('pickerSend').disabled = true;
    $('pickerFooterCount').textContent = '';
    return;
  }
  gridSection.style.display = 'block';
  $('pickerSend').disabled = false;
  $('pickedCount').textContent = pickedFiles.length;
  $('pickerFooterCount').textContent = `Всего: ${pickedFiles.length} · ${fmtSize(pickedFiles.reduce((s,f)=>s+f.size,0))}`;
  pickedFiles.forEach((f, idx) => {
    const t = document.createElement('div');
    t.className = 'file-tile';
    if(f.type.startsWith('image/')){
      const i = document.createElement('img'); i.src = URL.createObjectURL(f); t.appendChild(i);
    } else if(f.type.startsWith('video/')){
      const v = document.createElement('video'); v.src = URL.createObjectURL(f); v.muted = true; t.appendChild(v);
    } else {
      const i = document.createElement('div'); i.className = 'tile-icon'; i.textContent = '📄'; t.appendChild(i);
    }
    const o = document.createElement('div');
    o.className = 'tile-overlay';
    o.innerHTML = `<div class="tile-name">${esc(f.name)}</div><div class="tile-size">${fmtSize(f.size)}</div>`;
    t.appendChild(o);
    const r = document.createElement('button');
    r.className = 'tile-remove'; r.textContent = '✕';
    r.onclick = e => { e.stopPropagation(); pickedFiles.splice(idx, 1); renderPicks(); };
    t.appendChild(r);
    filesGrid.appendChild(t);
  });
}
const dz = $('dropZone');
dz.addEventListener('dragover', e => { e.preventDefault(); dz.style.borderColor = 'var(--cyan)'; });
dz.addEventListener('dragleave', () => { dz.style.borderColor = ''; });
dz.addEventListener('drop', e => { e.preventDefault(); dz.style.borderColor = ''; addPick([...e.dataTransfer.files]); });
$('pickerSend').onclick = async () => {
  if(!pickedFiles.length) return;
  const files = [...pickedFiles];
  closePicker();
  for(const f of files) await sendFile(f);
  showToast(`📤 Отправлено: ${files.length}`);
};

/* ============================================================
   DRAG & DROP
   ============================================================ */
const dropOverlay = $('dropOverlay');
let dragC = 0;
document.addEventListener('dragenter', e => { e.preventDefault(); if(!room) return; dragC++; dropOverlay.classList.add('active'); });
document.addEventListener('dragleave', e => { e.preventDefault(); dragC--; if(dragC <= 0){ dragC = 0; dropOverlay.classList.remove('active'); } });
document.addEventListener('dragover', e => e.preventDefault());
document.addEventListener('drop', e => {
  e.preventDefault(); dragC = 0; dropOverlay.classList.remove('active');
  if(!room) return;
  const files = [...e.dataTransfer.files];
  if(files.length) files.forEach(sendFile);
});

/* ============================================================
   ГОЛОСОВЫЕ
   ============================================================ */
$('recordBtn').onclick = async () => {
  const b = $('recordBtn');
  if(mediaRecorder && mediaRecorder.state === 'recording'){
    mediaRecorder.stop(); b.classList.remove('recording'); b.textContent = '🎤';
    return;
  }
  try{
    const s = await navigator.mediaDevices.getUserMedia({audio:true});
    mediaRecorder = new MediaRecorder(s);
    audioChunks = [];
    mediaRecorder.ondataavailable = e => audioChunks.push(e.data);
    mediaRecorder.onstop = () => {
      const blob = new Blob(audioChunks, {type:'audio/webm'});
      const f = new File([blob], `voice-${Date.now()}.webm`, {type:'audio/webm'});
      sendFile(f);
      s.getTracks().forEach(t => t.stop());
    };
    mediaRecorder.start();
    b.classList.add('recording');
    b.textContent = '⏹';
    showToast('🎤 Запись...');
  }catch(e){ showToast('Нет доступа к микрофону'); }
};

/* ============================================================
   ЗВОНКИ
   ============================================================ */
$('callAllAudioBtn').onclick = () => { if(peers.size) startCall([...peers.keys()][0], false); };
$('callAllVideoBtn').onclick = () => { if(peers.size) startCall([...peers.keys()][0], true); };
async function startCall(pid, isV){
  if(!room || peers.size === 0) return showToast('Нет участников');
  if(callState.active) return showToast('Уже идёт звонок');
  Object.assign(callState, {isVideo:isV, outgoing:true, active:true, remotePeerId:pid, micEnabled:true, camEnabled:isV});
  $('outgoingMini').classList.add('active');
  $('outgoingName').textContent = peerName(pid);
  $('outgoingStatus').textContent = isV ? 'Видеозвонок...' : 'Аудиозвонок...';
  try{
    const c = isV
      ? {audio:true, video:{width:{ideal:1280}, height:{ideal:720}, facingMode:'user'}}
      : {audio:true, video:false};
    const s = await navigator.mediaDevices.getUserMedia(c);
    callState.localStream = s;
    $('localVideo').srcObject = s;
    try{ sendStream(s, pid); }catch(e){}
    sendCallSignal({type:'offer', isVideo:isV}, pid);
  }catch(e){ showToast('Нет доступа'); resetCall(); }
}
$('cancelOutgoing').onclick = () => {
  if(callState.remotePeerId) sendCallSignal({type:'cancel'}, callState.remotePeerId);
  resetCall();
};
async function handleCall(sig, pid){
  if(sig.type === 'offer'){
    if(callState.active){ sendCallSignal({type:'reject', reason:'busy'}, pid); return; }
    Object.assign(callState, {incoming:true, isVideo:sig.isVideo, remotePeerId:pid});
    const n = peerName(pid);
    $('incomingName').textContent = n;
    $('incomingAvatar').textContent = initials(n);
    $('incomingType').textContent = sig.isVideo ? '📹 Видеозвонок' : '📞 Аудиозвонок';
    $('incomingScreen').classList.add('active');
    playRing();
  } else if(sig.type === 'accept'){
    $('outgoingMini').classList.remove('active');
    callState.outgoing = false;
    openCallUI(); startTimer();
  } else if(sig.type === 'reject'){
    $('outgoingMini').classList.remove('active');
    showToast(sig.reason === 'busy' ? 'Занят' : 'Отклонён');
    resetCall();
  } else if(sig.type === 'cancel'){
    $('incomingScreen').classList.remove('active');
    stopRing(); showToast('Отменён');
    resetCall();
  } else if(sig.type === 'end'){
    showToast('Звонок завершён');
    endCall(false);
  }
}
$('acceptCallBtn').onclick = async () => {
  const pid = callState.remotePeerId;
  $('incomingScreen').classList.remove('active');
  stopRing();
  Object.assign(callState, {incoming:false, active:true, micEnabled:true, camEnabled:callState.isVideo});
  try{
    const c = callState.isVideo
      ? {audio:true, video:{width:{ideal:1280}, height:{ideal:720}, facingMode:'user'}}
      : {audio:true, video:false};
    const s = await navigator.mediaDevices.getUserMedia(c);
    callState.localStream = s;
    $('localVideo').srcObject = s;
    try{ sendStream(s, pid); }catch(e){}
    sendCallSignal({type:'accept'}, pid);
    openCallUI(); startTimer();
  }catch(e){ sendCallSignal({type:'reject', reason:'no-media'}, pid); resetCall(); }
};
$('rejectCallBtn').onclick = () => {
  const pid = callState.remotePeerId;
  $('incomingScreen').classList.remove('active');
  stopRing();
  if(pid) sendCallSignal({type:'reject'}, pid);
  resetCall();
};
function openCallUI(){
  const n = peerName(callState.remotePeerId) || 'Собеседник';
  $('callScreen').classList.add('active');
  $('callPeerName').textContent = n;
  $('callAvatarName').textContent = n;
  $('callAvatarLetter').textContent = initials(n);
  $('callMain').classList.toggle('audio-only', !callState.isVideo);
  $('callLocal').classList.toggle('audio-only', !callState.isVideo);
  $('ctrlCam').style.display = callState.isVideo ? 'flex' : 'none';
  $('ctrlMic').classList.add('off');
  $('ctrlCam').classList.add('off');
  $('callAvatarStatus').textContent = 'Соединение...';
}
setInterval(() => {
  if(callState.active && callState.remoteStream && $('callScreen').classList.contains('active')){
    $('callAvatarStatus').textContent = 'На связи';
  }
}, 1000);
$('ctrlMic').onclick = () => {
  if(!callState.localStream) return;
  callState.micEnabled = !callState.micEnabled;
  callState.localStream.getAudioTracks().forEach(t => t.enabled = callState.micEnabled);
  $('ctrlMic').classList.toggle('off', callState.micEnabled);
  $('ctrlMic').textContent = callState.micEnabled ? '🎤' : '🔇';
};
$('ctrlCam').onclick = () => {
  if(!callState.localStream) return;
  callState.camEnabled = !callState.camEnabled;
  callState.localStream.getVideoTracks().forEach(t => t.enabled = callState.camEnabled);
  $('ctrlCam').classList.toggle('off', callState.camEnabled);
  $('ctrlCam').textContent = callState.camEnabled ? '📹' : '🚫';
};
$('ctrlEnd').onclick = () => {
  if(callState.remotePeerId) sendCallSignal({type:'end'}, callState.remotePeerId);
  endCall(true);
};
function endCall(n = true){
  if(n && room && callState.remotePeerId) sendCallSignal({type:'end'}, callState.remotePeerId);
  const name = peerName(callState.remotePeerId) || 'Собеседник';
  resetCall();
  addMsg(`Звонок с ${name} завершён`, 'system');
}
function resetCall(){
  stopRing();
  if(callState.localStream) callState.localStream.getTracks().forEach(t => t.stop());
  clearInterval(callState.timerInterval);
  Object.assign(callState, {active:false, incoming:false, outgoing:false, isVideo:false, localStream:null, remoteStream:null, remotePeerId:null, startTime:null, timerInterval:null, micEnabled:true, camEnabled:true});
  $('callScreen').classList.remove('active');
  $('incomingScreen').classList.remove('active');
  $('outgoingMini').classList.remove('active');
  $('remoteVideo').srcObject = null;
  $('localVideo').srcObject = null;
  $('callTimer').textContent = '00:00';
}
function startTimer(){
  callState.startTime = Date.now();
  clearInterval(callState.timerInterval);
  callState.timerInterval = setInterval(() => {
    const s = Math.floor((Date.now() - callState.startTime) / 1000);
    $('callTimer').textContent = `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`;
  }, 1000);
}
let audioCtx = null, ringInt = null;
function playRing(){
  try{
    audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
    const beep = () => {
      const o = audioCtx.createOscillator(), g = audioCtx.createGain();
      o.connect(g); g.connect(audioCtx.destination);
      o.frequency.value = 800; o.type = 'sine';
      g.gain.setValueAtTime(0.15, audioCtx.currentTime);
      g.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.4);
      o.start(); o.stop(audioCtx.currentTime + 0.4);
    };
    beep();
    ringInt = setInterval(beep, 1200);
  }catch(e){}
}
function stopRing(){ if(ringInt){ clearInterval(ringInt); ringInt = null; } }

/* ============================================================
   СТАРТ
   ============================================================ */
(async function init(){
  load(); applyTheme(); updateProfile(); renderPeers();
  log('Init. Aurora v9');

  const urlRoom = getRoomFromUrl();
  if(urlRoom){
    $('roomInput').value = urlRoom;
    setTimeout(() => joinRoomById(urlRoom), 300);
  } else {
    const last = localStorage.getItem('aurora_room');
    if(last){
      $('roomInput').value = last;
      setTimeout(() => joinRoomById(last), 300);
    }
  }
})();
