
// ── Setup Wizard ──
async function checkSetupRequired() {
    try {
        const resp = await fetch('/api/settings');
        if (!resp.ok) return;
        const data = await resp.json();
        window.currentConfig = data;

        // Auto-fill existing data into wizard inputs
        try {
            if (data.pikpak) {
                if(data.pikpak.username) document.getElementById('wPikUser').value = data.pikpak.username;
                if(data.pikpak.password) document.getElementById('wPikPass').value = data.pikpak.password;
            }

            if (data.teldrive) {
                if(data.teldrive.api_host) document.getElementById('wTdUrl').value = data.teldrive.api_host;
                if(data.teldrive.access_token) document.getElementById('wTdToken').value = data.teldrive.access_token;
            }
            if (data.telegram) {
                if(data.telegram.api_id) document.getElementById('wTgId').value = data.telegram.api_id;
                if(data.telegram.api_hash) document.getElementById('wTgHash').value = data.telegram.api_hash;
            }
            if (data.telegram_db) {
                if(data.telegram_db.host) document.getElementById('wDbHost').value = data.telegram_db.host;
                if(data.telegram_db.port) document.getElementById('wDbPort').value = data.telegram_db.port;
                if(data.telegram_db.name) document.getElementById('wDbName').value = data.telegram_db.name;
                if(data.telegram_db.user) document.getElementById('wDbUser').value = data.telegram_db.user;
            }
        } catch(fillErr) { /* some fields may not exist yet */ }

        // Fetch health
        let healthData = { healthy: false, details: {} };
        try {
            const hResp = await fetch('/api/settings/health');
            if (hResp.ok) healthData = await hResp.json();
        } catch(e) { /* health unavailable */ }

        window.healthDetails = healthData.details || {};
        const needsSetup = (data._meta && data._meta.needs_setup);

        if (needsSetup) {
            const wiz = document.getElementById('setupWizard');
            if (wiz) {
                wiz.classList.add('show');
                wiz.classList.add('active');
            }

            // Determine first failed step
            const d = healthData.details || {};
            let firstStep = 1;
            if (d.pikpak) firstStep = 2;
            if (firstStep === 2 && d.teldrive) firstStep = 3;
            if (firstStep === 3 && d.telegram) firstStep = 4;

            // Jump to first failed step
            if (firstStep > 1) {
                for (let i = 1; i <= 4; i++) {
                    const s = document.getElementById('wStep' + i);
                    if (s) s.classList.remove('active');
                }
                const target = document.getElementById('wStep' + firstStep);
                if (target) target.classList.add('active');
                document.querySelectorAll('.wizard-dot').forEach(dot => dot.classList.remove('active'));
                const dot = document.getElementById('dot' + firstStep);
                if (dot) dot.classList.add('active');
            }
        }
    } catch (e) {
        console.error('Failed to check setup', e);
    }
}

async function wizardNext(current, next) {
    if (next < current) { // "Previous" button
        document.getElementById('wStep' + current).classList.remove('active');
        document.getElementById('wStep' + next).classList.add('active');
        document.querySelectorAll('.wizard-dot').forEach(d => d.classList.remove('active'));
        const dot = document.getElementById('dot' + next);
        if(dot) dot.classList.add('active');
        return;
    }

    const btn = event.currentTarget;
    const oldHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;margin-right:8px;"></span> 验证中...';
    
    if(window._wizardErrTimeout) clearTimeout(window._wizardErrTimeout);
    let errMsg = "";
    let dataToSave = {};

    try {
        if (current === 1) { // PikPak
            const user = document.getElementById('wPikUser').value.trim();
            const pass = document.getElementById('wPikPass').value.trim();
            if(!user || !pass) throw new Error("您必须填写 PikPak 账密");
            const payload = {username: user, password: pass};
            const r = await fetch('/api/settings/test/pikpak', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            const d = await r.json();
            if(!d.success) throw new Error(d.message || "PikPak验证失败");
            dataToSave.pikpak = payload;
        }
        else if (current === 2) { // TelDrive
            const tUrl = document.getElementById('wTdUrl').value.trim();
            const tTok = document.getElementById('wTdToken').value.trim();
            if(!tUrl || !tTok) throw new Error("TelDrive API和Token为必填");
            const tdPayload = {api_host: tUrl, access_token: tTok};
            const r = await fetch('/api/settings/test/teldrive', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(tdPayload)
            });
            const d = await r.json();
            if(!d.success && !d.ok) throw new Error("TelDrive连接失败");

            dataToSave.teldrive = tdPayload;
        }
        else if (current === 3) { // Telegram
            const tid = document.getElementById('wTgId').value.trim();
            const tHash = document.getElementById('wTgHash').value.trim();
            if(!tid || !tHash) throw new Error("必须提供 Telegram 授权参数");
            const tgPayload = {api_id: parseInt(tid), api_hash: tHash};
            const r = await fetch('/api/settings/test/telegram', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(tgPayload)
            });
            const d = await r.json();
            if(!d.success) throw new Error(d.message || "Telegram验证失败");
            dataToSave.telegram = tgPayload;
        }
    } catch (e) {
        errMsg = e.message;
    }

    if (errMsg) {
        btn.disabled = false;
        btn.innerHTML = oldHtml;
        const info = document.createElement('div');
        info.className = 'wizard-err-toast';
        info.innerHTML = `<i class="ph-fill ph-warning-circle"></i> ${errMsg}`;
        info.style = 'position:absolute; bottom:-45px; right:0; background:var(--error); color:white; padding:8px 16px; border-radius:8px; font-size:13px; display:flex; align-items:center; gap:8px; box-shadow:0 4px 12px rgba(239,68,68,0.3); z-index:100; opacity:0; transform:translateY(-10px); transition:all 0.3s;';
        
        const footer = btn.parentElement;
        footer.style.position = 'relative';
        const oldToast = footer.querySelector('.wizard-err-toast');
        if(oldToast) oldToast.remove();
        footer.appendChild(info);
        setTimeout(() => { info.style.opacity='1'; info.style.transform='translateY(0)'; }, 10);
        window._wizardErrTimeout = setTimeout(() => {
            info.style.opacity='0';
            setTimeout(() => info.remove(), 300);
        }, 3500);
        return;
    }

    // Success! Save config incrementally if there is data to save
    if (Object.keys(dataToSave).length > 0) {
        if (!window.currentConfig) window.currentConfig = {};
        for(let k in dataToSave) {
            if(!window.currentConfig[k]) window.currentConfig[k] = {};
            // merge
            Object.assign(window.currentConfig[k], dataToSave[k]);
        }
        await fetch('/api/settings', {
            method: 'PUT', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(window.currentConfig)
        });
    }

    btn.disabled = false;
    btn.innerHTML = oldHtml;

    document.getElementById('wStep' + current).classList.remove('active');
    document.getElementById('wStep' + next).classList.add('active');
    document.querySelectorAll('.wizard-dot').forEach(d => d.classList.remove('active'));
    const dot = document.getElementById('dot' + next);
    if(dot) dot.classList.add('active');
}

async function wizardFinish() {
    const btn = event.currentTarget;
    const oldHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;margin-right:8px;"></span> 验证中...';

    // validate DB
    const dbHost = document.getElementById('wDbHost').value.trim();
    if (!dbHost) {
        alert("请输入数据库地址");
        btn.disabled = false;
        btn.innerHTML = oldHtml;
        return;
    }
    
    let dbPayload = {};
    try {
        dbPayload = {
            host: dbHost,
            port: parseInt(document.getElementById('wDbPort').value) || 5432,
            name: document.getElementById('wDbName').value.trim(),
            user: document.getElementById('wDbUser').value.trim(),
            password: document.getElementById('wDbPass').value.trim()
        };
        const r = await fetch('/api/settings/test/database', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(dbPayload)
        });
        const d = await r.json();
        if(!d.success) throw new Error(d.message || "连接失败");
    } catch(e) {
        alert('数据库连接失败: ' + e.message);
        btn.disabled = false;
        btn.innerHTML = oldHtml;
        return;
    }

    btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;margin-right:8px;"></span> 部署中...';
    
    if (!window.currentConfig) window.currentConfig = {};
    if (!window.currentConfig.telegram_db) window.currentConfig.telegram_db = {};
    Object.assign(window.currentConfig.telegram_db, dbPayload);
    
    try {
        await fetch('/api/settings', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(window.currentConfig)
        });
        const wiz = document.getElementById('setupWizard');
        if(wiz) { wiz.classList.remove('active'); wiz.classList.remove('show'); }
        location.reload();
    } catch (e) {
        alert("保存最终配置失败", e);
        btn.disabled = false;
        btn.innerHTML = oldHtml;
    }
}


// ── 全局 401 拦截 ──
const _origFetch = window.fetch;
window.fetch = async function (...args) {
    const resp = await _origFetch.apply(this, args);
    if (resp.status === 401) { window.location.href = '/login'; }
    return resp;
};

// ── Navigation ──
let guidedPageSwitchTimer = null;
let guidedPageSwitchTarget = '';
let pageSwitchStageTimer = null;
let pageSwitchCleanupTimer = null;
let lastPageSwitchAt = 0;
let lastPageSwitchName = '';

function runPageSideEffects(name) {
    if (name === 'tasks') refreshPikPakTasks();
    if (name === 'aria2teldrive') loadA2TDTasks();
    if (name === 'tel2teldrive') loadT2TDState();
    if (name === 'settings') loadConfig();
}

function switchPage(name, options = {}) {
    const targetPage = document.getElementById('page-' + name);
    if (!targetPage) return;

    if (guidedPageSwitchTimer) {
        clearTimeout(guidedPageSwitchTimer);
        guidedPageSwitchTimer = null;
        guidedPageSwitchTarget = '';
    }
    if (pageSwitchStageTimer) {
        clearTimeout(pageSwitchStageTimer);
        pageSwitchStageTimer = null;
    }
    if (pageSwitchCleanupTimer) {
        clearTimeout(pageSwitchCleanupTimer);
        pageSwitchCleanupTimer = null;
    }

    if (targetPage.classList.contains('active')) {
        runPageSideEffects(name);
        return;
    }

    const pageContent = document.querySelector('.page-content');
    const animated = !!options.animated;


    const activateTargetPage = () => {
        document.querySelectorAll('.page').forEach(p => {
            p.classList.remove('active', 'page-enter', 'page-enter-active', 'page-exit');
        });
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        targetPage.classList.add('active');
        const navItem = document.querySelector(`.nav-item[data-page="${name}"]`);
        if (navItem) navItem.classList.add('active');
        lastPageSwitchAt = Date.now();
        lastPageSwitchName = name;
        runPageSideEffects(name);
    };

    if (pageContent) pageContent.classList.remove('page-switching');

    if (!animated || !pageContent) {
        activateTargetPage();
        return;
    }

    const currentPage = document.querySelector('.page.active');
    pageContent.classList.add('page-switching');
    if (currentPage) currentPage.classList.add('page-exit');

    pageSwitchStageTimer = setTimeout(() => {
        pageSwitchStageTimer = null;
        activateTargetPage();
        targetPage.classList.add('page-enter');
        requestAnimationFrame(() => targetPage.classList.add('page-enter-active'));
        pageSwitchCleanupTimer = setTimeout(() => {
            pageSwitchCleanupTimer = null;
            pageContent.classList.remove('page-switching');
            targetPage.classList.remove('page-enter', 'page-enter-active');
        }, 480);
    }, 180);
}



function scheduleGuidedPageSwitch(name, message = '', delay = 620) {
    const targetPage = document.getElementById('page-' + name);
    if (!targetPage || targetPage.classList.contains('active')) return;
    if (guidedPageSwitchTimer && guidedPageSwitchTarget === name) return;

    if (guidedPageSwitchTimer) clearTimeout(guidedPageSwitchTimer);
    guidedPageSwitchTarget = name;
    if (message) showA2TDToast(message, 'info');

    guidedPageSwitchTimer = setTimeout(() => {
        guidedPageSwitchTimer = null;
        guidedPageSwitchTarget = '';
        switchPage(name);
    }, delay);
}



async function logout() {
    await fetch('/api/logout', { method: 'POST' });
    window.location.href = '/login';
}

// ── System Test ──
async function checkServicesStatus() {
    try {
        const resp = await fetch('/api/settings/health');
        const data = await resp.json();
        
        const icon = document.getElementById('serviceStatusIcon');
        const dot = document.getElementById('serviceStatusDot');
        const wrapper = document.getElementById('serviceStatusWrapper');
        
        if (data.healthy) {
            icon.style.color = 'var(--success)';
            dot.classList.add('connected');
            wrapper.setAttribute('data-tooltip', '服务运行正常');
        } else {
            icon.style.color = 'var(--error)';
            dot.classList.remove('connected');
            wrapper.setAttribute('data-tooltip', data.message || '服务异常');
        }
    } catch (e) {
        document.getElementById('serviceStatusIcon').style.color = 'var(--error)';
        document.getElementById('serviceStatusWrapper').setAttribute('data-tooltip', '服务断开连接');
    }
}

async function testSingle(type) {
    const ep = `/api/settings/test/${type}`;
    const el = document.getElementById(`${type}Status`);
    if (!el) return;
    
    el.textContent = '测试中...'; el.className = '';
    try {
        const resp = await fetch(ep, { method: 'POST' });
        const data = await resp.json();
        const ok = data.success || data.ok;
        const msg = data.message || (ok ? '正常' : '失败');
        el.textContent = ok ? `✓ ${msg}` : `✗ ${msg}`;
        el.className = ok ? 'ok' : 'fail';
    } catch(e) {
        el.textContent = '✗ 网络错误';
        el.className = 'fail';
    }
}

async function testConnection() {
    const btn = document.getElementById('testBtn');
    if (!btn) return;
    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> 测试中...';
    
    const tests = [
        { ep: '/api/settings/test/pikpak', el: document.getElementById('pikpakStatus') },
        { ep: '/api/settings/test/aria2', el: document.getElementById('aria2Status') },
        { ep: '/api/settings/test/teldrive', el: document.getElementById('teldriveStatus') },
        { ep: '/api/settings/test/telegram', el: document.getElementById('telegramStatus') },
        { ep: '/api/settings/test/database', el: document.getElementById('databaseStatus') }
    ];
    
    for (const t of tests) {
        if (!t.el) continue;
        t.el.textContent = '测试中...'; t.el.className = '';
        try {
            const resp = await fetch(t.ep, { method: 'POST' });
            const data = await resp.json();
            const ok = data.success || data.ok;
            const msg = data.message || (ok ? '正常' : '失败');
            t.el.textContent = ok ? `✓ ${msg}` : `✗ ${msg}`;
            t.el.className = ok ? 'ok' : 'fail';
        } catch(e) {
            t.el.textContent = '✗ 网络错误';
            t.el.className = 'fail';
        }
    }
    btn.disabled = false; btn.innerHTML = '<i class="ph ph-activity"></i> 重新自检';
}


// ── WebSocket (Unified) ──
let ws = null;
function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${proto}//${location.host}/ws`);
    ws.onopen = () => {
        console.log('[WS] Connected');
        const dot = document.getElementById('wsDot');
        if (dot) dot.classList.add('connected');
        refreshA2TDMonitorIfNeeded(true);
    };

    ws.onclose = () => {
        console.log('[WS] Disconnected, reconnecting in 3s...');
        const dot = document.getElementById('wsDot');
        if (dot) dot.classList.remove('connected');
        setTimeout(connectWS, 3000);
    };
    ws.onerror = (e) => { console.error('[WS] Error:', e); };
    ws.onmessage = (e) => {
        try {
            const msg = JSON.parse(e.data);
            console.log('[WS] Received:', msg.type, msg);
            handleWSMessage(msg);
        } catch(err) {
            console.error('[WS] Parse error:', err, e.data);
        }
    };
}

function handleWSMessage(msg) {
    if (msg.type === "init") {
        if (msg.data.tasks) setA2TDTasks(msg.data.tasks);
        if (msg.data.global_stat) renderA2TDStats(msg.data.global_stat);
        return;
    }
    if (msg.type === "global_stat") {
        renderA2TDStats(msg.data);
        return;
    }
    if (msg.type === "tasks_update") {
        setA2TDTasks(msg.data);
        return;
    }
    if (msg.type === "task_update") {
        upsertA2TDTask(msg.data);
        return;
    }
    if (msg.type === "task_deleted") {
        removeA2TDTask(msg.data && msg.data.task_id);
        return;
    }

    // ── 内置引擎进度条 ──
    if (msg.type === "download_progress") {
        updateProgressBar(
            msg.task_id,
            msg.filename,
            'download',
            msg.progress,
            msg.speed,
            msg.downloaded,
            msg.total,
            msg.eta,
            msg.connections,
            msg.status,
            msg.max_connections,
            msg.downloaded_bytes,
            msg.total_bytes
        );
        return;
    }
    if (msg.type === "upload_progress") {
        updateProgressBar(
            msg.task_id,
            msg.filename,
            'upload',
            msg.progress,
            msg.speed,
            msg.uploaded,
            msg.total,
            '',
            0,
            'uploading',
            0,
            msg.uploaded_bytes,
            msg.total_bytes
        );
        return;
    }

    if (msg.type === "upload_done") {
        updateProgressBar(msg.task_id, msg.filename, 'done', 100, '', '', '', '', 0, 'completed');
        return;
    }

    const logEntry = buildProgressLogEntry(msg);
    if (logEntry) addLogEntry(logEntry.icon, logEntry.text);

    if (!document.getElementById('page-progress').classList.contains('active') && msg.type === 'task_start') {
        switchPage('progress', { animated: true });
    }
}





const a2tdTaskStore = new Map();
const a2tdRemovedTaskIds = new Set();
const a2tdPendingTaskActions = new Map();
let a2tdRenderScheduled = false;
let a2tdSnapshotPending = false;
let a2tdTaskFilter = 'all';

const A2TD_TASK_FILTER_LABELS = {
    all: '全部',
    downloading: '下载中',
    uploading: '上传中',
    completed: '已完成',
    failed: '失败',
    paused: '已暂停',
    pending: '等待中'
};

function normalizeA2TDTaskId(taskId, fallback = '') {

    return String(taskId || fallback || 'unknown');
}

function parseA2TDTimestamp(value) {
    if (!value) return 0;
    if (typeof value === 'number') return value;
    const ts = new Date(value).getTime();
    return Number.isFinite(ts) ? ts : 0;
}

function rememberRemovedA2TDTask(taskId, fallback = '') {
    const normalizedTaskId = normalizeA2TDTaskId(taskId, fallback);
    if (normalizedTaskId) a2tdRemovedTaskIds.add(normalizedTaskId);
}

function isA2TDTaskRemoved(taskId, fallback = '') {
    return a2tdRemovedTaskIds.has(normalizeA2TDTaskId(taskId, fallback));
}

function getA2TDNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
}

function hasA2TDSpeed(speedText) {
    const value = String(speedText || '').trim();
    return !!value && value !== '0 B/s';
}

function queueA2TDTaskRender() {
    if (a2tdRenderScheduled) return;
    a2tdRenderScheduled = true;
    const runner = () => {
        a2tdRenderScheduled = false;
        renderA2TDTasks(getA2TDTaskList());
    };
    if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
        window.requestAnimationFrame(runner);
    } else {
        setTimeout(runner, 16);
    }
}

function showA2TDToast(message, type = 'info') {
    const container = document.getElementById('a2tdToastContainer');
    if (!container || !message) return;
    const toast = document.createElement('div');
    toast.className = `app-toast ${type}`;
    const icons = {
        success: 'ph-check-circle',
        error: 'ph-warning-circle',
        warning: 'ph-warning',
        info: 'ph-info'
    };
    toast.innerHTML = `<i class="ph ${icons[type] || icons.info}"></i><span>${escapeA2TDHtml(message)}</span>`;
    container.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('show'));
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 220);
    }, 2400);
}

function getA2TDTaskList() {
    return Array.from(a2tdTaskStore.values()).sort((a, b) => {
        const timeA = getA2TDNumber(a.last_event_at || parseA2TDTimestamp(a.updated_at) || parseA2TDTimestamp(a.created_at));
        const timeB = getA2TDNumber(b.last_event_at || parseA2TDTimestamp(b.updated_at) || parseA2TDTimestamp(b.created_at));
        return timeB - timeA;
    });
}

function getA2TDTaskFilterCounts(tasks = []) {
    const counts = {
        all: Array.isArray(tasks) ? tasks.length : 0,
        downloading: 0,
        uploading: 0,
        completed: 0,
        failed: 0,
        paused: 0,
        pending: 0
    };

    if (!Array.isArray(tasks)) return counts;
    tasks.forEach(task => {
        const status = String(task?.status || '');
        if (Object.prototype.hasOwnProperty.call(counts, status)) counts[status] += 1;
    });
    return counts;
}

function syncA2TDTaskFilterButtons(tasks = getA2TDTaskList()) {
    const counts = getA2TDTaskFilterCounts(tasks);
    document.querySelectorAll('[data-a2td-filter]').forEach(btn => {
        const filter = btn.dataset.a2tdFilter || 'all';
        const label = A2TD_TASK_FILTER_LABELS[filter] || '全部';
        const count = counts[filter] || 0;
        btn.classList.toggle('active', filter === a2tdTaskFilter);
        setA2TDHtmlIfChanged(btn, `${label}<span class="a2td-filter-count">${count}</span>`);
    });
}

function matchesA2TDTaskFilter(task) {
    if (a2tdTaskFilter === 'all') return true;
    return String(task?.status || '') === a2tdTaskFilter;
}

function setA2TDTaskFilter(filter) {
    a2tdTaskFilter = A2TD_TASK_FILTER_LABELS[filter] ? filter : 'all';
    syncA2TDTaskFilterButtons();
    renderA2TDTaskStore();
}

function renderA2TDTaskStore() {
    queueA2TDTaskRender();
}



function setA2TDTasks(tasks) {
    const previousStore = new Map(a2tdTaskStore);
    a2tdTaskStore.clear();
    if (Array.isArray(tasks)) {
        tasks.forEach(task => {
            if (!task) return;
            const taskId = normalizeA2TDTaskId(task.task_id, task.filename);
            if (isA2TDTaskRemoved(taskId) || task.status === 'cancelled') return;
            const existing = previousStore.get(taskId) || {};
            const eventTs = parseA2TDTimestamp(task.updated_at || task.created_at) || getA2TDNumber(existing.last_event_at) || Date.now();
            a2tdTaskStore.set(taskId, {
                ...existing,
                ...task,
                task_id: taskId,
                last_event_at: eventTs,
                last_progress_change_at: getA2TDNumber(existing.last_progress_change_at) || eventTs,
            });
        });
    }
    renderA2TDTaskStore();
}


function upsertA2TDTask(task) {
    if (!task) return;
    const taskId = normalizeA2TDTaskId(task.task_id, task.filename);
    if (task.status === 'cancelled') {
        removeA2TDTask(taskId);
        return;
    }
    if (isA2TDTaskRemoved(taskId)) return;

    const existing = a2tdTaskStore.get(taskId) || {};
    const nowTs = Date.now();
    const nextTask = {
        ...existing,
        ...task,
        task_id: taskId,
        last_event_at: getA2TDNumber(task.last_event_at) || nowTs,
    };
    const existingStatus = existing.status || '';
    const nextStatus = nextTask.status || '';
    const existingDownload = getA2TDNumber(existing.download_progress);
    const incomingDownload = getA2TDNumber(task.download_progress ?? nextTask.download_progress);
    const existingUpload = getA2TDNumber(existing.upload_progress);
    const incomingUpload = getA2TDNumber(task.upload_progress ?? nextTask.upload_progress);

    if (["downloading", "paused", "uploading"].includes(nextStatus) && ["downloading", "paused", "uploading"].includes(existingStatus) && existingDownload > incomingDownload) {
        nextTask.download_progress = existingDownload;
    }
    if (nextStatus === 'uploading' && existingStatus === 'uploading' && existingUpload > incomingUpload) {
        nextTask.upload_progress = existingUpload;
    }

    const completedByProgress = incomingDownload >= 100 && incomingUpload >= 100;
    if (existingStatus === 'completed' || nextStatus === 'completed' || completedByProgress) {
        nextTask.status = 'completed';
        nextTask.download_progress = Math.max(100, existingDownload, incomingDownload);
        nextTask.upload_progress = Math.max(100, existingUpload, incomingUpload);
        nextTask.download_speed = '';
        nextTask.upload_speed = '';
        nextTask.eta_text = '';
        nextTask.connections = 0;
        nextTask.max_connections = 0;
    }

    const progressed = getA2TDNumber(nextTask.download_progress) > existingDownload || getA2TDNumber(nextTask.upload_progress) > existingUpload;

    if (progressed || hasA2TDSpeed(nextTask.download_speed) || hasA2TDSpeed(nextTask.upload_speed)) {
        nextTask.last_progress_change_at = nowTs;
    } else {
        nextTask.last_progress_change_at = getA2TDNumber(existing.last_progress_change_at) || nowTs;
    }

    a2tdTaskStore.set(taskId, nextTask);
    renderA2TDTaskStore();
}

function removeA2TDTask(taskId) {
    if (!taskId) return;
    const normalizedTaskId = normalizeA2TDTaskId(taskId);
    rememberRemovedA2TDTask(normalizedTaskId);
    a2tdPendingTaskActions.delete(normalizedTaskId);
    a2tdTaskStore.delete(normalizedTaskId);
    renderA2TDTaskStore();
}

function updateProgressBar(taskId, filename, mode, progress, speed, transferredText, totalText, eta, connections, status, maxConnections = 0, transferredBytes = 0, totalBytes = 0) {
    const normalizedTaskId = normalizeA2TDTaskId(taskId, filename);
    if (isA2TDTaskRemoved(normalizedTaskId, filename)) return;

    const existing = a2tdTaskStore.get(normalizedTaskId) || {};
    const existingDownload = getA2TDNumber(existing.download_progress);
    const existingUpload = getA2TDNumber(existing.upload_progress);
    const incomingProgress = getA2TDNumber(progress);
    const nowTs = Date.now();
    const nextTask = {
        ...existing,
        task_id: normalizedTaskId,
        filename: filename || existing.filename || normalizedTaskId,
        updated_at: existing.updated_at,
        last_event_at: nowTs,
        transferred_text: transferredText || existing.transferred_text || '',
        total_text: totalText || existing.total_text || existing.file_size || '',
        transferred_bytes: Math.max(getA2TDNumber(existing.transferred_bytes), getA2TDNumber(transferredBytes)),
        total_bytes: Math.max(getA2TDNumber(existing.total_bytes), getA2TDNumber(totalBytes)),
    };

    if (mode === 'upload') {
        const previousBytes = getA2TDNumber(existing.transferred_bytes);
        const previousTs = getA2TDNumber(existing.last_transfer_sample_at);
        let uploadSpeed = speed || existing.upload_speed || '';
        if (!uploadSpeed && getA2TDNumber(transferredBytes) > previousBytes && previousTs > 0) {
            const elapsed = (nowTs - previousTs) / 1000;
            if (elapsed > 0) uploadSpeed = `${formatBytes((getA2TDNumber(transferredBytes) - previousBytes) / elapsed)}/s`;
        }
        nextTask.status = status || 'uploading';
        nextTask.download_progress = Math.max(100, existingDownload);
        nextTask.upload_progress = Math.max(existingUpload, incomingProgress);
        nextTask.upload_speed = uploadSpeed;
        nextTask.file_size = totalText || existing.file_size || '';
        nextTask.eta_text = '';
        nextTask.connections = 0;
        nextTask.max_connections = 0;
        nextTask.last_transfer_sample_at = nowTs;
        nextTask.last_progress_change_at = nextTask.upload_progress > existingUpload || hasA2TDSpeed(uploadSpeed)
            ? nowTs
            : (getA2TDNumber(existing.last_progress_change_at) || nowTs);
    } else if (mode === 'done') {
        nextTask.status = 'completed';
        nextTask.download_progress = 100;
        nextTask.upload_progress = 100;
        nextTask.download_speed = '';
        nextTask.upload_speed = '';
        nextTask.eta_text = '';
        nextTask.connections = 0;
        nextTask.max_connections = 0;
        nextTask.last_progress_change_at = nowTs;
    } else {
        let nextStatus = status || existing.status || 'downloading';
        if (existing.status === 'paused' && nextStatus === 'downloading' && incomingProgress <= existingDownload) {
            nextStatus = 'paused';
        }
        nextTask.status = nextStatus;
        nextTask.download_progress = Math.max(existingDownload, incomingProgress);
        nextTask.download_speed = speed || (nextStatus === 'paused' ? '' : existing.download_speed || '');
        nextTask.file_size = totalText || existing.file_size || '';
        nextTask.downloaded_text = transferredText || existing.downloaded_text || '';
        nextTask.eta_text = eta || existing.eta_text || '';
        nextTask.connections = Math.max(0, getA2TDNumber(connections));
        nextTask.max_connections = Math.max(getA2TDNumber(existing.max_connections), getA2TDNumber(maxConnections), getA2TDNumber(connections));
        nextTask.last_progress_change_at = nextTask.download_progress > existingDownload || hasA2TDSpeed(nextTask.download_speed)
            ? nowTs
            : (getA2TDNumber(existing.last_progress_change_at) || nowTs);
    }

    upsertA2TDTask(nextTask);

    const a2tdPage = document.getElementById('page-aria2teldrive');
    if (a2tdPage && !a2tdPage.classList.contains('active') && mode === 'download' && progress < 2) {
        scheduleGuidedPageSwitch('aria2teldrive', '下载已开始，正在切换到下载监控...', 680);
    }

}








function renderProgressLogMeta(items = []) {
    const html = items.filter(Boolean).map(item => `<span class="log-meta-item">${item}</span>`).join('');
    return html ? `<div class="log-meta">${html}</div>` : '';
}

function buildProgressLogEntry(msg) {
    const icons = {
        task_start: '<i class="ph-fill ph-spinner-gap info" style="animation:spin 2s linear infinite"></i>',
        task_added: '<i class="ph ph-cloud-check info"></i>',
        task_status: '<i class="ph ph-hourglass-high warning"></i>',
        task_error: '<i class="ph-fill ph-warning-circle error"></i>',
        files_found: '<i class="ph ph-files"></i>',
        file_resolved: '<i class="ph-fill ph-file-arrow-down success"></i>',
        link_pushed: '<i class="ph ph-paper-plane-tilt success"></i>',
        push_done: '<i class="ph-fill ph-check-circle success"></i>',
        aria2_done: '<i class="ph-fill ph-check-circle success"></i>',
        task_done: '<i class="ph-fill ph-check-square success"></i>',
        all_done: '<i class="ph-fill ph-flag-checkered success"></i>',
        error: '<i class="ph-fill ph-x-circle error"></i>'
    };
    const icon = icons[msg.type] || '<i class="ph-fill ph-asterisk"></i>';
    const indexLabel = msg.index !== undefined && msg.index !== null
        ? `<span class="highlight">[${escapeA2TDHtml(msg.index)}]</span>`
        : '';
    let text = '';

    switch (msg.type) {
        case 'task_start': {
            const totalText = msg.total ? `/${escapeA2TDHtml(msg.total)}` : '';
            text = `<span class="highlight">[${escapeA2TDHtml(msg.index)}${totalText}]</span> 开始处理新的推送对象` + renderProgressLogMeta([
                `来源：<span class="log-path">${escapeA2TDHtml(msg.magnet || '')}</span>`
            ]);
            break;
        }
        case 'task_added':
            text = `${indexLabel} PikPak 离线任务创建成功：<span class="log-file">${escapeA2TDHtml(msg.file_name || '未命名对象')}</span>`
                + renderProgressLogMeta([
                    msg.task_id ? `任务ID：<span class="log-path">${escapeA2TDHtml(msg.task_id)}</span>` : ''
                ]);
            break;
        case 'task_status':
            text = `${indexLabel} ${escapeA2TDHtml(msg.status || '')}`;
            break;
        case 'task_error':
            text = `${indexLabel} <span class="error">${escapeA2TDHtml(msg.message || '处理失败')}</span>`;
            break;
        case 'files_found': {
            const preview = Array.isArray(msg.files) ? msg.files.slice(0, 3).map(item => escapeA2TDHtml(item)).join('、') : '';
            text = `${indexLabel} 检测到 ${escapeA2TDHtml((msg.files || []).length)} 个可用文件` + renderProgressLogMeta([
                preview ? `示例：${preview}${(msg.files || []).length > 3 ? ' ...' : ''}` : ''
            ]);
            break;
        }
        case 'file_resolved':
            text = `${indexLabel} 解析成功 [${escapeA2TDHtml(msg.sequence)}/${escapeA2TDHtml(msg.total_files)}] <span class="log-file">${escapeA2TDHtml(msg.file_name || '未命名文件')}</span>`
                + renderProgressLogMeta([
                    msg.file_path ? `路径：<span class="log-path">${escapeA2TDHtml(msg.file_path)}</span>` : '',
                    msg.file_size ? `大小：${escapeA2TDHtml(msg.file_size)}` : ''
                ]);
            break;
        case 'link_pushed':
            text = `${indexLabel} 下载链接已推送 [${escapeA2TDHtml(msg.sequence)}/${escapeA2TDHtml(msg.total_files)}] <span class="log-file">${escapeA2TDHtml(msg.file_name || '未命名文件')}</span>`
                + renderProgressLogMeta([
                    msg.target ? `目标：${escapeA2TDHtml(msg.target)}` : '',
                    msg.file_path ? `路径：<span class="log-path">${escapeA2TDHtml(msg.file_path)}</span>` : '',
                    msg.file_size ? `大小：${escapeA2TDHtml(msg.file_size)}` : ''
                ]);
            break;
        case 'push_done':
        case 'aria2_done':
            text = `${indexLabel} <span class="success">下载链接推送完成 ${escapeA2TDHtml(msg.success_count)}/${escapeA2TDHtml(msg.total_count)}</span>`
                + renderProgressLogMeta([
                    msg.target ? `目标：${escapeA2TDHtml(msg.target)}` : ''
                ]);
            break;
        case 'task_done':
            text = `${indexLabel} <span class="success">当前对象处理完成：${escapeA2TDHtml(msg.file_name || '未命名对象')}</span>`;
            break;
        case 'all_done': {
            text = `<span class="success">全部 ${escapeA2TDHtml(msg.total)} 个对象已处理完成</span>`;
            const btn = document.getElementById('submitBtn');
            if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ph ph-rocket-launch"></i> 一键推送'; }
            break;
        }
        case 'error': {
            text = `<span class="error">${escapeA2TDHtml(msg.message || '操作失败')}</span>`;
            const btnE = document.getElementById('submitBtn');
            if (btnE) { btnE.disabled = false; btnE.innerHTML = '<i class="ph ph-rocket-launch"></i> 一键推送'; }
            break;
        }
    }

    return text ? { icon, text } : null;
}

function addLogEntry(icon, text) {

    const container = document.getElementById('logContainer');
    if(!container) return;
    const empty = document.getElementById('logEmpty');
    if (empty) empty.remove();
    const now = new Date().toLocaleTimeString('zh-CN', { hour12: false });
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `<span class="log-icon">${icon}</span><span class="log-text">${text}</span><span class="log-time">${now}</span>`;
    container.appendChild(entry);
    container.scrollTop = container.scrollHeight;
}

function clearLog() {
    const el = document.getElementById('logContainer');
    if (el) el.innerHTML = '<div class="log-empty" id="logEmpty">系统处于空闲状态...</div>';
}

// ── PikPak Magnet Logic ──

// ── Aria2TelDrive Tasks & Stats ──

function formatBytes(bytes, decimals = 2) {
    if (!+bytes) return '0 B';
    const k = 1024, dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

async function loadA2TDTasks(force = false) {
    if (a2tdSnapshotPending && !force) return;
    try {
        a2tdSnapshotPending = true;
        const resp = await fetch('/api/a2td/snapshot');
        const data = await resp.json();
        if (Array.isArray(data.tasks)) setA2TDTasks(data.tasks);
        if (data.global_stat) renderA2TDStats(data.global_stat);
    } catch (e) {} finally {
        a2tdSnapshotPending = false;
    }
}

function refreshA2TDMonitorIfNeeded(force = false) {
    const page = document.getElementById('page-aria2teldrive');
    if (!page || !page.classList.contains('active')) return;
    loadA2TDTasks(force);
}


function renderA2TDStats(stats) {
    if (!stats) return;
    if (stats.cpu) {
        document.getElementById('sysCpuStat').textContent = `${stats.cpu.percent.toFixed(1)}%`;
    }
    if (stats.disk) {
        const totalBytes = stats.disk.total !== undefined
            ? stats.disk.total
            : getA2TDNumber(stats.disk.total_gb) * 1024 * 1024 * 1024;
        const usedBytes = stats.disk.used !== undefined
            ? stats.disk.used
            : getA2TDNumber(stats.disk.used_gb) * 1024 * 1024 * 1024;
        const totalStr = formatBytes(totalBytes || 0, 0);
        const usedStr = formatBytes(usedBytes || 0, 0);
        const diskEl = document.getElementById('sysDiskStat');
        diskEl.textContent = `${usedStr} / ${totalStr}`;
        diskEl.title = stats.disk.percent !== undefined
            ? `${getA2TDNumber(stats.disk.percent).toFixed(1)}%`
            : '';
    }

    if (stats.download_speed !== undefined) {
        const detail = stats.download_speed_detail || {};
        const externalSpeed = formatBytes(getA2TDNumber(detail.aria2));
        const builtinSpeed = formatBytes(getA2TDNumber(detail.builtin));
        const el = document.getElementById('sysDownloadStat');
        el.textContent = `${formatBytes(stats.download_speed)}/s`;
        el.title = `外部接收端: ${externalSpeed}/s | 内置下载器: ${builtinSpeed}/s`;
    }

    if (stats.upload_speed !== undefined) {
        document.getElementById('sysUploadStat').textContent = `${formatBytes(stats.upload_speed)}/s`;
    }
}


function escapeA2TDHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function getA2TDTaskStatusLabel(status) {
    const map = {
        pending: '等待中',
        downloading: '下载中',
        paused: '已暂停',
        uploading: '上传中',
        completed: '已完成',
        failed: '失败',
        cancelled: '已取消'
    };
    return map[status] || (status || '未知状态');
}

function formatA2TDRelativeTime(timestamp) {
    const ts = parseA2TDTimestamp(timestamp);
    if (!ts) return '刚刚';
    const diff = Math.max(0, Date.now() - ts);
    if (diff < 5000) return '刚刚';
    if (diff < 60000) return `${Math.floor(diff / 1000)} 秒前`;
    if (diff < 3600000) return `${Math.floor(diff / 60000)} 分钟前`;
    return `${Math.floor(diff / 3600000)} 小时前`;
}

function getA2TDUploadChunkStats(task) {
    const total = Math.max(0, getA2TDNumber(task.upload_chunk_total));
    const doneRaw = Math.max(0, getA2TDNumber(task.upload_chunk_done));
    const done = total > 0 ? Math.min(doneRaw, total) : doneRaw;
    return { done, total };
}

function getA2TDTaskProgress(task) {
    if (task.status === 'completed') return 100;
    const { done: uploadChunkDone, total: uploadChunkTotal } = getA2TDUploadChunkStats(task);
    let progress = 0;
    if (task.status === 'uploading' || uploadChunkTotal > 0) {
        progress = uploadChunkTotal > 0
            ? (uploadChunkDone / uploadChunkTotal) * 100
            : Number(task.upload_progress || 0);
    } else if (Number(task.upload_progress || 0) > 0 && Number(task.download_progress || 0) >= 100) {
        progress = Number(task.upload_progress || 0);
    } else {
        progress = Number(task.download_progress || 0);
    }
    if (task.status !== 'completed' && progress >= 100) return 99.9;
    return progress;
}



function getA2TDTaskMode(task) {
    if (task.status === 'completed') return 'done';
    const { total: uploadChunkTotal } = getA2TDUploadChunkStats(task);
    if (task.status === 'uploading' || uploadChunkTotal > 0 || (Number(task.upload_progress || 0) > 0 && Number(task.download_progress || 0) >= 100)) {
        return 'upload';
    }
    return 'download';
}


function isA2TDUploadReadyTask(task) {
    return !task.aria2_gid && (
        getA2TDNumber(task.download_progress) >= 100 ||
        getA2TDNumber(task.upload_progress) > 0
    );
}

function isA2TDTaskStalled(task) {

    if (task.status !== 'downloading') return false;
    if (getA2TDNumber(task.download_progress) >= 100) return false;
    const stalledFor = Date.now() - getA2TDNumber(task.last_progress_change_at);
    return !hasA2TDSpeed(task.download_speed) && stalledFor >= 15000;
}

function getA2TDActionButton(taskId, action, label, icon, tone = 'neutral') {
    const encodedTaskId = encodeURIComponent(taskId);
    return `<button class="btn btn-ghost btn-sm btn-action btn-action-${tone}" data-task-id="${escapeA2TDHtml(taskId)}" data-task-action="${action}" onclick="a2tdAction('${encodedTaskId}', '${action}')"><i class="ph ${icon}"></i> ${label}</button>`;
}

function getA2TDUploadActionLabel(task) {
    const { done, total } = getA2TDUploadChunkStats(task);
    if (total > 0) return `上传中 ${done}/${total}`;
    const progress = getA2TDNumber(task.upload_progress);
    return progress > 0 ? `上传中 ${progress.toFixed(1)}%` : '上传中';
}

function getA2TDTaskActions(task) {
    const pendingAction = a2tdPendingTaskActions.get(task.task_id);
    if (pendingAction) {
        return `<button class="btn btn-ghost btn-sm btn-action is-loading" disabled><span class="spinner"></span> ${escapeA2TDHtml(getA2TDTaskStatusLabel(task.status))}处理中</button>`;
    }

    if (task.status === 'downloading') {
        return `
            ${getA2TDActionButton(task.task_id, 'pause', '暂停', 'ph-pause', 'warning')}
            ${getA2TDActionButton(task.task_id, 'cancel', '取消', 'ph-x', 'danger')}
        `;
    }
    if (task.status === 'paused') {
        return `
            ${getA2TDActionButton(task.task_id, 'resume', isA2TDUploadReadyTask(task) ? '继续上传' : '恢复', 'ph-play', 'success')}
            ${getA2TDActionButton(task.task_id, 'cancel', '取消', 'ph-x', 'danger')}
        `;
    }
    if (task.status === 'uploading') {
        return `<button class="btn btn-ghost btn-sm btn-action is-loading" disabled><span class="spinner"></span> ${escapeA2TDHtml(getA2TDUploadActionLabel(task))}</button>`;
    }

    if (task.status === 'failed') {
        return `
            ${getA2TDActionButton(task.task_id, 'retry', '重试', 'ph-arrow-clockwise', 'warning')}
            ${getA2TDActionButton(task.task_id, 'delete', '删除', 'ph-trash', 'danger')}
        `;
    }
    if (task.status === 'pending') {
        return getA2TDActionButton(task.task_id, 'cancel', '取消', 'ph-x', 'danger');
    }
    return getA2TDActionButton(task.task_id, 'delete', '删除记录', 'ph-trash', 'neutral');
}


function getA2TDTaskCardId(task) {
    return 'pb-' + String(task?.task_id || 'unknown').replace(/[^a-zA-Z0-9_-]/g, '_');
}

function buildA2TDTaskCardContent(task) {
    const mode = getA2TDTaskMode(task);
    const stalled = isA2TDTaskStalled(task);
    const progress = Math.max(0, Math.min(100, getA2TDTaskProgress(task)));
    const filenameText = task.filename || task.task_id || '未命名任务';
    const statusLabel = escapeA2TDHtml(getA2TDTaskStatusLabel(task.status));
    const downloadProgress = Math.min(task.status === 'completed' ? 100 : 99.9, Number(task.download_progress || 0)).toFixed(1);
    const transferredText = mode === 'upload'
        ? `${task.transferred_text || '0 B'} / ${task.total_text || task.file_size || '--'}`
        : `${task.downloaded_text || '--'} / ${task.total_text || task.file_size || '--'}`;
    const speedText = task.download_speed || '0 B/s';
    const etaText = stalled
        ? '已无进度超过 15 秒'
        : (task.status === 'downloading' ? (task.eta_text || '--') : '--');
    const connectionText = task.status === 'downloading' || task.status === 'paused'
        ? `${Math.max(0, getA2TDNumber(task.connections))}/${Math.max(1, getA2TDNumber(task.max_connections) || 1)}`
        : '--';
    const activityText = formatA2TDRelativeTime(task.last_event_at || task.updated_at || task.created_at);
    const { done: uploadChunkDone, total: uploadChunkTotal } = getA2TDUploadChunkStats(task);
    const isUploadStage = mode === 'upload' || (task.status !== 'completed' && Number(task.upload_progress || 0) > 0 && Number(task.download_progress || 0) >= 100);
    const actionsHtml = getA2TDTaskActions(task);
    const inlineItems = [];

    const pushMetaItem = (text, className = 'muted') => {
        if (!text) return;
        inlineItems.push(`<span class="task-inline-item ${className}">${text}</span>`);
    };

    if (!['downloading', 'uploading', 'completed'].includes(task.status)) {
        pushMetaItem(statusLabel, 'status');
    }

    if (task.status === 'completed') {
        pushMetaItem('已完成', 'primary');
        pushMetaItem(`大小 ${escapeA2TDHtml(task.file_size || task.total_text || '--')}`);
    } else if (isUploadStage) {
        pushMetaItem(
            uploadChunkTotal > 0 ? `${uploadChunkDone}/${uploadChunkTotal} 块` : '等待上传',
            'primary upload'
        );
        pushMetaItem(`已确认 ${escapeA2TDHtml(transferredText)}`);
        if (task.upload_speed) {
            pushMetaItem(`速度 ${escapeA2TDHtml(task.upload_speed)}`);
        }
    } else {
        pushMetaItem(`${downloadProgress}%`, 'primary');
        pushMetaItem(`已下 ${escapeA2TDHtml(transferredText)}`);
        if (task.status === 'downloading' && speedText && speedText !== '--') {
            pushMetaItem(`速度 ${escapeA2TDHtml(speedText)}`);
        }
    }

    if ((task.status === 'downloading' || task.status === 'paused') && connectionText !== '--') {
        pushMetaItem(`连接 ${escapeA2TDHtml(connectionText)}`, 'secondary');
    }
    if (task.status === 'downloading' && etaText && etaText !== '--') {
        pushMetaItem(`剩余 ${escapeA2TDHtml(etaText)}`, 'secondary');
    }
    pushMetaItem(`最近活动 ${escapeA2TDHtml(activityText)}`, 'secondary');

    if (stalled) {
        pushMetaItem('<i class="ph ph-warning"></i> 疑似卡住', 'warning');
    }

    const uploadNote = task.upload_note
        ? `<div class="task-note ${task.upload_note_level === 'error' ? 'error' : 'warning'}"><i class="ph ${task.upload_note_level === 'error' ? 'ph-warning-circle' : 'ph-arrow-clockwise'}"></i><span>${escapeA2TDHtml(task.upload_note)}</span></div>`
        : '';
    const errorNote = task.error
        ? `<div class="task-note error"><i class="ph ph-warning-circle"></i><span>${escapeA2TDHtml(task.error)}</span></div>`
        : '';
    const stalledNote = !task.error && !task.upload_note && stalled
        ? '<div class="task-note warning"><i class="ph ph-warning"></i><span>连接长时间没有新数据，下载器会自动重试当前分块。</span></div>'
        : '';
    const barClass = `progress-bar-fill ${mode === 'done' ? 'done' : mode}`;
    const iconClass = stalled
        ? 'ph-warning-circle'
        : mode === 'upload'
            ? 'ph-upload-simple ul-icon'
            : task.status === 'completed'
                ? 'ph-check-circle'
                : 'ph-download-simple dl-icon';

    return {
        className: `progress-card ${task.status || 'pending'} ${mode === 'done' ? 'completed' : mode === 'upload' ? 'uploading' : 'downloading'} ${stalled ? 'stalled' : ''}`.trim(),
        filenameText,
        iconClass,
        actionsHtml,
        inlineRowHtml: inlineItems.join(''),
        barClass,
        progressWidth: `${progress}%`,
        notesHtml: `${uploadNote}${errorNote}${stalledNote}`
    };
}


function setA2TDHtmlIfChanged(element, html) {
    if (!element) return;
    const nextHtml = html || '';
    if ((element.dataset.renderHtml || '') === nextHtml) return;
    element.innerHTML = nextHtml;
    element.dataset.renderHtml = nextHtml;
}

function createA2TDTaskCardElement(task, view) {
    const card = document.createElement('div');
    card.id = getA2TDTaskCardId(task);
    card.innerHTML = `
        <div class="progress-header">
            <div class="progress-filename">
                <i class="ph" data-role="icon"></i>
                <span data-role="name"></span>
            </div>
            <div class="progress-header-actions" data-role="actions"></div>
        </div>
        <div class="task-inline-row" data-role="inline-row"></div>
        <div class="progress-bar-track">
            <div class="progress-bar-fill" data-role="bar"></div>
        </div>
        <div data-role="notes"></div>
    `;
    patchA2TDTaskCardElement(card, view);
    return card;
}

function patchA2TDTaskCardElement(card, view) {
    if (!card || !view) return;
    if (card.className !== view.className) card.className = view.className;

    const iconEl = card.querySelector('[data-role="icon"]');
    const nameEl = card.querySelector('[data-role="name"]');
    const actionsEl = card.querySelector('[data-role="actions"]');
    const inlineRowEl = card.querySelector('[data-role="inline-row"]');
    const barEl = card.querySelector('[data-role="bar"]');
    const notesEl = card.querySelector('[data-role="notes"]');

    const nextIconClass = `ph ${view.iconClass}`;
    if (iconEl && iconEl.className !== nextIconClass) iconEl.className = nextIconClass;
    if (nameEl && nameEl.textContent !== view.filenameText) nameEl.textContent = view.filenameText;
    setA2TDHtmlIfChanged(actionsEl, view.actionsHtml);
    setA2TDHtmlIfChanged(inlineRowEl, view.inlineRowHtml);
    if (barEl && barEl.className !== view.barClass) barEl.className = view.barClass;
    if (barEl && barEl.style.width !== view.progressWidth) barEl.style.width = view.progressWidth;
    setA2TDHtmlIfChanged(notesEl, view.notesHtml);
}


function renderA2TDTasks(tasks) {
    const container = document.getElementById('progressBarsContainer');
    const barsEl = document.getElementById('progressBars');
    const placeholder = document.getElementById('a2tdEmptyPlaceholder');
    if (!container || !barsEl || !placeholder) return;

    const sourceTasks = Array.isArray(tasks) ? tasks : [];
    syncA2TDTaskFilterButtons(sourceTasks);
    const visibleTasks = sourceTasks.filter(matchesA2TDTaskFilter);


    if (sourceTasks.length === 0) {
        barsEl.innerHTML = '';
        container.style.display = 'none';
        placeholder.style.display = 'block';
        placeholder.innerHTML = '<i class="ph ph-tray"></i> 队列空闲中';
        return;
    }

    if (visibleTasks.length === 0) {
        barsEl.innerHTML = '';
        container.style.display = 'none';
        placeholder.style.display = 'block';
        placeholder.innerHTML = `<i class="ph ph-funnel"></i> 当前筛选“${escapeA2TDHtml(A2TD_TASK_FILTER_LABELS[a2tdTaskFilter] || '全部')}”下暂无任务`;
        return;
    }

    container.style.display = 'block';
    placeholder.style.display = 'none';

    const activeIds = new Set();
    visibleTasks.forEach((task, index) => {
        const cardId = getA2TDTaskCardId(task);
        const view = buildA2TDTaskCardContent(task);
        let card = document.getElementById(cardId);
        if (!card) {
            card = createA2TDTaskCardElement(task, view);
        } else {
            patchA2TDTaskCardElement(card, view);
        }

        const anchor = barsEl.children[index] || null;
        if (card !== anchor) {
            barsEl.insertBefore(card, anchor);
        }
        activeIds.add(cardId);
    });

    Array.from(barsEl.children).forEach(card => {
        if (!activeIds.has(card.id)) card.remove();
    });
}




async function a2tdAction(taskId, action) {
    const rawTaskId = decodeURIComponent(taskId);
    a2tdPendingTaskActions.set(rawTaskId, action);
    renderA2TDTaskStore();
    try {
        const url = action === 'delete' ? `/api/a2td/task/${taskId}` : `/api/a2td/task/${taskId}/${action}`;
        const method = action === 'delete' ? 'DELETE' : 'POST';
        const resp = await fetch(url, { method });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) throw new Error(data.detail || data.message || '任务操作失败');

        if (action === 'cancel' || action === 'delete') {
            removeA2TDTask(rawTaskId);
        } else if (action === 'pause') {
            upsertA2TDTask({ task_id: rawTaskId, status: 'paused', download_speed: '', last_event_at: Date.now() });
        } else if (action === 'resume') {
            upsertA2TDTask({ task_id: rawTaskId, status: 'downloading', last_event_at: Date.now() });
            await loadA2TDTasks();
        } else {
            await loadA2TDTasks();
        }
        showA2TDToast(data.message || '操作已提交', 'success');
    } catch(e) {
        showA2TDToast(e.message || '任务操作失败', 'error');
    } finally {
        a2tdPendingTaskActions.delete(rawTaskId);
        renderA2TDTaskStore();
    }
}

async function a2tdBulkAction(action) {
    const btn = document.querySelector(`[data-bulk-action="${action}"]`);
    const oldHtml = btn ? btn.innerHTML : '';
    try {
        if (btn) {
            btn.disabled = true;
            btn.classList.add('is-loading');
            btn.innerHTML = '<span class="spinner"></span> 执行中';
        }
        const resp = await fetch(`/api/a2td/tasks/${action}`, { method: 'POST' });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) throw new Error(data.detail || data.message || '批量操作失败');
        await loadA2TDTasks();
        showA2TDToast(data.message || '批量操作已完成', 'success');
    } catch(e) {
        showA2TDToast(e.message || '批量操作失败', 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.classList.remove('is-loading');
            btn.innerHTML = oldHtml;
        }
    }
}

async function clearCompletedTasks() {
    return a2tdBulkAction('clear-completed');
}



// ── Settings ──
function togglePikpakLoginMode() {
    const mode = document.getElementById('cfgPikpakLoginMode')?.value || 'password';
    const passwordFields = document.getElementById('cfgPikpakPasswordFields');
    const sessionFields = document.getElementById('cfgPikpakSessionFields');
    if (passwordFields) passwordFields.style.display = mode === 'session' ? 'none' : 'grid';
    if (sessionFields) sessionFields.style.display = mode === 'session' ? 'grid' : 'none';
}

function extractPikpakSessionValue(rawText) {
    const text = String(rawText || '').replace(/^\uFEFF/, '').trim();
    if (!text) return '';
    try {
        const parsed = JSON.parse(text);
        if (typeof parsed === 'string' && parsed.trim()) return parsed.trim();
        if (parsed && typeof parsed === 'object') {
            for (const key of ['encoded_token', 'session', 'token']) {
                if (typeof parsed[key] === 'string' && parsed[key].trim()) {
                    return parsed[key].trim();
                }
            }
        }
    } catch (e) {}
    return text;
}

async function importPikpakSessionFile(event) {
    const fileInput = event?.target;
    const file = fileInput?.files?.[0];
    if (!file) return;

    const modeInput = document.getElementById('cfgPikpakLoginMode');
    const sessionInput = document.getElementById('cfgPikpakSession');
    const fileNameLabel = document.getElementById('cfgPikpakSessionFileName');

    try {
        const rawText = await file.text();
        const session = extractPikpakSessionValue(rawText);
        if (!session) throw new Error('文件中未识别到有效的 Session / encoded_token');

        if (modeInput) {
            modeInput.value = 'session';
            togglePikpakLoginMode();
        }
        if (sessionInput) sessionInput.value = session;
        if (fileNameLabel) fileNameLabel.textContent = `已导入: ${file.name}`;

        await doAutoSave(sessionInput || modeInput || fileInput);
    } catch (e) {
        if (fileNameLabel) fileNameLabel.textContent = '导入失败';
        alert('Session 文件导入失败: ' + (e.message || '未知错误'));
    } finally {
        if (fileInput) fileInput.value = '';
    }
}

function collectSettingsConfig() {

    return {
        auth: {
            username: document.getElementById('cfgAuthUser').value.trim(),
            password: document.getElementById('cfgAuthPass').value.trim()
        },
        server: { port: parseInt(document.getElementById('cfgServerPort').value) || 8888 },
        pikpak: {
            login_mode: document.getElementById('cfgPikpakLoginMode').value || 'password',
            username: document.getElementById('cfgPikpakUsername').value,
            password: document.getElementById('cfgPikpakPassword').value,
            session: document.getElementById('cfgPikpakSession').value.trim(),
            save_dir: document.getElementById('cfgPikpakSaveDir').value || '/',
            delete_after_download: document.getElementById('cfgPikpakDelete').checked,
            download_engine: document.getElementById('cfgPikpakEngine').value || 'builtin',
            max_concurrent_downloads: parseInt(document.getElementById('cfgPikpakMaxDownloads').value) || 3,
            connections_per_task: parseInt(document.getElementById('cfgPikpakConnections').value) || 8
        },
        aria2: {
            rpc_url: document.getElementById('cfgAria2Url').value,
            rpc_secret: document.getElementById('cfgAria2Secret').value,
            download_dir: document.getElementById('cfgAria2Dir').value
        },
        teldrive: {
            api_host: document.getElementById('cfgTeldriveHost').value,
            access_token: document.getElementById('cfgTeldriveToken').value,
            channel_id: parseInt(document.getElementById('cfgTeldriveChannel').value) || 0,
            upload_concurrency: parseInt(document.getElementById('cfgTeldriveConcurrency').value) || 4,
            chunk_size: "500M"
        },
        upload: {
            auto_delete: document.getElementById('cfgUploadAutoDelete').checked,
            max_retries: 3
        },

        telegram: {
            api_id: parseInt(document.getElementById('cfgTelegramApiId').value) || 0,
            api_hash: document.getElementById('cfgTelegramApiHash').value,
            channel_id: parseInt(document.getElementById('cfgTelegramChannelId').value) || 0,
            sync_interval: parseInt(document.getElementById('cfgTelegramSyncInterval').value) || 10,
            sync_enabled: document.getElementById('cfgTelegramSyncEnabled').checked
        },
        telegram_db: {
            host: document.getElementById('cfgDbHost').value,
            port: parseInt(document.getElementById('cfgDbPort').value) || 5432,
            user: document.getElementById('cfgDbUser').value,
            password: document.getElementById('cfgDbPassword').value,
            name: document.getElementById('cfgDbName').value || 'postgres'
        }
    };
}

async function loadConfig() {

    try {
        const resp = await fetch('/api/settings');
        const cfg = await resp.json();
        
        document.getElementById('cfgAuthUser').value = cfg.auth?.username || '';
        document.getElementById('cfgAuthPass').value = cfg.auth?.password || '';
        
        document.getElementById('cfgServerPort').value = cfg.server?.port || 8888;

        document.getElementById('cfgPikpakLoginMode').value = cfg.pikpak?.login_mode || 'password';
        document.getElementById('cfgPikpakUsername').value = cfg.pikpak?.username || '';
        document.getElementById('cfgPikpakPassword').value = cfg.pikpak?.password || '';
        document.getElementById('cfgPikpakSession').value = cfg.pikpak?.session || '';
        document.getElementById('cfgPikpakSessionFileName').textContent = cfg.pikpak?.session ? '已保存 Session' : '未选择文件';
        document.getElementById('cfgPikpakSaveDir').value = cfg.pikpak?.save_dir || '/';

        document.getElementById('cfgPikpakDelete').checked = cfg.pikpak?.delete_after_download || false;
        document.getElementById('cfgPikpakEngine').value = cfg.pikpak?.download_engine || 'builtin';
        document.getElementById('cfgPikpakMaxDownloads').value = cfg.pikpak?.max_concurrent_downloads || 3;
        document.getElementById('cfgPikpakConnections').value = cfg.pikpak?.connections_per_task || 8;
        togglePikpakLoginMode();

        
        document.getElementById('cfgAria2Url').value = cfg.aria2?.rpc_url || '';
        document.getElementById('cfgAria2Secret').value = cfg.aria2?.rpc_secret || '';
        document.getElementById('cfgAria2Dir').value = cfg.aria2?.download_dir || '';
        
        document.getElementById('cfgTeldriveHost').value = cfg.teldrive?.api_host || '';
        document.getElementById('cfgTeldriveToken').value = cfg.teldrive?.access_token || '';
        document.getElementById('cfgTeldriveChannel').value = cfg.teldrive?.channel_id || 0;
        document.getElementById('cfgTeldriveConcurrency').value = cfg.teldrive?.upload_concurrency || 4;
        
        document.getElementById('cfgUploadAutoDelete').checked = cfg.upload?.auto_delete || false;

        document.getElementById('cfgTelegramApiId').value = cfg.telegram?.api_id || '';
        document.getElementById('cfgTelegramApiHash').value = cfg.telegram?.api_hash || '';
        document.getElementById('cfgTelegramChannelId').value = cfg.telegram?.channel_id || 0;
        document.getElementById('cfgTelegramSyncInterval').value = cfg.telegram?.sync_interval || 10;
        document.getElementById('cfgTelegramSyncEnabled').checked = cfg.telegram?.sync_enabled !== false;

        document.getElementById('cfgDbHost').value = cfg.telegram_db?.host || '';
        document.getElementById('cfgDbPort').value = cfg.telegram_db?.port || 5432;
        document.getElementById('cfgDbUser').value = cfg.telegram_db?.user || '';
        document.getElementById('cfgDbPassword').value = cfg.telegram_db?.password || '';
        document.getElementById('cfgDbName').value = cfg.telegram_db?.name || 'postgres';
        
    } catch (e) {
        console.error('加载配置失败:', e);
    }
}

async function saveConfig() {
    const btn = document.getElementById('saveBtn');
    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> 保存...';
    
    const cfg = collectSettingsConfig();
    

    try {
        const resp = await fetch('/api/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
        if (resp.ok) { 
            const msg = document.getElementById('saveMsg'); 
            msg.classList.add('show'); 
            setTimeout(() => msg.classList.remove('show'), 2500); 
        }
    } catch (e) { 
        alert('保存失败: ' + e.message); 
    }
    btn.disabled = false; btn.innerHTML = '<i class="ph ph-floppy-disk"></i> 闪存同步';
}

// ── Auto-save: 修改设置后自动保存并显示绿勾 ──
let _autoSaveTimer = null;
function initAutoSave() {
    const settingsPage = document.getElementById('page-settings');
    if (!settingsPage) return;

    const inputs = settingsPage.querySelectorAll('input.form-input, textarea.form-input, select.form-input, input[type="checkbox"]');

    inputs.forEach(input => {
        const eventType = input.type === 'checkbox' ? 'change' : 'change';
        input.addEventListener(eventType, () => {
            // debounce 500ms
            if (_autoSaveTimer) clearTimeout(_autoSaveTimer);
            _autoSaveTimer = setTimeout(() => doAutoSave(input), 500);
        });
    });
}

async function doAutoSave(triggerInput) {
    const cfg = collectSettingsConfig();


    try {
        const resp = await fetch('/api/settings', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cfg)
        });
        if (resp.ok) showFieldCheck(triggerInput);
    } catch (e) { /* silent fail */ }
}

function showFieldCheck(input) {
    // 在输入框旁边显示一个临时绿色对号
    const parent = input.closest('.form-group') || input.closest('.toggle-row') || input.parentElement;
    if (!parent) return;

    // 避免重复
    const old = parent.querySelector('.auto-save-check');
    if (old) old.remove();

    const check = document.createElement('span');
    check.className = 'auto-save-check';
    check.innerHTML = '<i class="ph-fill ph-check-circle"></i>';
    check.style.cssText = 'color:var(--success); font-size:16px; margin-left:6px; opacity:0; transition:opacity 0.3s; display:inline-flex; align-items:center;';
    
    // 对 checkbox toggle 特殊处理位置
    if (input.type === 'checkbox') {
        parent.appendChild(check);
    } else {
        // 插在 input 后面
        input.parentElement.style.position = 'relative';
        check.style.cssText += 'position:absolute; right:12px; top:50%; transform:translateY(-50%);';
        input.parentElement.appendChild(check);
    }

    requestAnimationFrame(() => { check.style.opacity = '1'; });
    setTimeout(() => {
        check.style.opacity = '0';
        setTimeout(() => check.remove(), 300);
    }, 2000);
}

// ── Startup ──
window.onload = () => {
    checkSetupRequired();
    connectWS();
    checkServicesStatus();
    setInterval(checkServicesStatus, 30000);
    setInterval(() => refreshA2TDMonitorIfNeeded(), 4000);
    document.addEventListener('visibilitychange', () => {
        if (!document.hidden) refreshA2TDMonitorIfNeeded(true);
    });
    initAutoSave();
    // 监听 Tel2TelDrive SSE 事件
    const es = new EventSource('/api/t2td/stream');
    es.onmessage = (e) => {
        try {
            const data = JSON.parse(e.data);
            if(data.type === "state" || data.type === "qr" || data.type === "password_required") {
                updateT2TDState(data.payload || data);
            } else if(data.type === "log") {
                appendT2TDLog(data.payload || data);
            }
        }catch(e){}
    };
};


// ── Tel2TelDrive Integration ──
let t2tdQrRefreshPending = false;

function formatT2TDExpireAt(expiresAt) {
    if (!expiresAt) return '';
    const date = new Date(expiresAt);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleString('zh-CN', { hour12: false });
}

async function loadT2TDState() {
    try {
        const res = await fetch('/api/t2td/bootstrap');
        const d = await res.json();
        updateT2TDState(d.state);
        const container = document.getElementById('t2tdLogContainer');
        if (container && d.logs) {
            container.innerHTML = '';
            d.logs.forEach(l => appendT2TDLog(l));
        }
    } catch(e) {}
}

async function refreshT2TDQr(manual = false) {
    if (t2tdQrRefreshPending) return;
    const area = document.getElementById('t2tdQrArea');
    const qrImg = document.getElementById('t2tdQrImg');
    const hint = document.getElementById('t2tdQrHint');
    try {
        t2tdQrRefreshPending = true;
        if (qrImg) {
            qrImg.style.display = 'none';
            qrImg.removeAttribute('src');
            qrImg.style.pointerEvents = 'none';
        }
        if (hint) {
            hint.style.display = 'block';
            hint.textContent = '正在获取新二维码...';
        }
        if (area) {
            const text = area.querySelector('p');
            if (text) text.textContent = manual ? '二维码刷新中，请稍候...' : '二维码获取中，请稍候...';
        }
        const resp = await fetch('/api/t2td/login/refresh', { method: 'POST' });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.detail || '刷新二维码失败');
        }
    } catch (e) {
        t2tdQrRefreshPending = false;
        if (hint) hint.textContent = e.message || '刷新二维码失败';
        if (qrImg) qrImg.style.pointerEvents = 'auto';
        alert(e.message || '刷新二维码失败');
    }
}

function updateT2TDState(state) {
    const area = document.getElementById('t2tdQrArea');
    const qrImg = document.getElementById('t2tdQrImg');
    const form = document.getElementById('t2td2faForm');
    const hint = document.getElementById('t2tdQrHint');
    if (!area || !qrImg || !form) return;

    const text = area.querySelector('p');
    const expireText = formatT2TDExpireAt(state.qr_expires_at);

    if (state.phase === 'awaiting_qr' || (state.qr_image && !state.authorized)) {
        form.style.display = 'none';
        if (state.qr_image || state.url) {
            t2tdQrRefreshPending = false;
            qrImg.style.display = 'block';
            qrImg.style.pointerEvents = 'auto';
            qrImg.src = state.qr_image || state.url;
            if (text) text.textContent = `请使用 Telegram App 扫描登录二维码 (${state.session_name || ''})`;
            if (hint) {
                hint.style.display = 'block';
                hint.textContent = expireText ? `二维码有效至 ${expireText}，点击二维码可主动刷新` : '点击二维码可主动刷新';
            }
        } else {
            qrImg.style.display = 'none';
            qrImg.removeAttribute('src');
            if (text) text.textContent = t2tdQrRefreshPending ? '二维码刷新中，请稍候...' : '二维码获取中，请稍候...';
            if (hint) {
                hint.style.display = 'block';
                hint.textContent = '正在获取新二维码...';
            }
        }
    } else if (state.type === 'password_required' || (state.phase === 'awaiting_password')) {
        t2tdQrRefreshPending = false;
        qrImg.style.display = 'none';
        qrImg.removeAttribute('src');
        qrImg.style.pointerEvents = 'auto';
        form.style.display = 'block';
        if (hint) hint.style.display = 'none';
        if (text) text.textContent = '两步验证: 账号存在密码锁，请在此输入';
        form.onsubmit = async (e) => {
            e.preventDefault();
            const pass = document.getElementById('t2tdPassword').value;
            await fetch('/api/t2td/login/password', { method:'POST', body: JSON.stringify({password: pass}), headers: {'Content-Type': 'application/json'}});
        }
    } else if (state.authorized || state.phase === 'running' || state.phase === 'authorized') {
        t2tdQrRefreshPending = false;
        qrImg.style.display = 'none';
        qrImg.removeAttribute('src');
        qrImg.style.pointerEvents = 'auto';
        form.style.display = 'none';
        if (hint) hint.style.display = 'none';
        if (text) text.innerHTML = `<span style="color:var(--success)"><b><i class="ph-fill ph-check-circle"></i> 服务运行中</b></span> - 频道监听已激活`;
    } else {
        t2tdQrRefreshPending = false;
        qrImg.style.display = 'none';
        qrImg.removeAttribute('src');
        qrImg.style.pointerEvents = 'auto';
        form.style.display = 'none';
        if (hint) {
            hint.style.display = 'block';
            hint.textContent = state.last_error || state.phase_label || '服务准备中...';
        }
        if (text) text.textContent = state.last_error || state.phase_label || '服务准备中...';
    }
}


function appendT2TDLog(log) {
    const container = document.getElementById('t2tdLogContainer');
    if (!container) return;
    const empty = document.getElementById('t2tdLogEmpty');
    if (empty) empty.remove();
    
    // Normalize log fields (handle both direct object and payload wrapper)
    const logData = log.payload || log;
    
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    let c = '#fff';
    if(logData.level === 'ERROR') c = '#f87171';
    else if(logData.level === 'WARN' || logData.level === 'WARNING') c = '#f59e0b';
    
    // Extract time from timestamp
    let t = logData.time || logData.timestamp || '(none)';
    if (t && t.includes('T')) {
        t = t.split('T')[1].split('.')[0] || t;
        t = t.replace('Z', '').replace(/[+-]\d+:\d+$/, ''); // Strip extra timezone fragments visually
    }

    entry.innerHTML = `<span style="color:${c}">[${t}] [${logData.level || 'INFO'}] ${logData.message || ''}</span>`;
    container.appendChild(entry);
    container.scrollTop = container.scrollHeight;
}
// ==========================================


// ==========================================
// PikPak Magnet, Share & RSS Parsing Implementations
// ==========================================

let magnetCurrentFileId = null;
let magnetFileData = [];

// === Magnet Parsing ===
async function parseMagnet() {
    const input = document.getElementById('magnetInput').value.trim();
    if (!input) return alert('请输入磁力链接');
    const parseBtn = document.getElementById('magnetParseBtn');
    parseBtn.disabled = true;
    parseBtn.innerHTML = '<span class="spinner"></span> 解析中...';
    document.getElementById('magnetFileArea').style.display = 'none';

    try {
        const resp = await fetch('/api/pikpak/magnet/parse', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ magnet: input.split('\n')[0] })
        });
        const data = await resp.json();
        
        if (!resp.ok) {
            throw new Error(data.error || '解析失败');
        }

        magnetCurrentFileId = data.file_id;
        document.getElementById('magnetFileName').innerHTML = `<i class="ph-fill ph-folder-open"></i> ${data.file_name}`;
        
        magnetFileData = data.files || [];
        renderPickerTree('magnetFileList', magnetFileData, 'magnet');
        
        document.getElementById('magnetFileArea').style.display = 'block';
        updatePickerSelection('magnet');
    } catch(e) {
        alert(e.message);
    } finally {
        parseBtn.disabled = false;
        parseBtn.innerHTML = '<i class="ph ph-magnifying-glass"></i> 解析后选择';
    }
}

async function submitMagnets() {
    const input = document.getElementById('magnetInput').value.trim();
    if (!input) return alert('请输入磁力链接');
    
    const btn = document.getElementById('submitBtn');
    if(!btn) return;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 提交中...';

    try {
        const resp = await fetch('/api/pikpak/add', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ magnets: input })
        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-rocket-launch"></i> 一键推送';
    } 
}


function toggleMagnetSelectAll() {
    const isChecked = document.getElementById('magnetSelectAll').checked;
    const checkboxes = document.querySelectorAll('#magnetFileList input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = isChecked);
    updatePickerSelection('magnet');
}

async function downloadMagnetFiles() {
    if (!magnetCurrentFileId) return;
    const checkboxes = document.querySelectorAll('#magnetFileList input[type="checkbox"]:checked');
    const selectedIds = Array.from(checkboxes).map(cb => cb.value);
    
    if (!selectedIds.length) {
        return alert('请先选择需要下载的文件');
    }

    const keepStructure = document.getElementById('magnetKeepStructure').checked;
    const btn = document.getElementById('magnetDownloadBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 提交中...';

    try {
        const resp = await fetch('/api/pikpak/magnet/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                file_id: magnetCurrentFileId,
                selected_ids: selectedIds,
                keep_structure: keepStructure
            })
        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-download-simple"></i> 推送下载链接';


    }
}


// === Share Parsing ===
let shareCurrentData = null;
let shareFileData = [];

async function parseShareLink() {
    const shareLink = document.getElementById('shareLink').value.trim();
    const passCode = document.getElementById('sharePassCode').value.trim();
    if (!shareLink) return alert('请输入 分享链接');

    const parseBtn = document.getElementById('shareParseBtn');
    parseBtn.disabled = true;
    parseBtn.innerHTML = '<span class="spinner"></span> 解析中...';
    document.getElementById('shareFileArea').style.display = 'none';

    try {
        const resp = await fetch('/api/pikpak/share/list', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ share_link: shareLink, pass_code: passCode })
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || '解析失败');

        shareCurrentData = data;
        shareFileData = data.files || [];
        renderPickerTree('fileList', shareFileData, 'share');
        
        document.getElementById('shareFileArea').style.display = 'block';
        updatePickerSelection('share');
    } catch(e) {
        alert(e.message);
    } finally {
        parseBtn.disabled = false;
        parseBtn.innerHTML = '<i class="ph ph-magnifying-glass"></i> 解析';
    }
}

function toggleSelectAll() {
    const isChecked = document.getElementById('selectAll').checked;
    const checkboxes = document.querySelectorAll('#fileList input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = isChecked);
    updatePickerSelection('share');
}

function reRenderShareFileList() {
    renderPickerTree('fileList', shareFileData, 'share');
}

async function downloadShareFiles() {
    if (!shareCurrentData) return;
    const checkboxes = document.querySelectorAll('#fileList input[type="checkbox"]:checked');
    const selectedIds = Array.from(checkboxes).map(cb => cb.value);
    
    if (!selectedIds.length) return alert('请先选择需要下载的分享节点');

    const keepStructure = document.getElementById('shareKeepStructure').checked;
    const renameByFolder = document.getElementById('shareRenameByFolder').checked;
    const filePaths = Object.fromEntries(
        shareFileData
            .filter(item => selectedIds.includes(item.id))
            .map(item => [item.id, item.path || item.name || ''])
    );
    
    const btn = document.getElementById('downloadShareBtn');

    if(!btn) return;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 同步中...';

    try {
        const resp = await fetch('/api/pikpak/share/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                share_id: shareCurrentData.share_id,
                file_ids: selectedIds,
                pass_code_token: shareCurrentData.pass_code_token,
                keep_structure: keepStructure,
                file_paths: filePaths,
                rename_by_folder: renameByFolder
            })

        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-cloud-arrow-down"></i> 执行下载';

    }
}

// === RSS Parsing ===
let rssFileData = [];

async function parseRSS() {
    const url = document.getElementById('rssUrl').value.trim();
    if (!url) return alert('请输入 RSS 地址');

    const parseBtn = document.getElementById('rssParseBtn');
    parseBtn.disabled = true;
    parseBtn.innerHTML = '<span class="spinner"></span> 扫描中...';
    document.getElementById('rssResultArea').style.display = 'none';

    try {
        const resp = await fetch('/api/pikpak/rss/parse', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url: url })
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || '扫描失败');

        document.getElementById('rssFeedTitle').innerHTML = `<i class="ph ph-feed"></i> ${data.title} (${data.count} 项)`;
        rssFileData = data.items || [];
        renderPickerTree('rssList', rssFileData, 'rss');
        
        document.getElementById('rssResultArea').style.display = 'block';
        updatePickerSelection('rss');
    } catch(e) {
        alert(e.message);
    } finally {
        parseBtn.disabled = false;
        parseBtn.innerHTML = '<i class="ph ph-radar"></i> 扫描提取';
    }
}

function toggleRssSelectAll() {
    const isChecked = document.getElementById('rssSelectAll').checked;
    const checkboxes = document.querySelectorAll('#rssList input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = isChecked);
    updatePickerSelection('rss');
}

async function downloadRssItems() {
    const checkboxes = document.querySelectorAll('#rssList input[type="checkbox"]:checked');
    const selectedUrls = Array.from(checkboxes).map(cb => cb.value);
    
    if (!selectedUrls.length) return alert('请先选择需要订阅的项目');

    const btn = document.getElementById('rssDownloadBtn');
    if(!btn) return;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 提交中...';

    try {
        const resp = await fetch('/api/pikpak/rss/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ urls: selectedUrls })
        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-download-simple"></i> 执行订阅下载';

    }
}


// === File Picker UI Utils ===

function renderPickerTree(containerId, files, prefix) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    if (!files || files.length === 0) {
        container.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-dim);">没有找到任何内容或空文件夹</div>';
        return;
    }

    let html = '';
    files.forEach(f => {
        let isFolder = !!f.original_url && f.size_str === 0 && !f.extension; 
        if (f.type === 'folder' || f.mime_type === 'application/vnd.google-apps.folder') isFolder = true;
        
        let icon = isFolder ? '<i class="ph-fill ph-folder"></i>' : '<i class="ph-fill ph-file"></i>';
        let rowClass = isFolder ? 'file-row folder-row' : 'file-row';
        
        // For RSS, the value is the download_url, for others it's file_id / id
        let val = f.download_url || f.file_id || f.id;
        let title = f.name || f.title;
        let sizeOrTime = f.size_str || f.published || '0 B';
        
        html += `
            <label class="${rowClass}">
                <input type="checkbox" value="${val}" onchange="updatePickerSelection('${prefix}')" checked>
                <div class="file-icon">${icon}</div>
                <div class="file-name" title="${title}">${title}</div>
                <div class="file-size">${sizeOrTime}</div>
            </label>
        `;
    });
    
    container.innerHTML = html;
}

function updatePickerSelection(prefix) {
    let cbList = [];
    let countSpan = null;
    let selectedInfoSpan = null;
    
    if (prefix === 'magnet') {
        cbList = document.querySelectorAll('#magnetFileList input[type="checkbox"]');
        countSpan = document.getElementById('magnetFileCount');
        selectedInfoSpan = document.getElementById('magnetSelectedInfo');
    } else if (prefix === 'share') {
        cbList = document.querySelectorAll('#fileList input[type="checkbox"]');
        countSpan = document.getElementById('fileCount');
        selectedInfoSpan = document.getElementById('selectedInfo');
    } else if (prefix === 'rss') {
        cbList = document.querySelectorAll('#rssList input[type="checkbox"]');
        countSpan = document.getElementById('rssCount');
        selectedInfoSpan = document.getElementById('rssSelectedInfo');
    }

    if (!cbList || cbList.length === 0) return;

    let total = cbList.length;
    let checkedCount = 0;
    cbList.forEach(cb => { if(cb.checked) checkedCount++; });

    if (countSpan) countSpan.textContent = `共 ${total} 项`;
    if (selectedInfoSpan) {
        if (prefix === 'rss') {
            selectedInfoSpan.textContent = `已选 ${checkedCount} 项准备下载`;
        } else {
            selectedInfoSpan.textContent = `已选 ${checkedCount} 个对象`;
        }
    }
}
