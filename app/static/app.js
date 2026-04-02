
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
function switchPage(name) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.getElementById('page-' + name).classList.add('active');
    const navItem = document.querySelector(`.nav-item[data-page="${name}"]`);
    if(navItem) navItem.classList.add('active');
    
    if (name === 'tasks') refreshPikPakTasks();
    if (name === 'aria2teldrive') loadA2TDTasks();
    if (name === 'tel2teldrive') loadT2TDState();
    if (name === 'settings') loadConfig();
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
        if (msg.data.tasks) renderA2TDTasks(msg.data.tasks);
        if (msg.data.global_stat) renderA2TDStats(msg.data.global_stat);
        return;
    }
    if (msg.type === "global_stat") {
        renderA2TDStats(msg.data);
        return;
    }
    if (msg.type === "tasks_update") {
        renderA2TDTasks(msg.data);
        return;
    }
    if (msg.type === "task_update") {
        loadA2TDTasks();
        return;
    }

    // ── 内置引擎进度条 ──
    if (msg.type === "download_progress") {
        updateProgressBar(msg.filename, 'download', msg.progress, msg.speed, msg.downloaded, msg.total, msg.eta, msg.connections, msg.status);
        return;
    }
    if (msg.type === "upload_progress") {
        updateProgressBar(msg.filename, 'upload', msg.progress, '', msg.uploaded, msg.total, '', 0, 'uploading');
        return;
    }
    if (msg.type === "upload_done") {
        updateProgressBar(msg.filename, 'done', 100, '', '', '', '', 0, 'completed');
        // 3 秒后淡出移除进度条
        setTimeout(() => {
            const card = document.getElementById('pb-' + CSS.escape(msg.filename));
            if (card) { card.style.opacity = '0'; setTimeout(() => card.remove(), 500); }
            // 隐藏容器
            const barsEl = document.getElementById('progressBars');
            if (barsEl && barsEl.children.length === 0) {
                document.getElementById('progressBarsContainer').style.display = 'none';
            }
        }, 3000);
        return;
    }
    
    // PikPak log messages
    const icons = { task_start: '<i class="ph-fill ph-spinner-gap info" style="animation:spin 2s linear infinite"></i>', task_added: '<i class="ph ph-cloud-arrow-up info"></i>', task_status: '<i class="ph ph-hourglass-high warning"></i>', task_error: '<i class="ph-fill ph-warning-circle error"></i>', files_found: '<i class="ph ph-files"></i>', aria2_done: '<i class="ph-fill ph-check-circle success"></i>', task_done: '<i class="ph-fill ph-check-square success"></i>', all_done: '<i class="ph-fill ph-flag-checkered success"></i>', error: '<i class="ph-fill ph-x-circle error"></i>' };
    const icon = icons[msg.type] || '<i class="ph-fill ph-asterisk"></i>';
    let text = '';
    switch (msg.type) {
        case 'task_start': text = `<span class="highlight">[${msg.index}/${msg.total}]</span> 开始处理: ${msg.magnet}`; break;
        case 'task_added': text = `<span class="highlight">[${msg.index}]</span> 离线任务已添加: <span class="file-name">${msg.file_name}</span>`; break;
        case 'task_status': text = `<span class="highlight">[${msg.index}]</span> 状态: ${msg.status}`; break;
        case 'task_error': text = `<span class="highlight">[${msg.index}]</span> <span class="error">${msg.message}</span>`; break;
        case 'files_found': text = `<span class="highlight">[${msg.index}]</span> 找到 ${msg.files.length} 个文件: <span class="file-name">${msg.files.join(', ')}</span>`; break;
        case 'aria2_done': text = `<span class="highlight">[${msg.index}]</span> <span class="success">已推送 ${msg.success_count}/${msg.total_count} 到 Aria2</span>`; break;
        case 'task_done': text = `<span class="highlight">[${msg.index}]</span> <span class="success">✓ ${msg.file_name} 完成</span>`; break;
        case 'all_done':
            text = `<span class="success">全部 ${msg.total} 个磁链处理完毕！</span>`;
            const btn = document.getElementById('submitBtn');
            if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ph ph-rocket-launch"></i> 一键推送'; }
            break;
        case 'error':
            text = `<span class="error">${msg.message}</span>`;
            const btnE = document.getElementById('submitBtn');
            if (btnE) { btnE.disabled = false; btnE.innerHTML = '<i class="ph ph-rocket-launch"></i> 一键推送'; }
            break;
    }
    if (text) addLogEntry(icon, text);
    
    if (!document.getElementById('page-progress').classList.contains('active') && msg.type === 'task_start') {
        switchPage('progress');
    }
}


function updateProgressBar(filename, mode, progress, speed, downloaded, total, eta, connections, status) {
    const container = document.getElementById('progressBarsContainer');
    const barsEl = document.getElementById('progressBars');
    const placeholder = document.getElementById('a2tdEmptyPlaceholder');
    if (!container || !barsEl) return;
    
    container.style.display = 'block';
    if (placeholder) placeholder.style.display = 'none';
    
    const cardId = 'pb-' + filename.replace(/[^a-zA-Z0-9_-]/g, '_');
    let card = document.getElementById(cardId);
    
    if (!card) {
        card = document.createElement('div');
        card.id = cardId;
        card.className = 'progress-card ' + (mode === 'download' ? 'downloading' : mode === 'upload' ? 'uploading' : 'completed');
        card.innerHTML = `
            <div class="progress-header">
                <div class="progress-filename">
                    <i class="ph ${mode === 'download' ? 'ph-download-simple dl-icon' : mode === 'upload' ? 'ph-upload-simple ul-icon' : 'ph-check-circle'}" data-icon></i>
                    <span data-name>${filename}</span>
                </div>
                <div class="progress-pct" data-pct>0%</div>
            </div>
            <div class="progress-bar-track">
                <div class="progress-bar-fill ${mode}" data-bar style="width:0%"></div>
            </div>
            <div class="progress-meta">
                <span data-size></span>
                <span class="speed" data-speed></span>
                <span data-extra></span>
            </div>
        `;
        barsEl.prepend(card);
    }

    // 更新卡片样式
    card.className = 'progress-card ' + (status === 'completed' ? 'completed' : mode === 'download' ? 'downloading' : mode === 'upload' ? 'uploading' : 'completed');
    
    // 更新图标
    const iconEl = card.querySelector('[data-icon]');
    if (iconEl) {
        if (mode === 'download') { iconEl.className = 'ph ph-download-simple dl-icon'; }
        else if (mode === 'upload') { iconEl.className = 'ph ph-upload-simple ul-icon'; }
        else { iconEl.className = 'ph-fill ph-check-circle'; iconEl.style.color = 'var(--success)'; }
    }

    // 更新百分比
    const pctEl = card.querySelector('[data-pct]');
    if (pctEl) pctEl.textContent = Math.round(progress) + '%';

    // 更新进度条
    const barEl = card.querySelector('[data-bar]');
    if (barEl) {
        barEl.style.width = progress + '%';
        barEl.className = 'progress-bar-fill ' + (mode === 'done' ? 'done' : mode);
    }

    // 更新元数据
    const sizeEl = card.querySelector('[data-size]');
    if (sizeEl && downloaded && total) sizeEl.textContent = `${downloaded} / ${total}`;

    const speedEl = card.querySelector('[data-speed]');
    if (speedEl) speedEl.textContent = speed || '';

    const extraEl = card.querySelector('[data-extra]');
    if (extraEl) {
        const parts = [];
        if (eta) parts.push('ETA: ' + eta);
        if (connections > 0) parts.push(connections + ' 连接');
        extraEl.textContent = parts.join(' · ');
    }

    // 自动切换到下载监控页
    const a2tdPage = document.getElementById('page-aria2teldrive');
    if (a2tdPage && !a2tdPage.classList.contains('active') && mode === 'download' && progress < 2) {
        switchPage('aria2teldrive');
    }
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

async function loadA2TDTasks() {
    try {
        const resp = await fetch('/api/a2td/tasks');
        const data = await resp.json();
        renderA2TDTasks(data.tasks);
    } catch (e) {}
}

function renderA2TDStats(stats) {
    if (!stats) return;
    if (stats.cpu) {
        document.getElementById('sysCpuStat').textContent = `${stats.cpu.percent.toFixed(1)}%`;
    }
    if (stats.disk) {
        const totalStr = formatBytes(stats.disk.total || 0, 0);
        const freeStr = formatBytes(stats.disk.free || 0, 0);
        document.getElementById('sysDiskStat').textContent = `${freeStr} 可用 / ${totalStr}`;
    }
    if (stats.download_speed !== undefined) {
        document.getElementById('sysDownloadStat').textContent = `${formatBytes(stats.download_speed)}/s`;
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

function getA2TDTaskProgress(task) {
    if (task.status === 'completed') return 100;
    if (task.status === 'uploading') return Number(task.upload_progress || 0);
    if (Number(task.upload_progress || 0) > 0 && Number(task.download_progress || 0) >= 100) {
        return Number(task.upload_progress || 0);
    }
    return Number(task.download_progress || 0);
}

function getA2TDTaskMode(task) {
    if (task.status === 'completed') return 'done';
    if (task.status === 'uploading') return 'upload';
    return 'download';
}

function getA2TDTaskActions(task) {
    const taskId = encodeURIComponent(task.task_id);
    if (task.status === 'downloading') {
        return `
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'pause')"><i class="ph ph-pause"></i> 暂停</button>
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'cancel')" style="color:var(--error);"><i class="ph ph-x"></i> 取消</button>
        `;
    }
    if (task.status === 'paused') {
        return `
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'resume')" style="color:var(--success);"><i class="ph ph-play"></i> 恢复</button>
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'cancel')" style="color:var(--error);"><i class="ph ph-x"></i> 取消</button>
        `;
    }
    if (task.status === 'uploading') {
        return `
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'retry')" style="color:var(--warning);"><i class="ph ph-arrow-clockwise"></i> 重传</button>
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'cancel')" style="color:var(--error);"><i class="ph ph-x"></i> 取消</button>
        `;
    }
    if (task.status === 'failed') {
        return `
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'retry')" style="color:var(--warning);"><i class="ph ph-arrow-clockwise"></i> 重试</button>
            <button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'delete')" style="color:var(--error);"><i class="ph ph-trash"></i> 删除</button>
        `;
    }
    if (task.status === 'pending') {
        return `<button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'cancel')" style="color:var(--error);"><i class="ph ph-x"></i> 取消</button>`;
    }
    return `<button class="btn btn-ghost btn-sm" onclick="a2tdAction('${taskId}', 'delete')"><i class="ph ph-trash"></i> 删除记录</button>`;
}

function renderA2TDTasks(tasks) {
    const container = document.getElementById('progressBarsContainer');
    const barsEl = document.getElementById('progressBars');
    const placeholder = document.getElementById('a2tdEmptyPlaceholder');
    if (!container || !barsEl || !placeholder) return;

    if (!Array.isArray(tasks) || tasks.length === 0) {
        barsEl.innerHTML = '';
        container.style.display = 'none';
        placeholder.style.display = 'block';
        return;
    }

    container.style.display = 'block';
    placeholder.style.display = 'none';
    barsEl.innerHTML = '';

    tasks.forEach(task => {
        const mode = getA2TDTaskMode(task);
        const progress = Math.max(0, Math.min(100, getA2TDTaskProgress(task)));
        const filename = escapeA2TDHtml(task.filename || task.task_id || '未命名任务');
        const statusLabel = escapeA2TDHtml(getA2TDTaskStatusLabel(task.status));
        const downloadProgress = Number(task.download_progress || 0).toFixed(1);
        const uploadProgress = Number(task.upload_progress || 0).toFixed(1);
        const speedText = task.status === 'downloading' ? (task.download_speed || '') : (task.upload_speed || '');
        const sizeText = task.file_size || '';
        const extraParts = [`状态：${statusLabel}`];
        if (task.status === 'uploading') extraParts.push(`上传 ${uploadProgress}%`);
        else if (task.status !== 'completed') extraParts.push(`下载 ${downloadProgress}%`);
        if (task.updated_at) extraParts.push(`更新于 ${escapeA2TDHtml(task.updated_at)}`);

        const card = document.createElement('div');
        card.id = 'pb-' + String(task.filename || task.task_id || 'unknown').replace(/[^a-zA-Z0-9_-]/g, '_');
        card.className = 'progress-card ' + (task.status === 'completed' ? 'completed' : mode === 'upload' ? 'uploading' : 'downloading');
        card.innerHTML = `
            <div class="progress-header">
                <div class="progress-filename">
                    <i class="ph ${mode === 'upload' ? 'ph-upload-simple ul-icon' : task.status === 'completed' ? 'ph-check-circle' : 'ph-download-simple dl-icon'}" data-icon></i>
                    <span data-name>${filename}</span>
                </div>
                <div class="progress-pct" data-pct>${Math.round(progress)}%</div>
            </div>
            <div class="progress-bar-track">
                <div class="progress-bar-fill ${mode === 'done' ? 'done' : mode}" data-bar style="width:${progress}%"></div>
            </div>
            <div class="progress-meta">
                <span data-size>${escapeA2TDHtml(sizeText)}</span>
                <span class="speed" data-speed>${escapeA2TDHtml(speedText)}</span>
                <span data-extra>${extraParts.join(' · ')}</span>
            </div>
            ${task.error ? `<div style="margin-top:10px; color:var(--error); font-size:12px; line-height:1.5;">${escapeA2TDHtml(task.error)}</div>` : ''}
            <div class="page-actions" style="display:flex; flex-wrap:wrap; justify-content:flex-end; gap:8px; margin-top:12px;">
                ${getA2TDTaskActions(task)}
            </div>
        `;
        barsEl.appendChild(card);
    });
}

async function a2tdAction(taskId, action) {
    try {
        const url = action === 'delete' ? `/api/a2td/task/${taskId}` : `/api/a2td/task/${taskId}/${action}`;
        const method = action === 'delete' ? 'DELETE' : 'POST';
        const resp = await fetch(url, { method });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.detail || '操作失败');
        }
        loadA2TDTasks();
    } catch(e) {
        alert(e.message || '任务操作失败');
    }
}


async function a2tdBulkAction(action) {
    try {
        await fetch(`/api/a2td/tasks/${action}`, { method: 'POST' });
        loadA2TDTasks();
    } catch(e) {}
}

async function clearCompletedTasks() {
    try {
        await fetch('/api/a2td/tasks/clear-completed', { method: 'POST' });
        loadA2TDTasks();
    } catch(e) {}
}

// ── Settings ──
async function loadConfig() {
    try {
        const resp = await fetch('/api/settings');
        const cfg = await resp.json();
        
        document.getElementById('cfgAuthUser').value = cfg.auth?.username || '';
        document.getElementById('cfgAuthPass').value = cfg.auth?.password || '';
        
        document.getElementById('cfgServerPort').value = cfg.server?.port || 8888;

        document.getElementById('cfgPikpakUsername').value = cfg.pikpak?.username || '';
        document.getElementById('cfgPikpakPassword').value = cfg.pikpak?.password || '';
        document.getElementById('cfgPikpakSaveDir').value = cfg.pikpak?.save_dir || '/';
        document.getElementById('cfgPikpakDelete').checked = cfg.pikpak?.delete_after_download || false;
        document.getElementById('cfgPikpakEngine').value = cfg.pikpak?.download_engine || 'builtin';
        document.getElementById('cfgPikpakMaxDownloads').value = cfg.pikpak?.max_concurrent_downloads || 3;
        document.getElementById('cfgPikpakConnections').value = cfg.pikpak?.connections_per_task || 8;
        
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
    
    // Parse form to nested map
    const cfg = {
        auth: {
            username: document.getElementById('cfgAuthUser').value.trim(),
            password: document.getElementById('cfgAuthPass').value.trim()
        },
        server: { port: parseInt(document.getElementById('cfgServerPort').value) || 8888 },
        pikpak: { 
            username: document.getElementById('cfgPikpakUsername').value, 
            password: document.getElementById('cfgPikpakPassword').value, 
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
            max_retries: 3,
            check_interval: 3,
            max_disk_usage_gb: 0,
            cpu_usage_limit: 85
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

    const inputs = settingsPage.querySelectorAll('input.form-input, input[type="checkbox"]');
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
    // 复用 saveConfig 的数据收集逻辑
    const cfg = {
        auth: {
            username: document.getElementById('cfgAuthUser').value.trim(),
            password: document.getElementById('cfgAuthPass').value.trim()
        },
        server: { port: parseInt(document.getElementById('cfgServerPort').value) || 8888 },
        pikpak: {
            username: document.getElementById('cfgPikpakUsername').value,
            password: document.getElementById('cfgPikpakPassword').value,
            save_dir: document.getElementById('cfgPikpakSaveDir').value || '/',
            delete_after_download: document.getElementById('cfgPikpakDelete').checked
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
            max_retries: 3, check_interval: 3, max_disk_usage_gb: 0, cpu_usage_limit: 85
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
        
        switchPage('progress');
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
        
        switchPage('progress');
    } catch(e) {
        alert(e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-download-simple"></i> 同步到 Aria2';
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
        
        switchPage('progress');
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
        
        switchPage('progress');
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
