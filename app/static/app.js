

// ── Setup Wizard ──
const WIZARD_TOTAL_STEPS = 6;
const WIZARD_ALL_STEPS = [1, 2, 3, 4, 5, 6];
let wizardCurrentStep = 1;
let wizardActiveSteps = [...WIZARD_ALL_STEPS];
let wizardAria2PollTimer = null;
let latestAria2Runtime = null;

function clearWizardAria2Poll() {
    if (wizardAria2PollTimer) {
        clearTimeout(wizardAria2PollTimer);
        wizardAria2PollTimer = null;
    }
}

function scheduleWizardAria2Poll(delay = 1200) {
    clearWizardAria2Poll();
    wizardAria2PollTimer = setTimeout(() => refreshAria2RuntimeStatus(), delay);
}

async function readJsonSafe(resp) {
    return await resp.json().catch(() => ({}));
}

function showWizardError(btn, message) {
    if (!btn || !message) return;
    if (window._wizardErrTimeout) clearTimeout(window._wizardErrTimeout);
    const footer = btn.parentElement;
    if (!footer) return;
    footer.style.position = 'relative';
    const oldToast = footer.querySelector('.wizard-err-toast');
    if (oldToast) oldToast.remove();

    const info = document.createElement('div');
    info.className = 'wizard-err-toast';
    info.innerHTML = `<i class="ph-fill ph-warning-circle"></i> ${message}`;
    info.style = 'position:absolute; bottom:-45px; right:0; background:var(--error); color:white; padding:8px 16px; border-radius:8px; font-size:13px; display:flex; align-items:center; gap:8px; box-shadow:0 4px 12px rgba(239,68,68,0.3); z-index:100; opacity:0; transform:translateY(-10px); transition:all 0.3s;';
    footer.appendChild(info);
    setTimeout(() => {
        info.style.opacity = '1';
        info.style.transform = 'translateY(0)';
    }, 10);
    window._wizardErrTimeout = setTimeout(() => {
        info.style.opacity = '0';
        setTimeout(() => info.remove(), 300);
    }, 3500);
}

function getWizardStepLabel(step) {
    const labels = {
        1: 'PikPak',
        2: 'aria2 安装',
        3: 'aria2 参数',
        4: 'TelDrive',
        5: 'Telegram 中转',
        6: 'Postgres'
    };
    return labels[Number(step)] || `步骤 ${step}`;
}

function getWizardActiveSteps() {
    return Array.isArray(wizardActiveSteps) && wizardActiveSteps.length
        ? wizardActiveSteps
        : [...WIZARD_ALL_STEPS];
}

function setWizardActiveSteps(steps = []) {
    const normalized = [...new Set((steps || [])
        .map(step => Number(step))
        .filter(step => WIZARD_ALL_STEPS.includes(step)))];
    wizardActiveSteps = normalized.length ? normalized : [...WIZARD_ALL_STEPS];

    for (let i = 1; i <= WIZARD_TOTAL_STEPS; i++) {
        const enabled = wizardActiveSteps.includes(i);
        const stepEl = document.getElementById('wStep' + i);
        const dotEl = document.getElementById('dot' + i);
        if (stepEl) {
            stepEl.style.display = enabled ? '' : 'none';
            if (!enabled) stepEl.classList.remove('active');
        }
        if (dotEl) {
            dotEl.style.display = enabled ? '' : 'none';
            if (!enabled) dotEl.classList.remove('active');
        }
    }

    const subtitle = document.getElementById('setupWizardSubtitle');
    if (subtitle) {
        subtitle.textContent = wizardActiveSteps.length >= WIZARD_TOTAL_STEPS
            ? '首次运行需要先安装并配置 aria2，完成后才能进入主界面'
            : `检测到以下配置仍需补全：${wizardActiveSteps.map(getWizardStepLabel).join('、')}`;
    }
}

function getPrevWizardStep(step) {
    const active = getWizardActiveSteps();
    const index = active.indexOf(Number(step));
    if (index <= 0) return Number(step) || active[0] || 1;
    return active[index - 1];
}

function getNextWizardStep(step) {
    const active = getWizardActiveSteps();
    const index = active.indexOf(Number(step));
    if (index === -1) return active[0] || null;
    return active[index + 1] || null;
}

function getWizardPendingSteps(data = {}, healthDetails = {}) {
    const pikpakMode = normalizePikpakLoginMode(data.pikpak?.login_mode || 'password');
    const pikpakReady = typeof healthDetails?.pikpak === 'boolean'
        ? healthDetails.pikpak
        : (pikpakMode === 'token'
            ? !!data.pikpak?.session
            : !!(data.pikpak?.username && data.pikpak?.password));

    const aria2Ready = typeof healthDetails?.aria2 === 'boolean'
        ? healthDetails.aria2
        : !!(latestAria2Runtime?.installed || data.aria2?.installed);

    const teldriveReady = typeof healthDetails?.teldrive === 'boolean'
        ? healthDetails.teldrive
        : !!(data.teldrive?.api_host && data.teldrive?.access_token && Number(data.teldrive?.channel_id || 0));

    const telegramReady = typeof healthDetails?.telegram === 'boolean'
        ? healthDetails.telegram
        : !!(data.telegram?.api_id && data.telegram?.api_hash && Number(data.telegram?.channel_id || 0));

    const databaseReady = typeof healthDetails?.database === 'boolean'
        ? healthDetails.database
        : !!String(data.telegram_db?.host || '').trim();

    const pending = [];
    if (!pikpakReady) pending.push(1);
    if (!aria2Ready) pending.push(2, 3);
    if (!teldriveReady) pending.push(4);
    if (!telegramReady) pending.push(5);
    if (!databaseReady) pending.push(6);
    return pending;
}

function setWizardStep(step) {
    const active = getWizardActiveSteps();
    if (!active.length) return;

    const requested = Number(step) || active[0] || 1;
    wizardCurrentStep = active.includes(requested)
        ? requested
        : (active.find(item => item >= requested) || active[active.length - 1] || active[0]);

    for (let i = 1; i <= WIZARD_TOTAL_STEPS; i++) {
        document.getElementById('wStep' + i)?.classList.remove('active');
        document.getElementById('dot' + i)?.classList.remove('active');
    }
    document.getElementById('wStep' + wizardCurrentStep)?.classList.add('active');
    document.getElementById('dot' + wizardCurrentStep)?.classList.add('active');
    if (wizardCurrentStep === 2) refreshAria2RuntimeStatus();
}

function ensureCurrentConfig() {

    if (!window.currentConfig || typeof window.currentConfig !== 'object') {
        window.currentConfig = {};
    }
    return window.currentConfig;
}

function mergeCurrentConfig(patch = {}) {
    const target = ensureCurrentConfig();
    Object.entries(patch || {}).forEach(([section, value]) => {
        if (value && typeof value === 'object' && !Array.isArray(value)) {
            target[section] = { ...(target[section] || {}), ...value };
        } else {
            target[section] = value;
        }
    });
    return target;
}

async function syncCurrentConfigFromServer() {
    const resp = await fetch('/api/settings');
    if (!resp.ok) throw new Error('读取配置失败');
    const data = await readJsonSafe(resp);
    window.currentConfig = data;
    fillWizardInputs(data);
    return data;
}

async function persistCurrentConfig(patch = null) {
    if (patch) mergeCurrentConfig(patch);
    const resp = await fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(ensureCurrentConfig())
    });
    const data = await readJsonSafe(resp);
    if (!resp.ok || data.success === false) {
        throw new Error(data.detail || data.message || data.error || '保存配置失败');
    }
    return data;
}

function normalizePikpakLoginMode(mode = 'password') {
    const normalized = String(mode || 'password').trim().toLowerCase();
    return normalized === 'session' || normalized === 'token' ? 'token' : 'password';
}

function syncPikpakLoginModeButtons(groupId, mode) {
    const normalized = normalizePikpakLoginMode(mode);
    const group = document.getElementById(groupId);
    if (!group) return;
    group.querySelectorAll('[data-mode]').forEach(btn => {
        const active = normalizePikpakLoginMode(btn.dataset.mode) === normalized;
        btn.classList.toggle('active', active);
        btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
}

function updatePikpakLoginMode(inputId, groupId, mode, autoSave = false) {
    const normalized = normalizePikpakLoginMode(mode);
    const input = document.getElementById(inputId);
    if (input) input.value = normalized;
    syncPikpakLoginModeButtons(groupId, normalized);

    if (inputId === 'wPikLoginMode') {
        toggleWizardPikpakLoginMode();
        return;
    }

    togglePikpakLoginMode();
    if (autoSave && input) {
        if (_autoSaveTimer) clearTimeout(_autoSaveTimer);
        _autoSaveTimer = setTimeout(() => doAutoSave(input), 200);
    }
}

function toggleWizardPikpakLoginMode() {
    const mode = normalizePikpakLoginMode(document.getElementById('wPikLoginMode')?.value || 'password');
    const passwordFields = document.getElementById('wPikPasswordFields');
    const tokenFields = document.getElementById('wPikTokenFields');
    syncPikpakLoginModeButtons('wPikLoginModeSwitch', mode);
    if (passwordFields) passwordFields.style.display = mode === 'token' ? 'none' : 'grid';
    if (tokenFields) tokenFields.style.display = mode === 'token' ? 'grid' : 'none';
}


function fillWizardInputs(data = {}) {
    try {
        const pikpakMode = normalizePikpakLoginMode(data.pikpak?.login_mode || 'password');
        document.getElementById('wPikLoginMode').value = pikpakMode;
        document.getElementById('wPikUser').value = data.pikpak?.username || '';
        document.getElementById('wPikPass').value = data.pikpak?.password || '';
        document.getElementById('wPikToken').value = data.pikpak?.session || '';
        toggleWizardPikpakLoginMode();
        document.getElementById('wTdUrl').value = data.teldrive?.api_host || '';
        document.getElementById('wTdToken').value = data.teldrive?.access_token || '';
        document.getElementById('wTdChannel').value = data.teldrive?.channel_id || '';
        document.getElementById('wTgId').value = data.telegram?.api_id || '';
        document.getElementById('wTgHash').value = data.telegram?.api_hash || '';
        document.getElementById('wTgChannel').value = data.telegram?.channel_id || '';
        document.getElementById('wDbHost').value = data.telegram_db?.host || '';

        document.getElementById('wDbPort').value = data.telegram_db?.port || 5432;
        document.getElementById('wDbName').value = data.telegram_db?.name || 'postgres';
        document.getElementById('wDbUser').value = data.telegram_db?.user || 'postgres';
        updateWizardAria2Os(data.aria2?.os_type || document.getElementById('wAria2Os')?.value || '');
        document.getElementById('wAria2MaxConcurrent').value = data.aria2?.max_concurrent || 3;
        document.getElementById('wAria2Split').value = data.aria2?.split || 8;
        document.getElementById('wAria2MaxConnPerServer').value = data.aria2?.max_connection_per_server || 8;
        document.getElementById('wAria2MinSplitSize').value = data.aria2?.min_split_size_mb || 5;
        document.getElementById('wAria2Secret').value = data.aria2?.rpc_secret || '';
        document.getElementById('wAria2AllowRemoteAccess').checked = !!data.aria2?.allow_remote_access;
    } catch (e) {
        console.warn('填充向导配置失败', e);
    }
}


function validateAria2AccessConfig(aria2 = {}) {
    if (aria2.allow_remote_access && !String(aria2.rpc_secret || '').trim()) {
        throw new Error('开启外部访问时必须设置 aria2 RPC 密码');
    }
}


function getWizardAria2Config() {
    const existing = window.currentConfig?.aria2 || {};
    const wizardPortInput = document.getElementById('wAria2Port');
    const settingsPortInput = document.getElementById('cfgAria2Port');
    const secretInput = document.getElementById('wAria2Secret');
    const aria2 = {
        rpc_port: Math.max(1, parseInt(wizardPortInput?.value || settingsPortInput?.value || existing.rpc_port || '6822', 10) || 6822),
        rpc_secret: (secretInput?.value ?? existing.rpc_secret ?? '').trim(),
        allow_remote_access: !!document.getElementById('wAria2AllowRemoteAccess')?.checked,
        max_concurrent: Math.max(1, parseInt(document.getElementById('wAria2MaxConcurrent').value, 10) || existing.max_concurrent || 3),
        split: Math.max(1, parseInt(document.getElementById('wAria2Split').value, 10) || existing.split || 8),
        max_connection_per_server: Math.max(1, parseInt(document.getElementById('wAria2MaxConnPerServer').value, 10) || existing.max_connection_per_server || 8),
        min_split_size_mb: Math.max(1, parseInt(document.getElementById('wAria2MinSplitSize').value, 10) || existing.min_split_size_mb || 5),
    };
    validateAria2AccessConfig(aria2);
    return aria2;
}



function setStatusBadge(el, status, text) {
    if (!el) return;
    el.className = `wizard-status-badge ${status}`;
    el.innerHTML = text;
}

function isAria2InstallBusyStatus(status = '') {
    return ['downloading', 'extracting', 'starting'].includes(status);
}

function applyWizardAria2ActionLock(runtime = latestAria2Runtime || {}) {
    const locked = isAria2InstallBusyStatus(runtime.status) || !!runtime.installed;
    const autoBtn = document.getElementById('wAria2AutoBtn');
    const uploadBtn = document.getElementById('wAria2UploadBtn');
    if (autoBtn) autoBtn.disabled = locked;
    if (uploadBtn) uploadBtn.disabled = locked;
}

function renderAria2RuntimeViews(runtime = {}) {
    latestAria2Runtime = runtime;
    const progress = Math.max(0, Math.min(100, Number(runtime.progress) || 0));
    const isBusy = isAria2InstallBusyStatus(runtime.status);
    const isInstalled = !!runtime.installed;
    const isRunning = !!runtime.running;

    const badgeState = runtime.status === 'failed'
        ? 'error'
        : isBusy
            ? 'warning'
            : isRunning
                ? 'success'
                : isInstalled
                    ? 'info'
                    : 'info';
    const badgeText = runtime.status === 'failed'
        ? '<i class="ph ph-warning-circle"></i> 安装失败'
        : isBusy
            ? '<i class="ph ph-spinner-gap"></i> 安装中'
            : isRunning
                ? '<i class="ph ph-check-circle"></i> 已运行'
                : isInstalled
                    ? '<i class="ph ph-check-circle"></i> 已安装'
                    : '<i class="ph ph-circle-dashed"></i> 未安装';

    const binaryPath = runtime.binary_path || '--';
    const downloadDir = runtime.download_dir || '--';
    const statusText = runtime.error || runtime.message || '尚未安装';
    const percentText = `${progress.toFixed(0)}%`;

    document.getElementById('wAria2InstallProgress')?.style.setProperty('width', `${progress}%`);
    const wText = document.getElementById('wAria2InstallText');
    if (wText) wText.textContent = statusText;
    const wPercent = document.getElementById('wAria2InstallPercent');
    if (wPercent) wPercent.textContent = percentText;
    const wHint = document.getElementById('wAria2InstallHint');
    if (wHint) {
        wHint.textContent = isInstalled
            ? `aria2 已部署到本地并固定使用 ${downloadDir}`
            : '请选择系统后开始安装。安装未完成前无法进入下一步。';
    }
    setStatusBadge(document.getElementById('wAria2InstallBadge'), badgeState, badgeText);
    const wBinary = document.getElementById('wAria2BinaryPath');
    if (wBinary) wBinary.textContent = binaryPath;
    const wDir = document.getElementById('wAria2DownloadDir');
    if (wDir) wDir.textContent = downloadDir;
    const wNext = document.getElementById('wAria2NextBtn');
    if (wNext) wNext.disabled = !isInstalled || isBusy;

    applyWizardAria2ActionLock(runtime);

    const cfgHint = document.getElementById('cfgAria2RuntimeHint');

    if (cfgHint) cfgHint.textContent = statusText;
    setStatusBadge(document.getElementById('cfgAria2RuntimeBadge'), badgeState, badgeText);
    const cfgBinary = document.getElementById('cfgAria2BinaryPath');
    if (cfgBinary) cfgBinary.textContent = binaryPath;
    const cfgDir = document.getElementById('cfgAria2DownloadDirText');
    if (cfgDir) cfgDir.textContent = downloadDir;
}

async function refreshAria2RuntimeStatus(showToast = false) {
    try {
        const resp = await fetch('/api/settings/aria2/runtime');
        const data = await readJsonSafe(resp);
        if (!resp.ok) throw new Error(data.detail || data.message || '读取 aria2 状态失败');
        renderAria2RuntimeViews(data);
        const osInput = document.getElementById('wAria2Os');
        if (osInput && !osInput.value && data.host_os) updateWizardAria2Os(data.host_os);
        if (data.installed && (!window.currentConfig?.aria2?.binary_path || window.currentConfig?.aria2?.installed !== true)) {

            await syncCurrentConfigFromServer();
        }
        if (['downloading', 'extracting', 'starting'].includes(data.status)) {
            scheduleWizardAria2Poll();
        } else {
            clearWizardAria2Poll();
        }
        if (showToast && typeof showA2TDToast === 'function') {
            showA2TDToast(data.message || 'aria2 状态已刷新', 'info');
        }
        return data;
    } catch (e) {
        clearWizardAria2Poll();
        renderAria2RuntimeViews({
            status: 'failed',
            progress: 0,
            message: '读取 aria2 状态失败',
            error: e.message,
            installed: false,
            running: false,
            binary_path: '',
            download_dir: ''
        });
        if (showToast && typeof showA2TDToast === 'function') {
            showA2TDToast(e.message || '读取 aria2 状态失败', 'error');
        }
        throw e;
    }
}

function normalizeWizardAria2Os(osType = '') {
    const normalized = String(osType || '').trim().toLowerCase();
    return ['win', 'linux'].includes(normalized) ? normalized : '';
}

function syncWizardAria2OsButtons(osType) {
    const normalized = normalizeWizardAria2Os(osType);
    const group = document.getElementById('wAria2OsSwitch');
    if (!group) return;
    group.querySelectorAll('[data-os]').forEach(btn => {
        const active = btn.dataset.os === normalized;
        btn.classList.toggle('active', active);
        btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
}

function updateWizardAria2Os(osType) {
    const normalized = normalizeWizardAria2Os(osType);
    const input = document.getElementById('wAria2Os');
    if (input) input.value = normalized;
    syncWizardAria2OsButtons(normalized);
}

function getWizardSelectedOs() {
    return normalizeWizardAria2Os(document.getElementById('wAria2Os')?.value || '');
}


async function startWizardAria2AutoInstall() {
    const osType = getWizardSelectedOs();
    if (!osType) {
        alert('请先选择当前运行的操作系统');
        return;
    }
    const btn = document.getElementById('wAria2AutoBtn');
    const oldHtml = btn?.innerHTML || '';
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner"></span> 提交中...';
    }
    try {
        const resp = await fetch('/api/settings/aria2/install/auto', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ os_type: osType })
        });
        const data = await readJsonSafe(resp);
        if (!resp.ok || data.success === false) throw new Error(data.detail || data.message || data.error || '提交自动安装失败');
        await refreshAria2RuntimeStatus();
    } catch (e) {
        alert(e.message || '自动安装失败');
    } finally {
        if (btn) {
            btn.innerHTML = oldHtml;
        }
        applyWizardAria2ActionLock();
    }
}


function triggerWizardAria2Upload() {
    const osType = getWizardSelectedOs();
    if (!osType) {
        alert('请先选择当前运行的操作系统');
        return;
    }
    document.getElementById('wAria2Archive')?.click();
}

async function handleWizardAria2Upload(event) {
    const fileInput = event?.target;
    const file = fileInput?.files?.[0];
    if (!file) return;
    const osType = getWizardSelectedOs();
    if (!osType) {
        alert('请先选择当前运行的操作系统');
        fileInput.value = '';
        return;
    }
    const btn = document.getElementById('wAria2UploadBtn');
    const oldHtml = btn?.innerHTML || '';
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner"></span> 上传中...';
    }
    try {
        const formData = new FormData();
        formData.append('os_type', osType);
        formData.append('archive', file);
        const resp = await fetch('/api/settings/aria2/install/upload', {
            method: 'POST',
            body: formData
        });
        const data = await readJsonSafe(resp);
        if (!resp.ok || data.success === false) throw new Error(data.detail || data.message || data.error || '上传安装包失败');
        await refreshAria2RuntimeStatus();
    } catch (e) {
        alert(e.message || '上传安装失败');
    } finally {
        if (btn) {
            btn.innerHTML = oldHtml;
        }
        applyWizardAria2ActionLock();
        if (fileInput) fileInput.value = '';
    }
}


async function checkSetupRequired() {
    try {
        const data = await syncCurrentConfigFromServer();
        let healthData = { healthy: false, details: {} };
        try {
            const hResp = await fetch('/api/settings/health');
            if (hResp.ok) healthData = await readJsonSafe(hResp);
        } catch (e) {}

        window.healthDetails = healthData.details || {};
        const needsSetup = !!(data._meta && data._meta.needs_setup);
        await refreshAria2RuntimeStatus();

        const wiz = document.getElementById('setupWizard');
        if (!wiz) return;
        if (!needsSetup) {
            wiz.classList.remove('active', 'show');
            clearWizardAria2Poll();
            return;
        }

        const pendingSteps = getWizardPendingSteps(data, window.healthDetails);
        if (!pendingSteps.length) {
            wiz.classList.remove('active', 'show');
            clearWizardAria2Poll();
            return;
        }

        setWizardActiveSteps(pendingSteps);
        wiz.classList.add('show', 'active');
        setWizardStep(pendingSteps[0]);
    } catch (e) {
        console.error('Failed to check setup', e);
    }
}


function closeSetupWizard() {
    const wiz = document.getElementById('setupWizard');
    if (wiz) wiz.classList.remove('active', 'show');
    clearWizardAria2Poll();
    location.reload();
}

async function wizardNext(current, next, btn = null) {
    if (next < current) {
        setWizardStep(getPrevWizardStep(current));
        return;
    }

    btn = btn || event?.currentTarget;
    if (!btn) return;

    const oldHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;margin-right:8px;"></span> 验证中...';

    let dataToSave = {};
    try {
        if (current === 1) {
            const loginMode = normalizePikpakLoginMode(document.getElementById('wPikLoginMode')?.value || 'password');
            let payload = null;
            if (loginMode === 'token') {
                const token = document.getElementById('wPikToken').value.trim();
                if (!token) throw new Error('请填写 PikPak Encoded Token');
                payload = { login_mode: 'token', username: '', password: '', session: token };
            } else {
                const user = document.getElementById('wPikUser').value.trim();
                const pass = document.getElementById('wPikPass').value.trim();
                if (!user || !pass) throw new Error('您必须填写 PikPak 账密');
                payload = { login_mode: 'password', username: user, password: pass, session: '' };
            }
            const r = await fetch('/api/settings/test/pikpak', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const d = await readJsonSafe(r);
            if (!d.success) throw new Error(d.message || 'PikPak 验证失败');
            dataToSave.pikpak = payload;

        } else if (current === 2) {
            const runtime = await refreshAria2RuntimeStatus();
            if (!runtime.installed) throw new Error('aria2 尚未安装完成，请先完成自动安装或上传安装包');
            await syncCurrentConfigFromServer();
        } else if (current === 3) {
            dataToSave.aria2 = getWizardAria2Config();
        } else if (current === 4) {
            const tUrl = document.getElementById('wTdUrl').value.trim();
            const tTok = document.getElementById('wTdToken').value.trim();
            const tChannel = parseInt(document.getElementById('wTdChannel').value, 10);
            if (!tUrl || !tTok) throw new Error('TelDrive API 和 Token 为必填');
            if (!tChannel) throw new Error('请填写 TelDrive 同步频道 ID');
            const tdPayload = { api_host: tUrl, access_token: tTok, channel_id: tChannel };
            const r = await fetch('/api/settings/test/teldrive', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(tdPayload)
            });
            const d = await readJsonSafe(r);
            if (!d.success && !d.ok) throw new Error(d.message || 'TelDrive 连接失败');
            dataToSave.teldrive = tdPayload;
        } else if (current === 5) {
            const tid = document.getElementById('wTgId').value.trim();
            const tHash = document.getElementById('wTgHash').value.trim();
            const tChannel = parseInt(document.getElementById('wTgChannel').value, 10);
            if (!tid || !tHash) throw new Error('必须提供 Telegram 授权参数');
            if (!tChannel) throw new Error('请填写 Telegram 监听频道 ID');
            const tgPayload = { api_id: parseInt(tid, 10), api_hash: tHash, channel_id: tChannel };
            const r = await fetch('/api/settings/test/telegram', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(tgPayload)
            });
            const d = await readJsonSafe(r);
            if (!d.success) throw new Error(d.message || 'Telegram 验证失败');
            dataToSave.telegram = tgPayload;
        }

        if (Object.keys(dataToSave).length > 0) {
            await persistCurrentConfig(dataToSave);
        }

        const nextStep = getNextWizardStep(current);
        if (nextStep == null) {
            closeSetupWizard();
            return;
        }

        btn.disabled = false;
        btn.innerHTML = oldHtml;
        setWizardStep(nextStep);
    } catch (e) {
        btn.disabled = false;
        btn.innerHTML = oldHtml;
        showWizardError(btn, e.message || '操作失败');
    }
}

async function wizardFinish(btn = null) {
    btn = btn || event?.currentTarget;
    if (!btn) return;

    const oldHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;margin-right:8px;"></span> 验证中...';

    try {
        const dbHost = document.getElementById('wDbHost').value.trim();
        if (!dbHost) throw new Error('请输入数据库地址');

        const dbPayload = {
            host: dbHost,
            port: parseInt(document.getElementById('wDbPort').value, 10) || 5432,
            name: document.getElementById('wDbName').value.trim() || 'postgres',
            user: document.getElementById('wDbUser').value.trim() || 'postgres',
            password: document.getElementById('wDbPass').value.trim()
        };
        const r = await fetch('/api/settings/test/database', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(dbPayload)
        });
        const d = await readJsonSafe(r);
        if (!d.success) throw new Error(d.message || '数据库连接失败');

        btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;margin-right:8px;"></span> 部署中...';
        await persistCurrentConfig({ telegram_db: dbPayload, aria2: getWizardAria2Config() });
        closeSetupWizard();
    } catch (e) {
        btn.disabled = false;
        btn.innerHTML = oldHtml;
        showWizardError(btn, e.message || '保存最终配置失败');
    }
}




// ── 全局 401 拦截 ──
const _origFetch = window.fetch;

function hasPendingAuthBootWait() {
    try {
        return sessionStorage.getItem('tdm-auth-boot-wait') === '1';
    } catch (e) {
        return false;
    }
}

function clearPendingAuthBootWait() {
    try {
        sessionStorage.removeItem('tdm-auth-boot-wait');
    } catch (e) {}
}

async function waitForFrontendAuthReady(maxAttempts = 18) {
    if (!hasPendingAuthBootWait()) return true;

    for (let i = 0; i < maxAttempts; i += 1) {
        try {
            const resp = await _origFetch('/api/auth/check', {
                method: 'GET',
                credentials: 'same-origin',
                cache: 'no-store',
                headers: { 'Cache-Control': 'no-cache' }
            });
            const data = await readJsonSafe(resp);
            if (resp.ok && data.authenticated) {
                clearPendingAuthBootWait();
                return true;
            }
        } catch (e) {}
        await new Promise(resolve => setTimeout(resolve, 120 + i * 40));
    }

    clearPendingAuthBootWait();
    return false;
}

window.fetch = async function (...args) {
    const resp = await _origFetch.apply(this, args);
    if (resp.status === 401 && !hasPendingAuthBootWait()) { window.location.href = '/login'; }
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
    if (name === 'aria2teldrive') loadA2TDTasks();
    if (name === 'tel2teldrive') loadT2TDState();
    if (name === 'settings') loadConfig();
    if (name === 'progress') loadParseWorkspaceSnapshot();
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
        loadParseWorkspaceSnapshot(true);
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
    if (msg.type === 'parse_job_state') {
        applyParseJobState(msg.job || null);
        return;
    }

    // ── 兼容旧版进度事件 ──

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

    appendProgressLogMessage(msg);

    if (!document.getElementById('page-progress').classList.contains('active') && msg.type === 'task_start' && msg.auto_open_progress === true) {
        switchPage('progress', { animated: true });
    }

}





const a2tdTaskStore = new Map();
const a2tdRemovedTaskIds = new Set();
const a2tdPendingTaskActions = new Map();
let a2tdRenderScheduled = false;
let a2tdSnapshotPending = false;
let a2tdTaskFilter = 'all';
let a2tdTaskSortSeed = 0;

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

function ensureA2TDTaskSortSerial(value = 0) {
    const serial = getA2TDNumber(value);
    if (serial > a2tdTaskSortSeed) a2tdTaskSortSeed = serial;
    if (serial > 0) return serial;
    a2tdTaskSortSeed += 1;
    return a2tdTaskSortSeed;
}

function getA2TDTaskSortBucket(status) {
    switch (String(status || '')) {
        case 'downloading':
            return 0;
        case 'uploading':
            return 1;
        case 'pending':
            return 2;
        case 'paused':
            return 3;
        case 'failed':
            return 4;
        case 'completed':
            return 5;
        default:
            return 4;
    }
}

function getA2TDTaskSortAnchor(task = {}, fallbackTs = 0) {
    return getA2TDNumber(task.sort_anchor_at)
        || parseA2TDTimestamp(task.created_at)
        || parseA2TDTimestamp(task.updated_at)
        || fallbackTs
        || Date.now();
}

function getA2TDTaskList() {
    return Array.from(a2tdTaskStore.values()).sort((a, b) => {
        const bucketDiff = getA2TDTaskSortBucket(a.status) - getA2TDTaskSortBucket(b.status);
        if (bucketDiff !== 0) return bucketDiff;

        const anchorA = getA2TDTaskSortAnchor(a);
        const anchorB = getA2TDTaskSortAnchor(b);
        if (anchorA !== anchorB) return anchorB - anchorA;

        const serialA = getA2TDNumber(a.sort_serial);
        const serialB = getA2TDNumber(b.sort_serial);
        if (serialA !== serialB) return serialA - serialB;

        return String(a.task_id || '').localeCompare(String(b.task_id || ''));
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
                sort_anchor_at: getA2TDTaskSortAnchor(existing, getA2TDTaskSortAnchor(task, eventTs)),
                sort_serial: ensureA2TDTaskSortSerial(existing.sort_serial),
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
        sort_anchor_at: getA2TDTaskSortAnchor(existing, getA2TDTaskSortAnchor(task, nowTs)),
        sort_serial: ensureA2TDTaskSortSerial(existing.sort_serial),
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

const progressLogSeenIds = new Set();
let progressSnapshotPending = false;
let activeParseJob = null;

const PARSE_BUTTON_CONFIG = {
    magnet: {
        buttonId: 'magnetParseBtn',
        idleHtml: '<i class="ph ph-magnifying-glass"></i> 解析后选择',
        busyHtml: '<span class="spinner"></span> 解析中...'
    },
    share: {
        buttonId: 'shareParseBtn',
        idleHtml: '<i class="ph ph-magnifying-glass"></i> 解析',
        busyHtml: '<span class="spinner"></span> 解析中...'
    },
    rss: {
        buttonId: 'rssParseBtn',
        idleHtml: '<i class="ph ph-radar"></i> 扫描提取',
        busyHtml: '<span class="spinner"></span> 扫描中...'
    }
};

function formatProgressLogTime(value) {
    if (!value) return new Date().toLocaleTimeString('zh-CN', { hour12: false });
    const normalized = String(value).includes(' ') ? String(value).replace(' ', 'T') : String(value);
    const date = new Date(normalized);
    if (Number.isNaN(date.getTime())) {
        const parts = String(value).split(' ');
        return parts[1] || String(value);
    }
    return date.toLocaleTimeString('zh-CN', { hour12: false });
}

function resetProgressLogView(emptyText = '系统处于空闲状态...') {
    const container = document.getElementById('logContainer');
    if (!container) return;
    progressLogSeenIds.clear();
    container.innerHTML = `<div class="log-empty" id="logEmpty"><i class="ph ph-ghost"></i> ${escapeA2TDHtml(emptyText)}</div>`;
}

function addLogEntry(icon, text, options = {}) {
    const container = document.getElementById('logContainer');
    if (!container) return;

    const normalizedLogId = options.logId !== undefined && options.logId !== null
        ? String(options.logId)
        : '';
    if (normalizedLogId) {
        if (progressLogSeenIds.has(normalizedLogId)) return;
        progressLogSeenIds.add(normalizedLogId);
    }

    const empty = document.getElementById('logEmpty');
    if (empty) empty.remove();

    const entry = document.createElement('div');
    entry.className = 'log-entry';
    if (normalizedLogId) entry.dataset.logId = normalizedLogId;
    entry.innerHTML = `<span class="log-icon">${icon}</span><span class="log-text">${text}</span><span class="log-time">${options.timeText || new Date().toLocaleTimeString('zh-CN', { hour12: false })}</span>`;
    container.appendChild(entry);
    container.scrollTop = container.scrollHeight;
}

function normalizeProgressLogMessage(record = {}) {
    const payload = record && typeof record.payload === 'object' && record.payload !== null
        ? { ...record.payload }
        : { ...(record || {}) };
    const logId = payload.log_id ?? record.id ?? record.log_id ?? null;
    const createdAt = payload.created_at || record.created_at || '';
    const jobId = payload.job_id || payload.parse_job_id || record.job_id || null;
    return {
        ...payload,
        type: payload.type || record.message_type || record.type || 'info',
        log_id: logId,
        created_at: createdAt,
        job_id: jobId,
        parse_job_id: payload.parse_job_id || jobId,
    };
}

function appendProgressLogMessage(msg) {
    const normalized = normalizeProgressLogMessage(msg);
    const entry = buildProgressLogEntry(normalized);
    if (!entry) return;
    addLogEntry(entry.icon, entry.text, {
        logId: normalized.log_id,
        timeText: formatProgressLogTime(normalized.created_at),
    });
}

function getParseButtonConfig(jobType = '') {
    return PARSE_BUTTON_CONFIG[String(jobType || '').trim().toLowerCase()] || null;
}

function setParseButtonsState(job = activeParseJob) {
    activeParseJob = job && typeof job === 'object' ? job : null;
    const activeType = String(activeParseJob?.job_type || '').trim().toLowerCase();
    const hasActive = !!(activeParseJob && ['pending', 'running'].includes(String(activeParseJob.status || '').trim().toLowerCase()));

    Object.entries(PARSE_BUTTON_CONFIG).forEach(([jobType, cfg]) => {
        const btn = document.getElementById(cfg.buttonId);
        if (!btn) return;
        btn.disabled = hasActive;
        btn.innerHTML = hasActive && jobType === activeType ? cfg.busyHtml : cfg.idleHtml;
        btn.title = hasActive && jobType !== activeType ? '当前已有解析任务正在执行' : '';
        btn.classList.toggle('is-loading', hasActive && jobType === activeType);
    });
}

function renderMagnetParseResult(result = {}) {
    if (!result || typeof result !== 'object') return;
    magnetCurrentFileId = result.file_id || null;
    magnetFileData = sortPickerItemsByName(result.files || []);
    const titleEl = document.getElementById('magnetFileName');
    const metaEl = document.getElementById('magnetPanelMeta');
    if (titleEl) {
        titleEl.innerHTML = `<i class="ph ph-folder-open"></i> ${escapeA2TDHtml(result.file_name || '磁链文件列表')}`;
    }
    if (metaEl) {
        metaEl.textContent = `已解析 ${magnetFileData.length} 项，可筛选节点并推送下载链接`;
    }
    renderPickerTree('magnetFileList', magnetFileData, 'magnet');
    const area = document.getElementById('magnetFileArea');
    if (area) area.style.display = 'flex';
    const selectAll = document.getElementById('magnetSelectAll');
    if (selectAll) selectAll.checked = true;
    updatePickerSelection('magnet');
}

function renderShareParseResult(result = {}) {
    if (!result || typeof result !== 'object') return;
    shareCurrentData = { ...(shareCurrentData || {}), ...result };
    shareFileData = sortPickerItemsByName(result.files || []);
    const shareMetaEl = document.getElementById('sharePanelMeta');
    if (shareMetaEl) {
        shareMetaEl.textContent = `已解析 ${shareFileData.length} 项，可筛选节点并执行同步下载`;
    }
    renderPickerTree('fileList', shareFileData, 'share');
    const area = document.getElementById('shareFileArea');
    area?.classList.add('visible');
    const selectAll = document.getElementById('selectAll');
    if (selectAll) selectAll.checked = true;
    updatePickerSelection('share');
}

function renderRssParseResult(result = {}) {
    if (!result || typeof result !== 'object') return;
    rssFileData = sortPickerItemsByName(result.items || []);
    const total = Number(result.count ?? rssFileData.length) || rssFileData.length;
    const titleEl = document.getElementById('rssFeedTitle');
    const metaEl = document.getElementById('rssPanelMeta');
    if (titleEl) {
        titleEl.innerHTML = `<i class="ph ph-feed"></i> ${escapeA2TDHtml(result.title || 'RSS Feed')}`;
    }
    if (metaEl) {
        metaEl.textContent = `已扫描 ${total} 项，可批量选择订阅项并执行下载`;
    }
    renderPickerTree('rssList', rssFileData, 'rss');
    const area = document.getElementById('rssResultArea');
    area?.classList.add('visible');
    const selectAll = document.getElementById('rssSelectAll');
    if (selectAll) selectAll.checked = true;
    updatePickerSelection('rss');
}

function restoreParseJobResult(job) {
    if (!job || String(job.status || '').toLowerCase() !== 'completed' || !job.result_payload) return;
    if (job.job_type === 'magnet') {
        renderMagnetParseResult(job.result_payload);
        return;
    }
    if (job.job_type === 'share') {
        renderShareParseResult(job.result_payload);
        return;
    }
    if (job.job_type === 'rss') {
        renderRssParseResult(job.result_payload);
    }
}

function restoreParseResultsFromSnapshot(snapshot = {}) {
    const latestJobs = snapshot.latest_jobs || {};
    ['magnet', 'share', 'rss'].forEach(jobType => restoreParseJobResult(latestJobs[jobType]));
}

function applyParseJobState(job) {
    const normalizedJob = job && typeof job === 'object' ? job : null;
    if (normalizedJob && String(normalizedJob.status || '').toLowerCase() === 'completed') {
        restoreParseJobResult(normalizedJob);
    }
    setParseButtonsState(normalizedJob && ['pending', 'running'].includes(String(normalizedJob.status || '').toLowerCase()) ? normalizedJob : null);
}

async function loadParseWorkspaceSnapshot(force = false) {
    if (progressSnapshotPending && !force) return;
    try {
        progressSnapshotPending = true;
        const resp = await fetch('/api/pikpak/progress/snapshot', { cache: 'no-store' });
        const data = await readJsonSafe(resp);
        if (!resp.ok) throw new Error(data.detail || data.message || data.error || '读取进度快照失败');

        resetProgressLogView(data.active_job ? '后台任务运行中，日志会持续写入...' : '系统处于空闲状态...');
        if (Array.isArray(data.logs)) {
            data.logs.forEach(item => appendProgressLogMessage(item));
        }
        restoreParseResultsFromSnapshot(data);
        applyParseJobState(data.active_job || null);
    } catch (e) {
        console.warn('加载解析快照失败:', e);
        setParseButtonsState(activeParseJob);
    } finally {
        progressSnapshotPending = false;
    }
}

async function clearLog() {
    try {
        const resp = await fetch('/api/pikpak/progress/logs', { method: 'DELETE' });
        const data = await readJsonSafe(resp);
        if (!resp.ok || data.success === false) throw new Error(data.detail || data.message || data.error || '清理日志失败');
        resetProgressLogView(activeParseJob ? '日志已清空，后台任务仍在继续...' : '系统处于空闲状态...');
    } catch (e) {
        alert(e.message || '清理日志失败');
    }
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

    const protection = stats.download_protection || {};
    const diskProtectNotice = document.getElementById('diskProtectNotice');
    const diskProtectNoticeText = document.getElementById('diskProtectNoticeText');
    if (diskProtectNotice) {
        if (protection.active) {
            const freeText = protection.free_bytes !== undefined ? formatBytes(getA2TDNumber(protection.free_bytes), 0) : '--';
            const thresholdText = protection.threshold_bytes !== undefined ? formatBytes(getA2TDNumber(protection.threshold_bytes), 0) : '--';
            const appliedMax = Math.max(1, getA2TDNumber(protection.applied_max_concurrent) || 1);
            const configuredMax = Math.max(1, getA2TDNumber(protection.configured_max_concurrent) || appliedMax);
            diskProtectNotice.style.display = 'flex';
            if (diskProtectNoticeText) {
                diskProtectNoticeText.textContent = `${protection.message || '磁盘不足，已自动保护'}（当前剩余 ${freeText}，保护阈值 ${thresholdText}，当前并发 ${appliedMax}/${configuredMax}）`;
            }
        } else {
            diskProtectNotice.style.display = 'none';
        }
    }

    if (stats.download_speed !== undefined) {
        const detail = stats.download_speed_detail || {};
        const aria2Speed = formatBytes(getA2TDNumber(detail.aria2 || stats.download_speed));
        const el = document.getElementById('sysDownloadStat');
        el.textContent = `${formatBytes(stats.download_speed)}/s`;
        el.title = `aria2 当前下载速度: ${aria2Speed}/s`;
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

function getA2TDTaskTotalText(task) {
    if (task.total_text) return task.total_text;
    const totalBytes = getA2TDNumber(task.total_bytes);
    if (totalBytes > 0) return formatBytes(totalBytes);
    return task.file_size || '--';
}

function getA2TDTaskDownloadedText(task) {
    if (task.downloaded_text) return task.downloaded_text;
    const downloadedBytes = getA2TDNumber(task.downloaded_bytes);
    if (downloadedBytes > 0) return formatBytes(downloadedBytes);
    const totalBytes = getA2TDNumber(task.total_bytes);
    const progress = Math.max(0, Math.min(100, getA2TDNumber(task.download_progress)));
    if (totalBytes > 0 && progress > 0) return formatBytes(Math.round(totalBytes * progress / 100));
    return '--';
}

function getA2TDTaskConnectionText(task) {
    if (!(task.status === 'downloading' || task.status === 'paused')) return '--';
    const current = Math.max(0, getA2TDNumber(task.connections));
    const max = Math.max(0, getA2TDNumber(task.max_connections));
    if (max > 0) return `${current}/${Math.max(current, max)}`;
    if (current > 0) return String(current);
    return task.status === 'paused' ? '0' : '--';
}

function buildA2TDTaskCardContent(task) {
    const mode = getA2TDTaskMode(task);
    const stalled = isA2TDTaskStalled(task);
    const progress = Math.max(0, Math.min(100, getA2TDTaskProgress(task)));
    const filenameText = task.filename || task.task_id || '未命名任务';
    const statusLabel = escapeA2TDHtml(getA2TDTaskStatusLabel(task.status));
    const downloadProgress = Math.min(task.status === 'completed' ? 100 : 99.9, Number(task.download_progress || 0)).toFixed(1);
    const totalText = getA2TDTaskTotalText(task);
    const transferredText = mode === 'upload'
        ? `${task.transferred_text || '0 B'} / ${totalText}`
        : `${getA2TDTaskDownloadedText(task)} / ${totalText}`;
    const speedText = task.download_speed || '0 B/s';
    const etaText = stalled
        ? '已无进度超过 15 秒'
        : (task.status === 'downloading' ? (task.eta_text || '--') : '--');
    const connectionText = getA2TDTaskConnectionText(task);
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
    const mode = normalizePikpakLoginMode(document.getElementById('cfgPikpakLoginMode')?.value || 'password');
    const passwordFields = document.getElementById('cfgPikpakPasswordFields');
    const tokenFields = document.getElementById('cfgPikpakTokenFields');
    syncPikpakLoginModeButtons('cfgPikpakLoginModeSwitch', mode);
    if (passwordFields) passwordFields.style.display = mode === 'token' ? 'none' : 'grid';
    if (tokenFields) tokenFields.style.display = mode === 'token' ? 'grid' : 'none';
}



function collectSettingsConfig() {
    const currentAria2 = window.currentConfig?.aria2 || {};
    const pikpakMode = normalizePikpakLoginMode(document.getElementById('cfgPikpakLoginMode')?.value || 'password');
    const pikpakUsername = document.getElementById('cfgPikpakUsername').value.trim();
    const pikpakPassword = document.getElementById('cfgPikpakPassword').value;
    const pikpakToken = document.getElementById('cfgPikpakToken').value.trim();
    return {
        auth: {
            username: document.getElementById('cfgAuthUser').value.trim(),
            password: document.getElementById('cfgAuthPass').value.trim()
        },
        server: { port: parseInt(document.getElementById('cfgServerPort').value, 10) || 8888 },
        pikpak: {
            login_mode: pikpakMode,
            username: pikpakMode === 'password' ? pikpakUsername : '',
            password: pikpakMode === 'password' ? pikpakPassword : '',
            session: pikpakMode === 'token' ? pikpakToken : '',
            save_dir: document.getElementById('cfgPikpakSaveDir').value || '/',
            delete_after_download: document.getElementById('cfgPikpakDelete').checked,
        },

        aria2: {
            rpc_url: document.getElementById('cfgAria2Url').value || currentAria2.rpc_url || 'http://127.0.0.1',
            rpc_port: Math.max(1, parseInt(document.getElementById('cfgAria2Port').value, 10) || currentAria2.rpc_port || 6800),
            rpc_secret: document.getElementById('cfgAria2Secret').value.trim(),
            allow_remote_access: !!document.getElementById('cfgAria2AllowRemoteAccess').checked,
            max_concurrent: Math.max(1, parseInt(document.getElementById('cfgAria2MaxConcurrent').value, 10) || currentAria2.max_concurrent || 3),
            disk_protection_threshold_gb: Math.max(1, parseInt(document.getElementById('cfgAria2DiskProtectionThreshold').value, 10) || currentAria2.disk_protection_threshold_gb || 5),
            split: Math.max(1, parseInt(document.getElementById('cfgAria2Split').value, 10) || currentAria2.split || 8),
            max_connection_per_server: Math.max(1, parseInt(document.getElementById('cfgAria2MaxConnPerServer').value, 10) || currentAria2.max_connection_per_server || 8),
            min_split_size_mb: Math.max(1, parseInt(document.getElementById('cfgAria2MinSplitSize').value, 10) || currentAria2.min_split_size_mb || 5),
        },


        teldrive: {
            api_host: document.getElementById('cfgTeldriveHost').value,
            access_token: document.getElementById('cfgTeldriveToken').value,
            channel_id: parseInt(document.getElementById('cfgTeldriveChannel').value, 10) || 0,
            upload_concurrency: parseInt(document.getElementById('cfgTeldriveConcurrency').value, 10) || 4,
            chunk_size: '500M'
        },
        upload: {
            auto_delete: document.getElementById('cfgUploadAutoDelete').checked,
            max_retries: 3
        },
        telegram: {
            api_id: parseInt(document.getElementById('cfgTelegramApiId').value, 10) || 0,
            api_hash: document.getElementById('cfgTelegramApiHash').value,
            channel_id: parseInt(document.getElementById('cfgTelegramChannelId').value, 10) || 0,
            sync_interval: parseInt(document.getElementById('cfgTelegramSyncInterval').value, 10) || 10,
            sync_enabled: document.getElementById('cfgTelegramSyncEnabled').checked
        },
        telegram_db: {
            host: document.getElementById('cfgDbHost').value,
            port: parseInt(document.getElementById('cfgDbPort').value, 10) || 5432,
            user: document.getElementById('cfgDbUser').value,
            password: document.getElementById('cfgDbPassword').value,
            name: document.getElementById('cfgDbName').value || 'postgres'
        },
        log: {
            buffer_size: Math.max(50, parseInt(document.getElementById('cfgLogBufferSize').value, 10) || window.currentConfig?.log?.buffer_size || 400)
        }
    };
}

async function loadConfig() {
    try {
        const resp = await fetch('/api/settings');
        const cfg = await readJsonSafe(resp);
        if (!resp.ok) throw new Error(cfg.detail || cfg.message || '读取配置失败');
        window.currentConfig = cfg;

        document.getElementById('cfgAuthUser').value = cfg.auth?.username || '';
        document.getElementById('cfgAuthPass').value = cfg.auth?.password || '';
        document.getElementById('cfgServerPort').value = cfg.server?.port || 8888;

        document.getElementById('cfgPikpakLoginMode').value = normalizePikpakLoginMode(cfg.pikpak?.login_mode || 'password');
        document.getElementById('cfgPikpakUsername').value = cfg.pikpak?.username || '';
        document.getElementById('cfgPikpakPassword').value = cfg.pikpak?.password || '';
        document.getElementById('cfgPikpakToken').value = cfg.pikpak?.session || '';
        document.getElementById('cfgPikpakSaveDir').value = cfg.pikpak?.save_dir || '/';
        document.getElementById('cfgPikpakDelete').checked = !!cfg.pikpak?.delete_after_download;
        togglePikpakLoginMode();


        document.getElementById('cfgAria2Url').value = cfg.aria2?.rpc_url || 'http://127.0.0.1';
        document.getElementById('cfgAria2Port').value = cfg.aria2?.rpc_port || 6800;
        document.getElementById('cfgAria2Secret').value = cfg.aria2?.rpc_secret || '';
        document.getElementById('cfgAria2AllowRemoteAccess').checked = !!cfg.aria2?.allow_remote_access;
        document.getElementById('cfgAria2MaxConcurrent').value = cfg.aria2?.max_concurrent || 3;
        document.getElementById('cfgAria2DiskProtectionThreshold').value = cfg.aria2?.disk_protection_threshold_gb || 5;
        document.getElementById('cfgAria2Split').value = cfg.aria2?.split || 8;
        document.getElementById('cfgAria2MaxConnPerServer').value = cfg.aria2?.max_connection_per_server || 8;
        document.getElementById('cfgAria2MinSplitSize').value = cfg.aria2?.min_split_size_mb || 5;
        document.getElementById('cfgAria2BinaryPath').textContent = cfg.aria2?.binary_path || '--';


        document.getElementById('cfgAria2DownloadDirText').textContent = cfg.aria2?.download_dir || '--';

        document.getElementById('cfgTeldriveHost').value = cfg.teldrive?.api_host || '';
        document.getElementById('cfgTeldriveToken').value = cfg.teldrive?.access_token || '';
        document.getElementById('cfgTeldriveChannel').value = cfg.teldrive?.channel_id || 0;
        document.getElementById('cfgTeldriveConcurrency').value = cfg.teldrive?.upload_concurrency || 4;
        document.getElementById('cfgUploadAutoDelete').checked = !!cfg.upload?.auto_delete;

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
        document.getElementById('cfgLogBufferSize').value = cfg.log?.buffer_size || 400;

        fillWizardInputs(cfg);
        await refreshAria2RuntimeStatus();
    } catch (e) {
        console.error('加载配置失败:', e);
    }
}


async function saveConfig() {
    const btn = document.getElementById('saveBtn');
    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> 保存...';
    
    const cfg = collectSettingsConfig();
    

    try {
        validateAria2AccessConfig(cfg.aria2);

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
        if (resp.ok) {
            mergeCurrentConfig(cfg);
            showFieldCheck(triggerInput);
        }

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
window.onload = async () => {
    const authReady = await waitForFrontendAuthReady();
    if (!authReady) {
        window.location.replace('/login');
        return;
    }

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
let magnetDownloadSubmitting = false;


// === Magnet Parsing ===
async function parseMagnet() {
    const input = document.getElementById('magnetInput').value.trim();
    if (!input) return alert('请输入磁力链接');
    if (activeParseJob) return alert('当前已有解析任务正在执行，请等待完成后再试');

    magnetCurrentFileId = null;
    magnetFileData = [];
    document.getElementById('magnetFileArea').style.display = 'none';
    setParseButtonsState({ job_type: 'magnet', status: 'running' });

    try {
        const resp = await fetch('/api/pikpak/magnet/parse', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ magnet: input.split('\n')[0] })
        });
        const data = await readJsonSafe(resp);
        if (resp.status === 409 && data.active_job) {
            applyParseJobState(data.active_job);
        }
        if (!resp.ok || data.success === false) {
            throw new Error(data.error || data.message || '解析失败');
        }

        applyParseJobState(data.job || { job_type: 'magnet', status: 'running' });
        if (typeof showA2TDToast === 'function') {
            showA2TDToast(data.message || '磁链解析任务已提交，正在后台执行', 'info');
        }
    } catch (e) {
        if (!activeParseJob) setParseButtonsState(null);
        alert(e.message || '解析失败');
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
    togglePickerSelectAll('magnet');
}


async function downloadMagnetFiles() {
    if (!magnetCurrentFileId || magnetDownloadSubmitting) return;

    const checkboxes = document.querySelectorAll('#magnetFileList input[data-role="file"]:checked');
    const selectedIds = Array.from(checkboxes).map(cb => cb.value);

    
    if (!selectedIds.length) {
        return alert('请先选择需要下载的文件');
    }

    const keepStructure = document.getElementById('magnetKeepStructure').checked;
    const teldrivePath = getTelDriveTargetPath('magnetTeldrivePath');
    const btn = document.getElementById('magnetDownloadBtn');
    if(!btn) return;
    magnetDownloadSubmitting = true;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 提交中...';


    try {
        const resp = await fetch('/api/pikpak/magnet/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                file_id: magnetCurrentFileId,
                selected_ids: selectedIds,
                keep_structure: keepStructure,
                teldrive_path: teldrivePath
            })
        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        magnetDownloadSubmitting = false;
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-download-simple"></i> 推送下载链接';
    }
}


// === Share Parsing ===
let shareCurrentData = null;
let shareFileData = [];
let shareDownloadSubmitting = false;
const pickerNameCollator = new Intl.Collator('zh-CN', { numeric: true, sensitivity: 'base' });
const pickerCollapsedState = {
    magnet: new Set(),
    share: new Set(),
    rss: new Set(),
};


function getPickerItemName(item = {}) {
    return String(item.name || item.title || '').trim();
}

function getPickerItemPath(item = {}) {
    return String(item.path || getPickerItemName(item)).trim();
}

function getPickerItemIdentity(item = {}) {
    return String(item.id || item.file_id || item.download_url || item.url || item.link || '').trim();
}

function comparePickerItemsByName(a = {}, b = {}) {
    const nameDiff = pickerNameCollator.compare(getPickerItemName(a), getPickerItemName(b));
    if (nameDiff !== 0) return nameDiff;

    const pathDiff = pickerNameCollator.compare(getPickerItemPath(a), getPickerItemPath(b));
    if (pathDiff !== 0) return pathDiff;

    return getPickerItemIdentity(a).localeCompare(getPickerItemIdentity(b));
}

function sortPickerItemsByName(files = []) {
    return Array.isArray(files) ? [...files].sort(comparePickerItemsByName) : [];
}

function getTelDriveTargetPath(inputId) {
    return (document.getElementById(inputId)?.value || '').trim() || '/';
}


async function parseShareLink() {
    const shareLink = document.getElementById('shareLink').value.trim();
    const passCode = document.getElementById('sharePassCode').value.trim();
    if (!shareLink) return alert('请输入分享链接');
    if (activeParseJob) return alert('当前已有解析任务正在执行，请等待完成后再试');

    shareCurrentData = null;
    shareFileData = [];
    document.getElementById('shareFileArea')?.classList.remove('visible');
    setParseButtonsState({ job_type: 'share', status: 'running' });

    try {
        const resp = await fetch('/api/pikpak/share/list', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ share_link: shareLink, pass_code: passCode })
        });
        const data = await readJsonSafe(resp);
        if (resp.status === 409 && data.active_job) {
            applyParseJobState(data.active_job);
        }
        if (!resp.ok || data.success === false) throw new Error(data.error || data.message || '解析失败');

        applyParseJobState(data.job || { job_type: 'share', status: 'running' });
        if (typeof showA2TDToast === 'function') {
            showA2TDToast(data.message || '分享解析任务已提交，正在后台执行', 'info');
        }
    } catch (e) {
        if (!activeParseJob) setParseButtonsState(null);
        alert(e.message || '解析失败');
    }
}

function toggleSelectAll() {
    togglePickerSelectAll('share');
}


function reRenderShareFileList() {
    shareFileData = sortPickerItemsByName(shareFileData);
    renderPickerTree('fileList', shareFileData, 'share');
}


async function downloadShareFiles() {
    if (!shareCurrentData || shareDownloadSubmitting) return;
    const checkboxes = document.querySelectorAll('#fileList input[data-role="file"]:checked');
    const selectedSet = new Set(Array.from(checkboxes).map(cb => String(cb.value || '')));

    const orderedSelectedItems = shareFileData.filter(item => selectedSet.has(String(item.id || '')));
    const selectedIds = orderedSelectedItems.map(item => item.id);
    
    if (!selectedIds.length) return alert('请先选择需要下载的分享节点');

    const keepStructure = document.getElementById('shareKeepStructure').checked;
    const renameByFolder = document.getElementById('shareRenameByFolder').checked;
    const teldrivePath = getTelDriveTargetPath('shareTeldrivePath');
    const filePaths = Object.fromEntries(
        orderedSelectedItems.map(item => [item.id, item.path || item.name || ''])
    );
    
    const btn = document.getElementById('downloadShareBtn');

    if(!btn) return;
    shareDownloadSubmitting = true;
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
                rename_by_folder: renameByFolder,
                teldrive_path: teldrivePath
            })

        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        shareDownloadSubmitting = false;
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-cloud-arrow-down"></i> 执行下载';

    }
}


// === RSS Parsing ===
let rssFileData = [];
let rssDownloadSubmitting = false;

async function parseRSS() {
    const url = document.getElementById('rssUrl').value.trim();
    if (!url) return alert('请输入 RSS 地址');
    if (activeParseJob) return alert('当前已有解析任务正在执行，请等待完成后再试');

    rssFileData = [];
    document.getElementById('rssResultArea')?.classList.remove('visible');
    setParseButtonsState({ job_type: 'rss', status: 'running' });

    try {
        const resp = await fetch('/api/pikpak/rss/parse', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url })
        });
        const data = await readJsonSafe(resp);
        if (resp.status === 409 && data.active_job) {
            applyParseJobState(data.active_job);
        }
        if (!resp.ok || data.success === false) throw new Error(data.error || data.message || '扫描失败');

        applyParseJobState(data.job || { job_type: 'rss', status: 'running' });
        if (typeof showA2TDToast === 'function') {
            showA2TDToast(data.message || 'RSS 解析任务已提交，正在后台执行', 'info');
        }
    } catch (e) {
        if (!activeParseJob) setParseButtonsState(null);
        alert(e.message || '扫描失败');
    }
}

function toggleRssSelectAll() {
    togglePickerSelectAll('rss');
}


async function downloadRssItems() {
    if (rssDownloadSubmitting) return;
    const checkboxes = document.querySelectorAll('#rssList input[data-role="file"]:checked');
    const selectedUrls = Array.from(checkboxes).map(cb => cb.value);

    
    if (!selectedUrls.length) return alert('请先选择需要订阅的项目');

    const teldrivePath = getTelDriveTargetPath('rssTeldrivePath');
    const btn = document.getElementById('rssDownloadBtn');
    if(!btn) return;
    rssDownloadSubmitting = true;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 提交中...';

    try {

        const resp = await fetch('/api/pikpak/rss/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ urls: selectedUrls, teldrive_path: teldrivePath })
        });
        const data = await resp.json();
        if(!resp.ok) throw new Error(data.error || '提交失败');
        
        switchPage('progress', { animated: true });
    } catch(e) {
        alert(e.message);
    } finally {
        rssDownloadSubmitting = false;
        btn.disabled = false;
        btn.innerHTML = '<i class="ph ph-download-simple"></i> 执行订阅下载';
    }
}


// === File Picker UI Utils ===

function getPickerContainerId(prefix) {
    if (prefix === 'magnet') return 'magnetFileList';
    if (prefix === 'share') return 'fileList';
    if (prefix === 'rss') return 'rssList';
    return '';
}

function getPickerContainer(prefix) {
    const containerId = getPickerContainerId(prefix);
    return containerId ? document.getElementById(containerId) : null;
}

function bindPickerWheelScroll(prefix) {
    const container = getPickerContainer(prefix);
    if (!container || container.dataset.wheelBound === 'true') return;

    container.dataset.wheelBound = 'true';
    container.addEventListener('wheel', (event) => {
        const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);
        if (maxScrollTop <= 0) return;

        const delta = event.deltaMode === 1 ? event.deltaY * 16 : event.deltaY;
        const nextScrollTop = Math.max(0, Math.min(container.scrollTop + delta, maxScrollTop));
        if (nextScrollTop === container.scrollTop) return;

        container.scrollTop = nextScrollTop;
        event.preventDefault();
        event.stopPropagation();
    }, { passive: false });
}

function getPickerSelectAllInput(prefix) {

    if (prefix === 'magnet') return document.getElementById('magnetSelectAll');
    if (prefix === 'share') return document.getElementById('selectAll');
    if (prefix === 'rss') return document.getElementById('rssSelectAll');
    return null;
}

function getPickerFileCheckboxes(prefix) {
    return Array.from(getPickerContainer(prefix)?.querySelectorAll('input[data-role="file"]') || []);
}

function getPickerFolderCheckboxes(prefix) {
    return Array.from(getPickerContainer(prefix)?.querySelectorAll('input[data-role="folder"]') || []);
}

function getPickerCollapsedSet(prefix) {
    if (!pickerCollapsedState[prefix]) pickerCollapsedState[prefix] = new Set();
    return pickerCollapsedState[prefix];
}

function isDescendantPickerPath(path = '', parentPath = '') {
    return !!path && !!parentPath && path !== parentPath && path.startsWith(`${parentPath}/`);
}

function isPickerFolder(item = {}) {
    const kind = String(item.kind || item.type || '').toLowerCase();
    const mimeType = String(item.mime_type || item.file_type || '').toLowerCase();
    return kind.includes('folder') || mimeType.includes('folder') || (!!item.original_url && Number(item.size || 0) === 0 && !item.extension);
}

function getPickerPathSegments(item = {}, prefix = '') {
    if (prefix === 'rss' && !isPickerFolder(item)) {
        const title = String(item.title || item.name || item.path || '').trim();
        return title ? [title] : [];
    }

    const rawPath = String(item.path || item.name || item.title || '').replace(/\\/g, '/').trim();
    return rawPath.split('/').map(part => part.trim()).filter(Boolean);
}

function createPickerTreeNode(name = '', path = '') {
    return { name, path, folders: new Map(), files: [], fileCount: 0 };
}

function buildPickerTree(files = [], prefix = '') {
    const root = createPickerTreeNode();

    files.forEach(item => {
        const segments = getPickerPathSegments(item, prefix);
        if (!segments.length) return;


        const folderSegments = isPickerFolder(item) ? segments : segments.slice(0, -1);
        let node = root;
        let currentPath = '';

        folderSegments.forEach(segment => {
            currentPath = currentPath ? `${currentPath}/${segment}` : segment;
            if (!node.folders.has(segment)) {
                node.folders.set(segment, createPickerTreeNode(segment, currentPath));
            }
            node = node.folders.get(segment);
        });

        if (!isPickerFolder(item)) {
            node.files.push(item);
        }
    });

    const countFiles = (node) => {
        let count = node.files.length;
        node.files = sortPickerItemsByName(node.files);
        node.folders.forEach(child => {
            count += countFiles(child);
        });
        node.fileCount = count;
        return count;
    };

    countFiles(root);
    return root;
}

function syncPickerSelectAll(prefix) {
    const selectAll = getPickerSelectAllInput(prefix);
    const fileCheckboxes = getPickerFileCheckboxes(prefix);
    if (!selectAll) return;
    if (!fileCheckboxes.length) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
        return;
    }

    const checkedCount = fileCheckboxes.filter(cb => cb.checked).length;
    selectAll.checked = checkedCount === fileCheckboxes.length;
    selectAll.indeterminate = checkedCount > 0 && checkedCount < fileCheckboxes.length;
}

function syncPickerFolderStates(prefix) {
    const fileCheckboxes = getPickerFileCheckboxes(prefix);
    getPickerFolderCheckboxes(prefix).forEach(folderCheckbox => {
        const folderPath = String(folderCheckbox.dataset.path || '');
        const descendants = fileCheckboxes.filter(cb => isDescendantPickerPath(String(cb.dataset.treePath || ''), folderPath));
        if (!descendants.length) {
            folderCheckbox.checked = false;
            folderCheckbox.indeterminate = false;
            return;
        }
        const checkedCount = descendants.filter(cb => cb.checked).length;
        folderCheckbox.checked = checkedCount === descendants.length;
        folderCheckbox.indeterminate = checkedCount > 0 && checkedCount < descendants.length;
    });
}

function applyPickerCollapsedState(prefix) {
    const container = getPickerContainer(prefix);
    if (!container) return;

    const collapsedSet = getPickerCollapsedSet(prefix);
    const rows = Array.from(container.querySelectorAll('[data-tree-path]'));
    rows.forEach(row => {
        const path = String(row.dataset.treePath || '');
        const isHidden = Array.from(collapsedSet).some(collapsedPath => isDescendantPickerPath(path, collapsedPath));
        row.classList.toggle('is-hidden', isHidden);
        row.setAttribute('aria-hidden', isHidden ? 'true' : 'false');
    });

    Array.from(container.querySelectorAll('.picker-folder-toggle')).forEach(toggleBtn => {
        const path = String(toggleBtn.dataset.path || '');
        toggleBtn.classList.toggle('is-collapsed', collapsedSet.has(path));
    });
}

function handlePickerFolderRowClick(event, row) {
    if (event?.target?.closest('input, button, a')) return;
    const toggleBtn = row?.querySelector('.picker-folder-toggle');
    if (toggleBtn) togglePickerFolderCollapsed(toggleBtn);
}

function togglePickerSelectAll(prefix) {

    const isChecked = !!getPickerSelectAllInput(prefix)?.checked;
    getPickerFileCheckboxes(prefix).forEach(cb => {
        cb.checked = isChecked;
    });
    syncPickerFolderStates(prefix);
    updatePickerSelection(prefix);
}

function togglePickerFolderSelection(input) {
    const prefix = String(input?.dataset?.prefix || '');
    const folderPath = String(input?.dataset?.path || '');
    const checked = !!input?.checked;
    if (!prefix || !folderPath) return;

    getPickerFileCheckboxes(prefix)
        .filter(cb => isDescendantPickerPath(String(cb.dataset.treePath || ''), folderPath))
        .forEach(cb => {
            cb.checked = checked;
        });

    getPickerFolderCheckboxes(prefix)
        .filter(cb => String(cb.dataset.path || '') !== folderPath && isDescendantPickerPath(String(cb.dataset.path || ''), folderPath))
        .forEach(cb => {
            cb.checked = checked;
            cb.indeterminate = false;
        });

    syncPickerFolderStates(prefix);
    updatePickerSelection(prefix);
}

function togglePickerFolderCollapsed(button) {
    const prefix = String(button?.dataset?.prefix || '');
    const folderPath = String(button?.dataset?.path || '');
    if (!prefix || !folderPath) return;

    const collapsedSet = getPickerCollapsedSet(prefix);
    if (collapsedSet.has(folderPath)) {
        collapsedSet.delete(folderPath);
    } else {
        collapsedSet.add(folderPath);
    }
    applyPickerCollapsedState(prefix);
}

function renderPickerTreeRows(node, prefix, depth = 0) {
    let html = '';
    const folders = Array.from(node.folders.values()).sort((a, b) => pickerNameCollator.compare(a.name, b.name));
    const collapsedSet = getPickerCollapsedSet(prefix);

    folders.forEach(folder => {
        const folderPathRaw = String(folder.path || folder.name || '');
        const folderPath = escapeA2TDHtml(folderPathRaw);
        const folderName = escapeA2TDHtml(folder.name || '未命名目录');
        const folderCount = escapeA2TDHtml(folder.fileCount > 0 ? `${folder.fileCount} 项` : '空目录');
        const isCollapsed = collapsedSet.has(folderPathRaw);
        html += `
            <div class="file-row folder-row picker-tree-row picker-folder-row" style="--picker-depth:${depth}" data-tree-path="${folderPath}" onclick="handlePickerFolderRowClick(event, this)">
                <input type="checkbox" data-role="folder" data-prefix="${prefix}" data-path="${folderPath}" onchange="togglePickerFolderSelection(this)">
                <button type="button" class="picker-folder-toggle${isCollapsed ? ' is-collapsed' : ''}" data-prefix="${prefix}" data-path="${folderPath}" onclick="togglePickerFolderCollapsed(this)" aria-label="切换目录折叠状态">

                    <i class="ph ph-caret-down"></i>
                </button>
                <div class="file-icon"><i class="ph-fill ph-folder"></i></div>
                <div class="file-name" title="${folderPath}">${folderName}</div>
                <div class="file-size">${folderCount}</div>
            </div>
        `;
        html += renderPickerTreeRows(folder, prefix, depth + 1);
    });

    node.files.forEach(item => {
        const val = item.download_url || item.file_id || item.id;
        const title = escapeA2TDHtml(item.name || item.title || '未命名文件');
        const fullPathRaw = String(item.path || item.name || item.title || '');
        const fullPath = escapeA2TDHtml(fullPathRaw);
        const sizeOrTime = escapeA2TDHtml(item.size_str || item.published || '0 B');
        html += `
            <label class="file-row picker-tree-row" style="--picker-depth:${depth}" data-tree-path="${fullPath}">
                <input type="checkbox" data-role="file" data-prefix="${prefix}" data-tree-path="${fullPath}" value="${escapeA2TDHtml(val || '')}" onchange="updatePickerSelection('${prefix}')" checked>
                <div class="file-check-spacer" aria-hidden="true"></div>
                <div class="file-icon"><i class="ph-fill ph-file"></i></div>
                <div class="file-name" title="${fullPath}">${title}</div>
                <div class="file-size">${sizeOrTime}</div>
            </label>
        `;
    });

    return html;
}

function renderPickerTree(containerId, files, prefix) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (!files || files.length === 0) {
        container.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-dim);">没有找到任何内容或空文件夹</div>';
        syncPickerSelectAll(prefix);
        return;
    }

    const tree = buildPickerTree(files, prefix);
    const html = renderPickerTreeRows(tree, prefix);

    container.innerHTML = html || '<div style="padding:24px;text-align:center;color:var(--text-dim);">没有找到任何内容或空文件夹</div>';
    bindPickerWheelScroll(prefix);
    syncPickerFolderStates(prefix);
    applyPickerCollapsedState(prefix);
    syncPickerSelectAll(prefix);
}


function updatePickerSelection(prefix) {
    let countSpan = null;
    let selectedInfoSpan = null;
    const cbList = getPickerFileCheckboxes(prefix);

    if (prefix === 'magnet') {
        countSpan = document.getElementById('magnetFileCount');
        selectedInfoSpan = document.getElementById('magnetSelectedInfo');
    } else if (prefix === 'share') {
        countSpan = document.getElementById('fileCount');
        selectedInfoSpan = document.getElementById('selectedInfo');
    } else if (prefix === 'rss') {
        countSpan = document.getElementById('rssCount');
        selectedInfoSpan = document.getElementById('rssSelectedInfo');
    }

    if (!cbList.length) {
        if (countSpan) countSpan.textContent = '共 0 项';
        if (selectedInfoSpan) {
            if (prefix === 'magnet') {
                selectedInfoSpan.textContent = '已选 0 项待推送';
            } else if (['share', 'rss'].includes(prefix)) {
                selectedInfoSpan.textContent = '已选 0 项待下载';
            } else {
                selectedInfoSpan.textContent = '已选 0 个对象';
            }
        }
        syncPickerSelectAll(prefix);
        return;
    }

    syncPickerFolderStates(prefix);

    const total = cbList.length;
    const checkedCount = cbList.filter(cb => cb.checked).length;

    if (countSpan) countSpan.textContent = `共 ${total} 项`;
    if (selectedInfoSpan) {
        if (prefix === 'magnet') {
            selectedInfoSpan.textContent = `已选 ${checkedCount} 项待推送`;
        } else if (['share', 'rss'].includes(prefix)) {
            selectedInfoSpan.textContent = `已选 ${checkedCount} 项待下载`;
        } else {
            selectedInfoSpan.textContent = `已选 ${checkedCount} 个对象`;
        }
    }

    syncPickerSelectAll(prefix);
}

