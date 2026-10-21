// Downloads require manual selection or a saved, explicitly enabled channel sync rule.
const telegramChannelsState = {
    mode: 'tasks', channels: [], channelId: '', videos: [], selected: new Set(),
    hasMore: false, nextBeforeId: null, loaded: false, loading: false,
    channelsRequest: 0, videosRequest: 0, selectionEpoch: 0, adding: false, submitting: false,
    scanning: new Set(), changing: new Set(), cooldownUntil: null, pollTimer: null,
    drafts: new Map(), saving: new Set(), settingsVersions: new Map(), pickerContext: null,
};
const TELEGRAM_VIDEO_PAGE_SIZE = 50;
const TELEGRAM_VIDEO_SELECTION_LIMIT = 100;
const TELEGRAM_VIDEO_STATUS = {
    pending: ['等待下载', 'primary'], downloading: ['下载中', 'primary'],
    uploading: ['上传中', 'primary'], cleaning: ['清理中', 'primary'],
    completed: ['已上传', 'success'], failed: ['下载失败', 'error'],
    cancelled: ['已取消', 'warning'],
};

function telegramChannelEscape(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
}

function telegramChannelId(value) {
    const id = String(value ?? '').trim();
    return /^-?\d+$/.test(id) ? id : '';
}

function telegramChannelSourceLink(value) {
    try {
        const url = new URL(String(value || ''));
        return url.protocol === 'https:' && url.hostname === 't.me' && !url.username && !url.password
            ? url.href : '';
    } catch (_) { return ''; }
}

function telegramChannelDate(value) {
    if (!value) return '时间未知';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '时间未知' : date.toLocaleString('zh-CN', { hour12: false });
}

function telegramVideoSize(value) {
    const size = Number(value);
    if (!Number.isFinite(size) || size <= 0) return '大小未知';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const index = Math.min(Math.floor(Math.log(size) / Math.log(1024)), units.length - 1);
    return `${(size / 1024 ** index).toFixed(index > 0 ? 1 : 0)} ${units[index]}`;
}

function telegramVideoDuration(value) {
    const seconds = Math.max(0, Math.floor(Number(value) || 0));
    if (!seconds) return '时长未知';
    return seconds >= 3600
        ? `${Math.floor(seconds / 3600)}:${String(Math.floor(seconds / 60) % 60).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`
        : `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}

function selectedTelegramChannel() {
    return telegramChannelsState.channels.find(item => String(item.channel_id) === telegramChannelsState.channelId);
}

function telegramVideoSelectable(video) {
    return !video.job_status && !video.job_id && !video.auto_job_id && video.auto_status !== 'submitted';
}

function telegramChannelLocalDate(value) {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    const pad = part => String(part).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function telegramChannelUTCDate(value) {
    if (!value) return '';
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/.test(value)) throw new Error('请选择有效的发布时间起点');
    const date = new Date(value);
    const canonical = value.length === 16 ? value + ':00' : value;
    if (Number.isNaN(date.getTime()) || telegramChannelLocalDate(date) !== canonical) throw new Error('请选择有效的本地日期和时间');
    return date.toISOString();
}

function telegramChannelSettingsValues(channel) {
    // datetime-local inputs omit the seconds segment when it is exactly zero.
    const local = telegramChannelLocalDate(channel?.sync_since).replace(/:00$/, '');
    return { targetPath: String(channel?.target_path || ''), syncSinceLocal: local };
}

function getTelegramChannelDraft(channel = selectedTelegramChannel()) {
    if (!channel) return null;
    const state = telegramChannelsState;
    const id = String(channel.channel_id);
    let draft = state.drafts.get(id);
    if (!draft || (!draft.dirty && !state.saving.has(id))) {
        const values = telegramChannelSettingsValues(channel);
        draft = { ...values, baseline: { ...values }, dirty: false, revision: draft?.revision || 0, message: draft?.message || '', messageKind: draft?.messageKind || '' };
        state.drafts.set(id, draft);
    }
    return draft;
}

function telegramChannelSyncStatus(channel) {
    if (!channel) return ['未选择频道', ''];
    const statuses = {
        manual: ['未启动', ''], paused: ['监控已暂停', 'warning'],
        relay_disabled: ['下载上传总开关已关闭', 'warning'], backfilling: ['补齐历史视频中', 'primary'],
        queued: ['自动任务已入队', 'primary'], watching: ['自动同步已开启', 'success'],
        backpressure: ['等待下载队列空位', 'warning'], error: ['同步异常', 'error'],
    };
    if (!channel.auto_sync) return statuses.manual;
    if (!channel.enabled) return statuses.paused;
    return statuses[channel.sync_status] || (channel.auto_sync ? statuses.watching : statuses.manual);
}

function renderTelegramChannelSettings() {
    const state = telegramChannelsState;
    const channel = selectedTelegramChannel();
    const draft = getTelegramChannelDraft(channel);
    const saving = state.saving.has(state.channelId);
    const busy = saving || state.changing.has(state.channelId);
    const title = document.getElementById('telegramChannelSettingsTitle');
    if (title) title.textContent = channel ? channel.title || channel.username || state.channelId : '频道同步设置';
    const [status, statusClass] = telegramChannelSyncStatus(channel);
    const badge = document.getElementById('telegramChannelSyncBadge');
    if (badge) { badge.textContent = status; badge.className = `telegram-channel-badge ${statusClass}`; }
    for (const [id, key] of [['telegramChannelTargetPath', 'targetPath'], ['telegramChannelSyncSince', 'syncSinceLocal']]) {
        const input = document.getElementById(id);
        if (input) {
            const value = draft?.[key] || '';
            if (input.value !== value) input.value = value;
            input.disabled = !channel || busy;
        }
    }
    const browse = document.getElementById('telegramChannelBrowseBtn');
    if (browse) browse.disabled = !channel || busy;
    const save = document.getElementById('telegramChannelSettingsSaveBtn');
    if (save) { save.disabled = !channel || !draft?.dirty || busy; save.textContent = saving ? '正在保存…' : '保存设置'; }
    const reset = document.getElementById('telegramChannelSettingsResetBtn');
    if (reset) reset.disabled = !draft?.dirty || busy;
    const message = document.getElementById('telegramChannelSettingsStatus');
    if (message) {
        const error = channel?.last_sync_error || '';
        const pending = Math.max(0, Number(channel?.auto_pending_count) || 0);
        const failed = Math.max(0, Number(channel?.auto_failed_count) || 0);
        const counts = [pending ? `待处理 ${pending}` : '', failed ? `失败 ${failed}` : ''].filter(Boolean).join(' · ');
        message.textContent = (draft?.messageKind === 'error' ? draft.message : error) || draft?.message || counts;
        message.hidden = !message.textContent;
        message.classList.toggle('error', draft?.messageKind === 'error' || !!error);
        message.classList.toggle('success', draft?.messageKind === 'success' && !error);
    }
    const timeInput = document.getElementById('telegramChannelSyncSince');
    if (timeInput) {
        const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
        timeInput.title = `包含所选时刻，按浏览器本地时区${zone ? '（' + zone + '）' : ''}填写。`;
    }
    refreshTelegramChannelDestination();
}

function editTelegramChannelSettings() {
    const state = telegramChannelsState;
    if (state.saving.has(state.channelId)) return;
    const draft = getTelegramChannelDraft();
    if (!draft) return;
    draft.targetPath = document.getElementById('telegramChannelTargetPath')?.value || '';
    draft.syncSinceLocal = document.getElementById('telegramChannelSyncSince')?.value || '';
    draft.dirty = draft.targetPath !== draft.baseline.targetPath || draft.syncSinceLocal !== draft.baseline.syncSinceLocal;
    draft.revision += 1;
    draft.message = '';
    draft.messageKind = '';
    renderTelegramChannelSettings();
}

function resetTelegramChannelSettings() {
    const state = telegramChannelsState;
    if (state.saving.has(state.channelId)) return;
    state.drafts.delete(state.channelId);
    closeTelegramChannelFolderPicker();
    renderTelegramChannelSettings();
}

function normalizeTelegramChannelPath(value) {
    const path = String(value || '').trim();
    if (!path) return '';
    if (typeof normalizeRelayTargetPath === 'function') return normalizeRelayTargetPath(path);
    if (/[\\\x00-\x1f]/.test(path) || path.split('/').some(part => part === '.' || part === '..')) throw new Error('TelDrive 文件夹路径无效');
    return '/' + path.split('/').filter(Boolean).join('/');
}

async function saveTelegramChannelSettings(event) {
    event?.preventDefault();
    const state = telegramChannelsState;
    const id = state.channelId;
    const epoch = state.selectionEpoch;
    const draft = getTelegramChannelDraft();
    if (!id || !draft?.dirty || state.saving.has(id) || state.changing.has(id)) return;
    let payload;
    try {
        payload = { target_path: normalizeTelegramChannelPath(draft.targetPath), sync_since: telegramChannelUTCDate(draft.syncSinceLocal) };
        if (!payload.sync_since) delete payload.sync_since;
    } catch (error) {
        draft.message = error.message;
        draft.messageKind = 'error';
        renderTelegramChannelSettings();
        return;
    }
    const revision = draft.revision;
    state.saving.add(id);
    draft.message = '';
    closeTelegramChannelFolderPicker();
    renderTelegramChannelSettings();
    try {
        const data = await telegramChannelsRequest(`/${id}`, telegramChannelJson('PATCH', payload));
        const updated = data.channel;
        if (!updated || String(updated.channel_id) !== id) throw new Error('保存响应无效，请刷新后确认设置');
        state.settingsVersions.set(id, (state.settingsVersions.get(id) || 0) + 1);
        state.channelsRequest += 1; // Invalidate older list snapshots taken before this save.
        state.channels = state.channels.map(channel => String(channel.channel_id) === id ? { ...channel, ...updated } : channel);
        if (state.drafts.get(id) === draft && draft.revision === revision) {
            const values = telegramChannelSettingsValues(updated);
            const message = updated.enabled === false ? '已保存，监控仍暂停' : '已保存';
            Object.assign(draft, values, { baseline: { ...values }, dirty: false, message, messageKind: 'success' });
        }
        if (state.channelId === id && state.selectionEpoch === epoch) {
            state.videosRequest += 1; // An older videos response also embeds the previous channel settings.
            state.loading = false;
        }
    } catch (error) {
        if (state.drafts.get(id) === draft && draft.revision === revision) {
            draft.message = error.message;
            draft.messageKind = 'error';
        }
    } finally {
        state.saving.delete(id);
        renderTelegramChannels();
        if (state.channelId === id) renderTelegramChannelVideos();
    }
}

function openTelegramChannelFolderPicker(trigger) {
    const state = telegramChannelsState;
    if (!selectedTelegramChannel() || state.saving.has(state.channelId)) return;
    state.pickerContext = { channelId: state.channelId, epoch: state.selectionEpoch };
    if (typeof openTelDriveFolderPicker === 'function') return openTelDriveFolderPicker('telegramChannelTargetPath', trigger);
}

function canApplyTelegramChannelFolder() {
    const state = telegramChannelsState;
    const context = state.pickerContext;
    return !!context && context.channelId === state.channelId && context.epoch === state.selectionEpoch && state.mode === 'channels' && !state.saving.has(state.channelId);
}

function closeTelegramChannelFolderPicker() {
    if (!telegramChannelsState.pickerContext) return;
    if (typeof closeTelDriveFolderPicker === 'function') closeTelDriveFolderPicker();
    telegramChannelsState.pickerContext = null;
}

function onTelegramChannelFolderPickerClosed(inputId) {
    if (inputId === 'telegramChannelTargetPath') telegramChannelsState.pickerContext = null;
}

function onTelegramChannelPageChange(page) {
    if (page !== 'telegramrelay') closeTelegramChannelFolderPicker();
}

function setTelegramChannelsNotice(message = '', kind = '') {
    const node = document.getElementById('telegramChannelsNotice');
    if (!node) return;
    node.textContent = message;
    node.classList.toggle('error', kind === 'error');
    node.hidden = !message;
}

async function telegramChannelsRequest(path = '', options = {}) {
    const response = await fetch(`/api/t2td/channels${path}`, { cache: 'no-store', ...options });
    let data = {};
    try { data = await response.json(); } catch (_) { /* Keep HTTP failures readable. */ }
    if (!response.ok || data.success === false) {
        let message = typeof data.detail === 'string' ? data.detail : data.message || '频道请求失败，请稍后重试';
        if (response.status === 429) {
            const seconds = Number(response.headers?.get('Retry-After'));
            if (seconds > 0) {
                telegramChannelsState.cooldownUntil = new Date(Date.now() + seconds * 1000).toISOString();
                message += `（请等待 ${Math.ceil(seconds)} 秒后重试）`;
            }
        }
        throw new Error(message);
    }
    return data;
}

function telegramChannelJson(method, payload) {
    return { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) };
}

function refreshTelegramChannelDestination() {
    const node = document.getElementById('telegramChannelsDestination');
    const fallback = window.currentConfig?.telegram_relay?.target_path || window.currentConfig?.teldrive?.target_path || '默认上传目录';
    if (node) { node.textContent = fallback; node.title = `默认 TelDrive 目录：${fallback}`; }
    const download = document.getElementById('telegramVideosDownloadBtn');
    const channel = selectedTelegramChannel();
    if (download) download.title = `上传到 TelDrive：${channel?.target_path || `${fallback}（沿用默认目录）`}`;
}

function setTelegramModuleMode(mode) {
    telegramChannelsState.mode = mode === 'channels' ? 'channels' : 'tasks';
    if (telegramChannelsState.mode !== 'channels') closeTelegramChannelFolderPicker();
    for (const [value, prefix] of [['tasks', 'telegramTasks'], ['channels', 'telegramChannels']]) {
        const active = telegramChannelsState.mode === value;
        const tab = document.getElementById(`${prefix}Tab`);
        const panel = document.getElementById(`${prefix}Panel`);
        if (tab) {
            tab.classList.toggle('active', active);
            tab.setAttribute('aria-selected', String(active));
            tab.tabIndex = active ? 0 : -1;
        }
        if (panel) panel.hidden = !active;
    }
    if (telegramChannelsState.mode === 'channels') loadTelegramChannelsPage();
}

function renderTelegramChannels() {
    const state = telegramChannelsState;
    const node = document.getElementById('telegramChannelsList');
    if (!node) return;
    const focusId = document.activeElement?.id;
    const html = state.channels.map(channel => {
        const id = telegramChannelId(channel.channel_id);
        if (!id) return '';
        const active = state.channelId === id;
        const disabled = state.changing.has(id) || state.saving.has(id) ? ' disabled' : '';
        const [syncLabel, syncClass] = telegramChannelSyncStatus(channel);
        const running = !!channel.enabled && !!channel.auto_sync;
        const toggleLabel = running ? '暂停监控' : channel.auto_sync ? '继续监控' : '启动监控';
        return `<article class="telegram-channel-item${active ? ' active' : ''}">
            <button type="button" id="telegram-channel-${id}" class="telegram-channel-open" data-channel-action="select" data-channel-id="${id}" aria-pressed="${active}"><strong>${telegramChannelEscape(channel.title || channel.username || id)}</strong><span class="telegram-channel-hint">${telegramChannelEscape(channel.username ? '@' + channel.username : id)}</span></button>
            <div class="telegram-channel-item-meta"><span class="telegram-channel-badge ${syncClass}">${syncLabel}</span><span class="telegram-channel-hint">${Math.max(0, Number(channel.video_count) || 0)} 个视频</span>${channel.protected ? '<span class="telegram-channel-badge warning">受保护频道</span>' : ''}</div>
            <p class="telegram-channel-path-summary"><i class="ph ph-folder-open"></i> ${telegramChannelEscape(channel.target_path || '沿用默认上传目录')}</p>
            ${channel.last_error ? `<p class="telegram-channel-hint telegram-channel-item-error">${telegramChannelEscape(channel.last_error)}</p>` : ''}
            <div class="telegram-channel-actions"><button id="telegram-channel-toggle-${id}" type="button" class="btn btn-ghost btn-sm" data-channel-action="toggle" data-channel-id="${id}"${disabled}>${toggleLabel}</button><button id="telegram-channel-remove-${id}" type="button" class="btn btn-ghost btn-sm telegram-channel-remove" data-channel-action="remove" data-channel-id="${id}"${disabled}>移除</button></div>
        </article>`;
    }).join('') || `<div class="telegram-channel-empty"><i class="ph ph-broadcast"></i>${state.loaded ? '尚未添加频道' : '正在读取频道…'}</div>`;
    if (node.innerHTML !== html) {
        node.innerHTML = html;
        if (focusId?.startsWith('telegram-channel-')) document.getElementById(focusId)?.focus({ preventScroll: true });
    }
    renderTelegramChannelControls();
}

function renderTelegramChannelControls() {
    const state = telegramChannelsState;
    const channel = selectedTelegramChannel();
    const title = document.getElementById('telegramChannelVideosTitle');
    if (title) title.textContent = '视频列表';
    const summary = document.getElementById('telegramChannelSummary');
    if (summary) {
        summary.textContent = `${Math.max(0, Number(channel?.video_count) || 0)} 个视频`;
        summary.title = channel?.last_scan ? '上次扫描 ' + telegramChannelDate(channel.last_scan) : '';
        summary.hidden = !channel;
    }
    const scanning = state.scanning.has(state.channelId);
    const cooling = new Date(state.cooldownUntil || 0).getTime() > Date.now();
    const latest = document.getElementById('telegramChannelScanBtn');
    const older = document.getElementById('telegramChannelHistoryBtn');
    if (latest) {
        latest.disabled = !channel || scanning || cooling;
        latest.innerHTML = `<i class="ph ${scanning ? 'ph-spinner-gap' : 'ph-arrows-clockwise'}"></i> ${scanning ? '扫描中…' : '刷新最新'}`;
    }
    if (older) {
        older.disabled = !channel || scanning || cooling || !!channel.history_complete;
        older.textContent = channel?.history_complete ? '历史已扫描完' : '扫描更早';
    }
    const add = document.getElementById('telegramChannelAddBtn');
    if (add) { add.disabled = state.adding || cooling; add.textContent = state.adding ? '正在添加…' : '添加监控'; }
    refreshTelegramChannelDestination();
    renderTelegramChannelSettings();
}

function renderTelegramVideoRow(video) {
    const id = Number(video.message_id);
    if (!Number.isSafeInteger(id) || id <= 0) return '';
    const state = telegramChannelsState;
    const selected = state.selected.has(id);
    const selectable = telegramVideoSelectable(video);
    const archivedStatus = !video.job_status && video.auto_job_status ? TELEGRAM_VIDEO_STATUS[video.auto_job_status] : null;
    const [status, statusClass] = TELEGRAM_VIDEO_STATUS[video.job_status] || (archivedStatus ? [archivedStatus[0] + ' · 任务记录已清理', archivedStatus[1]] : video.auto_status === 'submitted' ? ['已提交', 'primary'] : video.auto_status === 'pending' ? ['等待自动同步', 'primary'] : video.auto_status === 'failed' ? ['同步失败', 'error'] : [video.job_status ? '处理中' : '未下载', '']);
    const link = telegramChannelSourceLink(video.link);
    const rawProgress = video.job_status === 'uploading' ? video.upload_progress : video.download_progress;
    const progress = ['uploading', 'downloading'].includes(video.job_status) ? ` ${Math.min(100, Math.max(0, Number(rawProgress) || 0)).toFixed(1)}%` : '';
    return `<article class="telegram-video-row${selected ? ' selected' : ''}" data-video-row="${id}">
        <input type="checkbox" class="telegram-video-check" id="telegram-video-${id}" data-message-id="${id}"${selected ? ' checked' : ''}${!selectable || state.submitting ? ' disabled' : ''}>
        <div class="telegram-video-content"><label for="telegram-video-${id}" class="telegram-video-title">${telegramChannelEscape(video.file_name || '视频 #' + id)}</label>
            <div class="telegram-video-meta"><span>${telegramVideoSize(video.file_size)}</span><span>${telegramVideoDuration(video.duration)}</span><span>${telegramChannelEscape(telegramChannelDate(video.date))}</span><span>#${id}</span></div>
            ${video.caption ? `<p class="telegram-video-caption">${telegramChannelEscape(video.caption)}</p>` : ''}
            <div class="telegram-video-footer"><span class="telegram-channel-badge ${statusClass}">${status}${progress}</span>${video.protected ? '<span class="telegram-channel-badge warning"><i class="ph ph-lock-simple"></i> 内容受保护</span>' : ''}${link ? `<a href="${telegramChannelEscape(link)}" target="_blank" rel="noopener noreferrer">查看源消息 <i class="ph ph-arrow-up-right"></i></a>` : ''}${['failed', 'cancelled'].includes(video.job_status) ? '<button type="button" class="btn btn-ghost btn-sm" onclick="setTelegramModuleMode(\'tasks\')">在任务列表重试</button>' : ''}</div>
            ${video.job_error || video.auto_error ? `<p class="telegram-channel-hint telegram-video-error">${telegramChannelEscape(video.job_error || video.auto_error)}</p>` : ''}
        </div></article>`;
}

function renderTelegramChannelVideos() {
    const state = telegramChannelsState;
    const node = document.getElementById('telegramChannelVideos');
    if (!node) return;
    const focusId = document.activeElement?.id;
    const html = state.videos.map(renderTelegramVideoRow).join('') || `<div class="telegram-channel-empty"><i class="ph ${state.loading ? 'ph-spinner-gap' : 'ph-video-camera'}"></i>${state.loading ? '正在读取视频列表…' : state.channelId ? '暂未收录视频，可点击“刷新最新”或“扫描更早”。' : '选择一个频道，查看并挑选视频。'}</div>`;
    if (node.innerHTML !== html) {
        node.innerHTML = html;
        if (focusId?.startsWith('telegram-video-')) document.getElementById(focusId)?.focus({ preventScroll: true });
    }
    const more = document.getElementById('telegramVideosLoadMoreBtn');
    if (more) { more.hidden = !state.hasMore; more.disabled = state.loading; more.textContent = state.loading ? '正在加载…' : '加载更多已收录视频'; }
    const count = document.getElementById('telegramVideosCount');
    if (count) count.textContent = state.videos.length ? `已显示 ${state.videos.length} 个视频` : '';
    syncTelegramVideoSelection();
}

function syncTelegramVideoSelection() {
    const state = telegramChannelsState;
    const available = state.videos.filter(telegramVideoSelectable);
    const valid = new Set(available.map(video => Number(video.message_id)));
    state.selected = new Set([...state.selected].filter(id => valid.has(id)));
    const selectAll = document.getElementById('telegramVideosSelectAll');
    if (selectAll) {
        selectAll.disabled = !available.length || state.submitting;
        selectAll.checked = available.length > 0 && state.selected.size === available.length;
        selectAll.indeterminate = state.selected.size > 0 && state.selected.size < available.length;
    }
    const summary = document.getElementById('telegramVideosSelectionSummary');
    if (summary) summary.textContent = `已选 ${state.selected.size} 项${state.selected.size ? ' · ' + telegramVideoSize(state.videos.filter(video => state.selected.has(Number(video.message_id))).reduce((sum, video) => sum + Math.max(0, Number(video.file_size) || 0), 0)) : ''}`;
    const clear = document.getElementById('telegramVideosClearSelection');
    if (clear) clear.disabled = !state.selected.size || state.submitting;
    const download = document.getElementById('telegramVideosDownloadBtn');
    if (download) { download.disabled = !state.selected.size || state.submitting; download.textContent = state.submitting ? '正在提交…' : '下载并上传'; }
    document.querySelectorAll('#telegramChannelVideos [data-message-id]').forEach(input => {
        input.checked = state.selected.has(Number(input.dataset.messageId));
        input.disabled = state.submitting || !valid.has(Number(input.dataset.messageId));
        input.closest('.telegram-video-row')?.classList.toggle('selected', input.checked);
    });
}

function selectTelegramChannelVideos(checked) {
    const state = telegramChannelsState;
    if (state.submitting) return;
    const available = state.videos.filter(telegramVideoSelectable);
    state.selected = new Set(checked ? available.slice(0, TELEGRAM_VIDEO_SELECTION_LIMIT).map(video => Number(video.message_id)) : []);
    if (checked && available.length > TELEGRAM_VIDEO_SELECTION_LIMIT) setTelegramChannelsNotice(`每次最多选择 ${TELEGRAM_VIDEO_SELECTION_LIMIT} 项，已选取前 ${TELEGRAM_VIDEO_SELECTION_LIMIT} 项。`);
    syncTelegramVideoSelection();
}

function clearTelegramVideoSelection() { selectTelegramChannelVideos(false); }

async function loadTelegramChannelsPage(force = false, quiet = false) {
    const state = telegramChannelsState;
    refreshTelegramChannelDestination();
    if (state.loaded && !force) {
        renderTelegramChannels();
        if (state.channelId && !state.videos.length && !state.loading) await loadTelegramChannelVideos();
        return;
    }
    const request = ++state.channelsRequest;
    renderTelegramChannels();
    try {
        const data = await telegramChannelsRequest();
        if (request !== state.channelsRequest) return;
        state.drafts.forEach(draft => {
            if (draft.messageKind === 'success') { draft.message = ''; draft.messageKind = ''; }
        });
        state.channels = (Array.isArray(data.channels) ? data.channels : []).filter(item => telegramChannelId(item.channel_id));
        state.cooldownUntil = data.cooldown_until || null;
        state.loaded = true;
        renderTelegramChannels();
        if (!selectedTelegramChannel()) await selectTelegramChannel(state.channels[0]?.channel_id || '');
    } catch (error) {
        if (request === state.channelsRequest && !quiet) setTelegramChannelsNotice(error.message, 'error');
    }
}

async function selectTelegramChannel(id) {
    const state = telegramChannelsState;
    const nextId = telegramChannelId(id);
    if (nextId === state.channelId && state.videos.length) return;
    closeTelegramChannelFolderPicker();
    state.channelId = nextId;
    state.selectionEpoch += 1;
    state.videosRequest += 1;
    state.loading = false;
    state.videos = [];
    state.selected.clear();
    state.hasMore = false;
    state.nextBeforeId = null;
    setTelegramChannelsNotice();
    renderTelegramChannels();
    renderTelegramChannelVideos();
    if (nextId) await loadTelegramChannelVideos();
}

async function loadTelegramChannelVideos(append = false, preserve = false) {
    const state = telegramChannelsState;
    const channelId = state.channelId;
    if (!channelId || state.loading || (append && !state.nextBeforeId)) return;
    const settingsVersion = state.settingsVersions.get(channelId) || 0;
    const request = ++state.videosRequest;
    state.loading = true;
    if (!preserve) renderTelegramChannelVideos();
    const before = append ? `&before_id=${state.nextBeforeId}` : '';
    try {
        const data = await telegramChannelsRequest(`/${channelId}/videos?limit=${TELEGRAM_VIDEO_PAGE_SIZE}${before}`);
        if (request !== state.videosRequest || channelId !== state.channelId) return;
        const videos = (Array.isArray(data.videos) ? data.videos : []).filter(item => Number.isSafeInteger(Number(item.message_id)) && Number(item.message_id) > 0);
        const merged = new Map((append || preserve ? state.videos : []).map(video => [Number(video.message_id), video]));
        // A burst larger than one page can leave a gap above the cached rows.
        // Continue from the newest page's cursor until that gap has been loaded.
        const newPageWithoutOverlap = preserve && videos.length > 0 && videos.every(video => !merged.has(Number(video.message_id)));
        videos.forEach(video => merged.set(Number(video.message_id), video));
        const previousLength = state.videos.length;
        state.videos = [...merged.values()].sort((a, b) => Number(b.message_id) - Number(a.message_id));
        if (append || !preserve || previousLength === 0 || (newPageWithoutOverlap && data.has_more)) {
            state.hasMore = !!data.has_more;
            state.nextBeforeId = Number(data.next_before_id) || null;
        }
        if (data.channel && (state.settingsVersions.get(channelId) || 0) === settingsVersion) state.channels = state.channels.map(channel => String(channel.channel_id) === channelId ? data.channel : channel);
        renderTelegramChannels();
    } catch (error) {
        if (request === state.videosRequest && channelId === state.channelId) setTelegramChannelsNotice(error.message, 'error');
    } finally {
        if (request === state.videosRequest && channelId === state.channelId) {
            state.loading = false;
            renderTelegramChannelVideos();
        }
    }
}

async function addTelegramChannel(event) {
    event?.preventDefault();
    const state = telegramChannelsState;
    const input = document.getElementById('telegramChannelReference');
    const reference = String(input?.value || '').trim();
    if (!reference || state.adding) return;
    state.adding = true;
    renderTelegramChannelControls();
    setTelegramChannelsNotice('正在检查频道访问权限…');
    try {
        const data = await telegramChannelsRequest('', telegramChannelJson('POST', { reference }));
        if (input) input.value = '';
        await loadTelegramChannelsPage(true);
        if (data.channel?.channel_id) {
            await selectTelegramChannel(data.channel.channel_id);
            // Populate the catalogue immediately; this scan never queues downloads.
            await scanTelegramChannel(false);
        } else setTelegramChannelsNotice('已添加监控。视频列表会自动收录，勾选后才会开始下载。');
    } catch (error) { setTelegramChannelsNotice(error.message, 'error'); }
    finally { state.adding = false; renderTelegramChannelControls(); }
}

async function changeTelegramChannel(id, action) {
    const state = telegramChannelsState;
    id = telegramChannelId(id);
    const channel = state.channels.find(item => String(item.channel_id) === id);
    if (!channel || state.changing.has(id) || state.saving.has(id)) return;
    const running = !!channel.enabled && !!channel.auto_sync;
    if (action !== 'remove' && !running) {
        const draft = getTelegramChannelDraft(channel);
        let validPath = false;
        try { validPath = !!normalizeTelegramChannelPath(channel.target_path); } catch (_) { /* Focus the saved path below. */ }
        const validSince = !!channel.sync_since && !Number.isNaN(new Date(channel.sync_since).getTime());
        if (draft?.dirty || !validPath || !validSince) {
            const selecting = state.channelId !== id ? selectTelegramChannel(id) : null;
            const currentDraft = getTelegramChannelDraft(channel);
            currentDraft.message = '请先保存目标文件夹和同步时间，再启动监控';
            currentDraft.messageKind = 'error';
            renderTelegramChannelSettings();
            const focusId = !validPath || currentDraft.targetPath !== currentDraft.baseline.targetPath ? 'telegramChannelTargetPath' : 'telegramChannelSyncSince';
            document.getElementById(focusId)?.focus();
            if (selecting) await selecting;
            return;
        }
    }
    if (action === 'remove' && !confirm(`移除频道“${channel.title || id}”的监控和视频列表？已经提交的下载任务会保留，源频道消息不会删除。`)) return;
    state.changing.add(id);
    renderTelegramChannels();
    try {
        const data = await telegramChannelsRequest(`/${id}`, action === 'remove' ? { method: 'DELETE' } : telegramChannelJson('PATCH', { enabled: !running, auto_sync: !running }));
        state.channelsRequest += 1;
        state.settingsVersions.set(id, (state.settingsVersions.get(id) || 0) + 1);
        if (action === 'remove') {
            state.drafts.delete(id);
            state.channels = state.channels.filter(item => String(item.channel_id) !== id);
            if (state.channelId === id) await selectTelegramChannel(state.channels[0]?.channel_id || '');
        } else {
            if (!data.channel || String(data.channel.channel_id) !== id) throw new Error('操作响应无效，请刷新后确认监控状态');
            state.channels = state.channels.map(item => String(item.channel_id) === id ? { ...item, ...data.channel } : item);
            const draft = getTelegramChannelDraft(data.channel);
            if (draft) {
                draft.message = data.channel.enabled && data.channel.auto_sync ? '监控已启动' : '监控已暂停';
                draft.messageKind = 'success';
            }
        }
    } catch (error) {
        const draft = getTelegramChannelDraft(channel);
        if (draft) { draft.message = error.message; draft.messageKind = 'error'; }
        if (state.channelId !== id) setTelegramChannelsNotice(`${channel.title || id}：${error.message}`, 'error');
    }
    finally { state.changing.delete(id); renderTelegramChannels(); }
}

async function scanTelegramChannel(older = false) {
    const state = telegramChannelsState;
    const id = state.channelId;
    const epoch = state.selectionEpoch;
    const settingsVersion = state.settingsVersions.get(id) || 0;
    if (!id || state.scanning.has(id)) return;
    state.scanning.add(id);
    renderTelegramChannelControls();
    setTelegramChannelsNotice(older ? '正在扫描更早的视频…' : '正在扫描最新视频…');
    try {
        const data = await telegramChannelsRequest(`/${id}/scan`, telegramChannelJson('POST', { older }));
        if (state.channelId !== id || state.selectionEpoch !== epoch) return;
        if (data.channel && (state.settingsVersions.get(id) || 0) === settingsVersion) state.channels = state.channels.map(channel => String(channel.channel_id) === id ? data.channel : channel);
        if (older && state.videos.length) {
            state.nextBeforeId = Math.min(...state.videos.map(video => Number(video.message_id)));
            await loadTelegramChannelVideos(true);
        } else await loadTelegramChannelVideos(false, true);
        if (state.channelId === id && state.selectionEpoch === epoch) setTelegramChannelsNotice(`扫描完成，新收录 ${Number(data.added) || 0} 个视频。${data.channel?.history_complete ? '频道历史已扫描完。' : ''}`);
    } catch (error) { if (state.channelId === id && state.selectionEpoch === epoch) setTelegramChannelsNotice(error.message, 'error'); }
    finally { state.scanning.delete(id); renderTelegramChannels(); }
}

async function downloadTelegramChannelVideos() {
    const state = telegramChannelsState;
    const channelId = state.channelId;
    const epoch = state.selectionEpoch;
    if (!channelId || state.submitting || !state.selected.size) return;
    const messageIds = [...state.selected].slice(0, TELEGRAM_VIDEO_SELECTION_LIMIT);
    state.submitting = true;
    syncTelegramVideoSelection();
    try {
        const data = await telegramChannelsRequest(`/${channelId}/download`, telegramChannelJson('POST', { message_ids: messageIds }));
        if (state.channelId !== channelId || state.selectionEpoch !== epoch) return;
        for (const result of data.results || []) {
            const id = Number(result.message_id);
            const video = state.videos.find(item => Number(item.message_id) === id);
            if (result.success) {
                state.selected.delete(id);
                if (video && (!video.job_status || video.job_status === 'pending')) Object.assign(video, { job_id: result.job_id, job_status: result.status || 'pending', job_error: '' });
            } else if (video) video.job_error = result.error || '提交失败，请重试';
        }
        const failed = Number(data.failed) || 0;
        setTelegramChannelsNotice(`已提交 ${Number(data.queued) || 0} 项。${failed ? `${failed} 项未能提交，请查看视频下方的错误。` : '可在“回源与下载任务”中查看进度。'}`, failed ? 'error' : '');
        renderTelegramChannelVideos();
        if (typeof loadT2TDRelayJobs === 'function') loadT2TDRelayJobs(true);
    } catch (error) { if (state.channelId === channelId && state.selectionEpoch === epoch) setTelegramChannelsNotice(error.message, 'error'); }
    finally { state.submitting = false; syncTelegramVideoSelection(); }
}

function updateTelegramChannelJob(job) {
    const state = telegramChannelsState;
    if (!job || String(job.source_channel_id) !== state.channelId) return;
    const video = state.videos.find(item => Number(item.message_id) === Number(job.source_message_id));
    if (!video) return;
    Object.assign(video, { job_id: job.job_id, job_status: job.status, job_error: job.error || '', download_progress: job.download_progress, upload_progress: job.upload_progress });
    if (state.mode === 'channels') renderTelegramChannelVideos();
}

function initTelegramChannels() {
    document.getElementById('telegramChannelAddForm')?.addEventListener('submit', addTelegramChannel);
    document.getElementById('telegramChannelSettingsForm')?.addEventListener('submit', saveTelegramChannelSettings);
    for (const id of ['telegramChannelTargetPath', 'telegramChannelSyncSince']) {
        const input = document.getElementById(id);
        input?.addEventListener('input', editTelegramChannelSettings);
        input?.addEventListener('change', editTelegramChannelSettings);
    }
    document.getElementById('telegramChannelsList')?.addEventListener('click', event => {
        const button = event.target.closest('[data-channel-action]');
        if (!button || button.disabled) return;
        if (button.dataset.channelAction === 'select') selectTelegramChannel(button.dataset.channelId);
        else changeTelegramChannel(button.dataset.channelId, button.dataset.channelAction);
    });
    document.getElementById('telegramChannelVideos')?.addEventListener('change', event => {
        const input = event.target.closest('[data-message-id]');
        if (!input || telegramChannelsState.submitting) return;
        const id = Number(input.dataset.messageId);
        const video = telegramChannelsState.videos.find(item => Number(item.message_id) === id);
        if (!video || !telegramVideoSelectable(video)) return;
        if (input.checked && telegramChannelsState.selected.size >= TELEGRAM_VIDEO_SELECTION_LIMIT) {
            input.checked = false;
            setTelegramChannelsNotice(`每次最多选择 ${TELEGRAM_VIDEO_SELECTION_LIMIT} 项，请分批提交。`);
        } else if (input.checked) telegramChannelsState.selected.add(id);
        else telegramChannelsState.selected.delete(id);
        syncTelegramVideoSelection();
    });
    document.querySelector('.telegram-module-tabs')?.addEventListener('keydown', event => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault();
        const mode = event.key === 'Home' ? 'tasks' : event.key === 'End' ? 'channels' : telegramChannelsState.mode === 'tasks' ? 'channels' : 'tasks';
        setTelegramModuleMode(mode);
        document.getElementById(mode === 'tasks' ? 'telegramTasksTab' : 'telegramChannelsTab')?.focus();
    });
    renderTelegramChannelVideos();
    telegramChannelsState.pollTimer = setInterval(async () => {
        if (document.hidden || telegramChannelsState.mode !== 'channels' || !document.getElementById('page-telegramrelay')?.classList.contains('active')) return;
        if (telegramChannelsState.loading || telegramChannelsState.submitting || telegramChannelsState.scanning.size) return;
        await loadTelegramChannelsPage(true, true);
        await loadTelegramChannelVideos(false, true);
    }, 15000);
}

document.addEventListener('DOMContentLoaded', initTelegramChannels);
