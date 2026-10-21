// Channel monitoring lists metadata only. Downloads always require explicit selection.
const telegramChannelsState = {
    mode: 'tasks', channels: [], channelId: '', videos: [], selected: new Set(),
    hasMore: false, nextBeforeId: null, loaded: false, loading: false,
    channelsRequest: 0, videosRequest: 0, selectionEpoch: 0, adding: false, submitting: false,
    scanning: new Set(), changing: new Set(), cooldownUntil: null, pollTimer: null,
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
    return !video.job_status && !video.job_id;
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
    if (node) node.textContent = window.currentConfig?.telegram_relay?.target_path || window.currentConfig?.teldrive?.target_path || '默认上传目录';
}

function setTelegramModuleMode(mode) {
    telegramChannelsState.mode = mode === 'channels' ? 'channels' : 'tasks';
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
        const disabled = state.changing.has(id) ? ' disabled' : '';
        return `<article class="telegram-channel-item${active ? ' active' : ''}">
            <button type="button" id="telegram-channel-${id}" class="telegram-channel-open" data-channel-action="select" data-channel-id="${id}" aria-pressed="${active}"><strong>${telegramChannelEscape(channel.title || channel.username || id)}</strong><span class="telegram-channel-hint">${telegramChannelEscape(channel.username ? '@' + channel.username : id)}</span></button>
            <div class="telegram-channel-item-meta"><span class="telegram-channel-badge ${channel.enabled ? 'success' : ''}">${channel.enabled ? '监控中' : '已暂停'}</span><span class="telegram-channel-hint">${Math.max(0, Number(channel.video_count) || 0)} 个视频</span>${channel.protected ? '<span class="telegram-channel-badge warning">受保护频道</span>' : ''}</div>
            ${channel.last_error ? `<p class="telegram-channel-hint telegram-channel-item-error">${telegramChannelEscape(channel.last_error)}</p>` : ''}
            <div class="telegram-channel-actions"><button id="telegram-channel-toggle-${id}" type="button" class="btn btn-ghost btn-sm" data-channel-action="toggle" data-channel-id="${id}"${disabled}>${channel.enabled ? '暂停监控' : '继续监控'}</button><button id="telegram-channel-remove-${id}" type="button" class="btn btn-ghost btn-sm telegram-channel-remove" data-channel-action="remove" data-channel-id="${id}"${disabled}>移除</button></div>
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
    if (title) title.textContent = channel ? (channel.title || channel.username || state.channelId) : '频道视频';
    const summary = document.getElementById('telegramChannelSummary');
    if (summary) summary.textContent = channel
        ? `${channel.enabled ? '自动收录新视频' : '自动监控已暂停'} · 已收录 ${Number(channel.video_count) || 0} 项${channel.last_scan ? ' · 上次扫描 ' + telegramChannelDate(channel.last_scan) : ''}`
        : '添加或选择一个频道';
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
}

function renderTelegramVideoRow(video) {
    const id = Number(video.message_id);
    if (!Number.isSafeInteger(id) || id <= 0) return '';
    const state = telegramChannelsState;
    const selected = state.selected.has(id);
    const selectable = telegramVideoSelectable(video);
    const [status, statusClass] = TELEGRAM_VIDEO_STATUS[video.job_status] || [video.job_status ? '处理中' : '未下载', ''];
    const link = telegramChannelSourceLink(video.link);
    const rawProgress = video.job_status === 'uploading' ? video.upload_progress : video.download_progress;
    const progress = ['uploading', 'downloading'].includes(video.job_status) ? ` ${Math.min(100, Math.max(0, Number(rawProgress) || 0)).toFixed(1)}%` : '';
    return `<article class="telegram-video-row${selected ? ' selected' : ''}" data-video-row="${id}">
        <input type="checkbox" class="telegram-video-check" id="telegram-video-${id}" data-message-id="${id}"${selected ? ' checked' : ''}${!selectable || state.submitting ? ' disabled' : ''}>
        <div class="telegram-video-content"><label for="telegram-video-${id}" class="telegram-video-title">${telegramChannelEscape(video.file_name || '视频 #' + id)}</label>
            <div class="telegram-video-meta"><span>${telegramVideoSize(video.file_size)}</span><span>${telegramVideoDuration(video.duration)}</span><span>${telegramChannelEscape(telegramChannelDate(video.date))}</span><span>#${id}</span></div>
            ${video.caption ? `<p class="telegram-video-caption">${telegramChannelEscape(video.caption)}</p>` : ''}
            <div class="telegram-video-footer"><span class="telegram-channel-badge ${statusClass}">${status}${progress}</span>${video.protected ? '<span class="telegram-channel-badge warning"><i class="ph ph-lock-simple"></i> 内容受保护</span>' : ''}${link ? `<a href="${telegramChannelEscape(link)}" target="_blank" rel="noopener noreferrer">查看源消息 <i class="ph ph-arrow-up-right"></i></a>` : ''}${['failed', 'cancelled'].includes(video.job_status) ? '<button type="button" class="btn btn-ghost btn-sm" onclick="setTelegramModuleMode(\'tasks\')">在任务列表重试</button>' : ''}</div>
            ${video.job_error ? `<p class="telegram-channel-hint telegram-video-error">${telegramChannelEscape(video.job_error)}</p>` : ''}
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
        if (data.channel) state.channels = state.channels.map(channel => String(channel.channel_id) === channelId ? data.channel : channel);
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
    if (!channel || state.changing.has(id)) return;
    if (action === 'remove' && !confirm(`移除频道“${channel.title || id}”的监控和视频列表？已经提交的下载任务会保留，源频道消息不会删除。`)) return;
    state.changing.add(id);
    renderTelegramChannels();
    try {
        await telegramChannelsRequest(`/${id}`, action === 'remove' ? { method: 'DELETE' } : telegramChannelJson('PATCH', { enabled: !channel.enabled }));
        await loadTelegramChannelsPage(true);
    } catch (error) { setTelegramChannelsNotice(error.message, 'error'); }
    finally { state.changing.delete(id); renderTelegramChannels(); }
}

async function scanTelegramChannel(older = false) {
    const state = telegramChannelsState;
    const id = state.channelId;
    const epoch = state.selectionEpoch;
    if (!id || state.scanning.has(id)) return;
    state.scanning.add(id);
    renderTelegramChannelControls();
    setTelegramChannelsNotice(older ? '正在扫描更早的视频…' : '正在扫描最新视频…');
    try {
        const data = await telegramChannelsRequest(`/${id}/scan`, telegramChannelJson('POST', { older }));
        if (state.channelId !== id || state.selectionEpoch !== epoch) return;
        if (data.channel) state.channels = state.channels.map(channel => String(channel.channel_id) === id ? data.channel : channel);
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
