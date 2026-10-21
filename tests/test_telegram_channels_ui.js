const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const { execFileSync } = require('node:child_process');

const source = fs.readFileSync(path.join(__dirname, '../app/static/telegram-channels.js'), 'utf8');

function setup() {
    const nodes = {};
    const document = {
        activeElement: null, hidden: false,
        getElementById(id) {
            if (!nodes[id]) {
                const classes = new Set();
                nodes[id] = {
                    id, innerHTML: '', textContent: '', value: '', hidden: false, disabled: false, checked: false,
                    classList: {
                        contains: value => classes.has(value),
                        toggle(value, enabled) { if (enabled) classes.add(value); else classes.delete(value); },
                    },
                    setAttribute() {}, addEventListener() {}, focus() { document.activeElement = this; },
                };
            }
            return nodes[id];
        },
        querySelector() { return null; }, querySelectorAll() { return []; }, addEventListener() {},
    };
    const context = vm.createContext({ document, window: { currentConfig: { telegram_relay: { target_path: '/视频' } } }, URL, Date, confirm: () => true, setInterval() {}, fetch: async () => response({}) });
    vm.runInContext(source + '\nglobalThis.state = telegramChannelsState;', context);
    context.state.channels = [{ channel_id: -1001, title: '频道 A', enabled: true }, { channel_id: -1002, title: '频道 B', enabled: true }];
    return { context, nodes, document, state: context.state };
}

function response(data, status = 200, retry = null) {
    return { ok: status >= 200 && status < 300, status, json: async () => data, headers: { get: () => retry } };
}

function deferred() {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return { promise, resolve };
}

const video = (message_id, extra = {}) => ({ channel_id: -1001, message_id, file_name: `视频 ${message_id}.mp4`, file_size: 1024, ...extra });

test('opening and switching channels fetches metadata without submitting downloads', async () => {
    const { context, state } = setup();
    const requests = [];
    context.fetch = async (url, options) => {
        requests.push({ url, options });
        return response(url.includes('/videos') ? { videos: [video(12)], has_more: false } : { channels: state.channels });
    };
    await context.loadTelegramChannelsPage();
    await context.selectTelegramChannel(-1002);
    assert.equal(state.channelId, '-1002');
    assert.equal(requests.length, 3);
    assert.ok(requests.every(item => !item.options.method || item.options.method === 'GET'));
    assert.equal(state.selected.size, 0);
});

test('adding a channel scans metadata immediately and keeps any scan error visible', async () => {
    for (const scanFails of [false, true]) {
        const { context, state, document, nodes } = setup();
        document.getElementById('telegramChannelReference').value = '@example';
        const requests = [];
        context.fetch = async (url, options) => {
            requests.push({ url, options });
            if (url.endsWith('/scan')) return response(scanFails ? { detail: '频道视频暂时不可读取' } : { added: 2, channel: state.channels[0] }, scanFails ? 403 : 200);
            if (options.method === 'POST') return response({ channel: state.channels[0] });
            if (url.includes('/videos')) return response({ videos: [video(1)], has_more: false });
            return response({ channels: state.channels });
        };
        await context.addTelegramChannel({ preventDefault() {} });
        assert.equal(requests.filter(item => item.url.endsWith('/scan')).length, 1);
        assert.equal(requests.filter(item => item.url.endsWith('/download')).length, 0);
        assert.equal(state.selected.size, 0);
        assert.equal(state.adding, false);
        assert.match(nodes.telegramChannelsNotice.textContent, scanFails ? /频道视频暂时不可读取/ : /新收录 2 个视频/);
    }
});

test('late success and failure from a previously selected channel cannot replace the current videos', async () => {
    for (const oldResponse of [response({ videos: [video(1)] }), response({ detail: 'A 错误' }, 500)]) {
        const { context, state, nodes } = setup();
        const slow = deferred();
        context.fetch = url => url.includes('/-1001/') ? slow.promise : Promise.resolve(response({ videos: [video(22)] }));
        const first = context.selectTelegramChannel(-1001);
        await context.selectTelegramChannel(-1002);
        slow.resolve(oldResponse);
        await first;
        assert.equal(state.channelId, '-1002');
        assert.deepEqual(Array.from(state.videos, item => item.message_id), [22]);
        assert.equal(state.loading, false);
        assert.doesNotMatch(nodes.telegramChannelsNotice.textContent, /A 错误/);
    }
});

test('pagination keeps chosen videos and refresh merges new metadata without duplicating rows', async () => {
    const { context, state } = setup();
    state.channelId = '-1001';
    let page = 0;
    const requests = [];
    context.fetch = async url => {
        requests.push(url);
        page += 1;
        return response(page === 1 ? { videos: [video(10)], has_more: true, next_before_id: 10 } : page === 2 ? { videos: [video(9)], has_more: false } : { videos: [video(11), video(10, { file_name: '新版名称' })], has_more: true, next_before_id: 10 });
    };
    await context.loadTelegramChannelVideos();
    state.selected.add(10);
    await context.loadTelegramChannelVideos(true);
    await context.loadTelegramChannelVideos(false, true);
    assert.match(requests[1], /before_id=10/);
    assert.deepEqual(Array.from(state.videos, item => item.message_id), [11, 10, 9]);
    assert.equal(state.videos[1].file_name, '新版名称');
    assert.ok(state.selected.has(10));
});

test('a burst of new videos leaves a cursor to load the gap above older selections', async () => {
    const { context, state } = setup();
    state.channelId = '-1001';
    state.videos = [video(1)];
    state.selected.add(1);
    state.hasMore = false;
    context.fetch = async () => response({ videos: Array.from({ length: 50 }, (_, index) => video(100 - index)), has_more: true, next_before_id: 51 });
    await context.loadTelegramChannelVideos(false, true);
    assert.equal(state.hasMore, true);
    assert.equal(state.nextBeforeId, 51);
    assert.equal(state.videos.length, 51);
    assert.ok(state.selected.has(1));
});

test('only selected available videos submit; duplicate clicks are suppressed and partial failures remain selected', async () => {
    const { context, state, nodes } = setup();
    state.channelId = '-1001';
    state.videos = [video(10), video(11), video(12, { job_status: 'completed' }), video(13, { job_status: 'downloading' }), video(14, { job_status: 'failed', job_id: 'failed-task' })];
    context.selectTelegramChannelVideos(true);
    assert.deepEqual(Array.from(state.selected), [10, 11]);
    const slow = deferred();
    const requests = [];
    context.fetch = async (url, options) => { requests.push({ url, options }); return slow.promise; };
    const first = context.downloadTelegramChannelVideos();
    await context.downloadTelegramChannelVideos();
    assert.equal(requests.length, 1);
    assert.deepEqual(JSON.parse(requests[0].options.body), { message_ids: [10, 11] });
    slow.resolve(response({ results: [{ message_id: 10, success: true, job_id: 'job10', status: 'pending' }, { message_id: 11, success: false, error: '文件不可访问' }], queued: 1, failed: 1 }));
    await first;
    assert.deepEqual(Array.from(state.selected), [11]);
    assert.equal(state.videos[1].job_error, '文件不可访问');
    assert.equal(state.submitting, false);
    assert.match(nodes.telegramChannelsNotice.textContent, /1 项未能提交/);
    assert.match(context.renderTelegramVideoRow(state.videos[4]), /在任务列表重试/);
});

test('a completed submission cannot replace videos or selection after switching channels', async () => {
    const { context, state } = setup();
    state.channelId = '-1001';
    state.videos = [video(10)];
    state.selected.add(10);
    const slow = deferred();
    context.fetch = (url, options) => options.method === 'POST' ? slow.promise : Promise.resolve(response({ videos: [video(10, { file_name: 'B 视频' })] }));
    const submitting = context.downloadTelegramChannelVideos();
    await context.selectTelegramChannel(-1002);
    slow.resolve(response({ results: [{ message_id: 10, success: true, job_id: 'A-task' }], queued: 1 }));
    await submitting;
    assert.equal(state.videos[0].file_name, 'B 视频');
    assert.equal(state.videos[0].job_id, undefined);
    assert.equal(state.selected.size, 0);
    assert.equal(state.submitting, false);
});

test('titles, captions and errors are escaped and source links cannot execute scripts', () => {
    const { context } = setup();
    const html = context.renderTelegramVideoRow(video(2, { file_name: '<img src=x onerror=alert(1)>', caption: '</p><script>bad()</script>', job_error: '<svg/onload=alert(1)>', link: 'javascript:alert(1)', protected: true }));
    assert.doesNotMatch(html, /<script>|<img |<svg|href=/);
    assert.match(html, /&lt;script&gt;/);
    assert.match(html, /内容受保护/);
    assert.equal(context.telegramChannelSourceLink('https://t.me.evil.example/1'), '');
    assert.equal(context.telegramChannelSourceLink('https://evil@t.me/channel/1'), '');
    assert.equal(context.telegramChannelSourceLink('https://t.me/channel/1'), 'https://t.me/channel/1');
});

test('a stale submission is ignored after switching away and back to the same channel', async () => {
    const { context, state } = setup();
    state.channelId = '-1001';
    state.videos = [video(10)];
    state.selected.add(10);
    const slow = deferred();
    context.fetch = (url, options) => options.method === 'POST' ? slow.promise : Promise.resolve(response({ videos: [video(10, { job_status: 'completed', job_id: 'finished' })] }));
    const submitting = context.downloadTelegramChannelVideos();
    await context.selectTelegramChannel(-1002);
    await context.selectTelegramChannel(-1001);
    slow.resolve(response({ results: [{ message_id: 10, success: true, job_id: 'old', status: 'pending' }], queued: 1 }));
    await submitting;
    assert.equal(state.videos[0].job_status, 'completed');
    assert.equal(state.videos[0].job_id, 'finished');
});

test('a submission result cannot downgrade a task already completed by a live event', async () => {
    const { context, state } = setup();
    state.channelId = '-1001';
    state.mode = 'channels';
    state.videos = [video(10)];
    state.selected.add(10);
    const slow = deferred();
    context.fetch = () => slow.promise;
    const submitting = context.downloadTelegramChannelVideos();
    context.updateTelegramChannelJob({ source_channel_id: -1001, source_message_id: 10, job_id: 'task', status: 'completed' });
    slow.resolve(response({ results: [{ message_id: 10, success: true, job_id: 'task', status: 'pending' }], queued: 1 }));
    await submitting;
    assert.equal(state.videos[0].job_status, 'completed');
    assert.equal(state.selected.size, 0);
});

test('select all is bounded and live task events retire completed choices', () => {
    const { context, state } = setup();
    state.channelId = '-1001';
    state.mode = 'channels';
    state.videos = Array.from({ length: 120 }, (_, index) => video(index + 1));
    context.selectTelegramChannelVideos(true);
    assert.equal(state.selected.size, 100);
    context.updateTelegramChannelJob({ source_channel_id: -1001, source_message_id: 1, job_id: 'x', status: 'completed' });
    assert.equal(state.selected.size, 99);
    assert.equal(state.videos[0].job_status, 'completed');
    context.updateTelegramChannelJob({ source_channel_id: -1002, source_message_id: 2, status: 'completed' });
    assert.equal(state.videos[1].job_status, undefined);
});

test('Telegram rate limits show the retry time and disable scans until cooldown', async () => {
    const { context, state, nodes } = setup();
    state.channelId = '-1001';
    context.fetch = async () => response({ detail: 'Telegram 请求频率过高' }, 429, '60');
    await context.scanTelegramChannel();
    assert.match(nodes.telegramChannelsNotice.textContent, /60 秒/);
    assert.ok(new Date(state.cooldownUntil).getTime() > Date.now());
    assert.equal(nodes.telegramChannelScanBtn.disabled, true);
    assert.equal(state.scanning.size, 0);
});

function settingsChannel(id = -1001, extra = {}) {
    return { channel_id: id, title: `频道 ${id}`, enabled: true, target_path: '/原目录', auto_sync: false, sync_since: '2026-10-21T06:15:16Z', sync_status: 'manual', ...extra };
}

function fillSettings(context, document, values = {}) {
    context.renderTelegramChannelSettings();
    if ('targetPath' in values) document.getElementById('telegramChannelTargetPath').value = values.targetPath;
    if ('localDate' in values) document.getElementById('telegramChannelSyncSince').value = values.localDate;
    context.editTelegramChannelSettings();
}

test('local datetime inputs round trip with seconds through UTC without assuming the browser is UTC', () => {
    const functions = source.slice(source.indexOf('function telegramChannelLocalDate('), source.indexOf('function telegramChannelSettingsValues('));
    const script = functions + `console.log(JSON.stringify([telegramChannelUTCDate('2026-10-21T14:15:16'), telegramChannelLocalDate('2026-10-21T06:15:16.000Z')]))`;
    const output = execFileSync(process.execPath, ['-e', script], { encoding: 'utf8', env: { ...process.env, TZ: 'Asia/Shanghai' } });
    assert.deepEqual(JSON.parse(output), ['2026-10-21T06:15:16.000Z', '2026-10-21T14:15:16']);
    const { context } = setup();
    assert.throws(() => context.telegramChannelUTCDate('2026-02-30T12:00:00'), /有效/);
    assert.throws(() => context.telegramChannelUTCDate('invalid'), /有效/);
});

test('unsaved channel settings survive polling and switching away and back without enabling automatic downloads', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(), settingsChannel(-1002, { target_path: '/频道B' })];
    state.channelId = '-1001';
    let mutations = 0;
    context.fetch = async (url, options) => {
        if (options.method) mutations += 1;
        return response(url.includes('/videos') ? { videos: [video(1)] } : { channels: state.channels.map(channel => ({ ...channel })) });
    };
    fillSettings(context, document, { targetPath: '/我的草稿', localDate: '2026-10-20T11:12:13' });
    await context.loadTelegramChannelsPage(true);
    assert.equal(document.getElementById('telegramChannelTargetPath').value, '/我的草稿');
    await context.selectTelegramChannel(-1002);
    assert.equal(document.getElementById('telegramChannelTargetPath').value, '/频道B');
    await context.selectTelegramChannel(-1001);
    assert.equal(document.getElementById('telegramChannelTargetPath').value, '/我的草稿');
    assert.equal(document.getElementById('telegramChannelSyncSince').value, '2026-10-20T11:12:13');
    assert.equal(state.drafts.get('-1001').dirty, true);
    assert.equal(state.channels[0].auto_sync, false);
    assert.equal(mutations, 0);
});

test('saving settings sends only this channel draft and suppresses repeated saves', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel()];
    state.channelId = '-1001';
    fillSettings(context, document, { targetPath: '/目标频道', localDate: '2026-10-20T11:12:13' });
    const requests = [];
    const pending = deferred();
    context.fetch = (url, options) => { requests.push({ url, options }); return pending.promise; };
    const saving = context.saveTelegramChannelSettings();
    await context.saveTelegramChannelSettings();
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, '/api/t2td/channels/-1001');
    assert.equal(requests[0].options.method, 'PATCH');
    assert.deepEqual(JSON.parse(requests[0].options.body), { target_path: '/目标频道', sync_since: new Date('2026-10-20T11:12:13').toISOString() });
    assert.equal(document.getElementById('telegramVideosDownloadBtn').title, '上传到 TelDrive：/原目录');
    pending.resolve(response({ channel: settingsChannel(-1001, { target_path: '/目标频道', sync_since: new Date('2026-10-20T11:12:13').toISOString() }) }));
    await saving;
    assert.equal(state.drafts.get('-1001').dirty, false);
    assert.equal(state.saving.size, 0);
    assert.equal(document.getElementById('telegramVideosDownloadBtn').title, '上传到 TelDrive：/目标频道');
    assert.equal(state.channels[0].auto_sync, false);
});

test('starting requires saved folder and time and never silently saves a draft', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(-1001, { target_path: '' })];
    state.channelId = '-1001';
    let requests = 0;
    context.fetch = async () => { requests += 1; return response({}); };
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.equal(document.getElementById('telegramChannelSettingsStatus').textContent, '请先保存目标文件夹和同步时间，再启动监控');
    assert.equal(document.activeElement.id, 'telegramChannelTargetPath');
    state.channels[0].target_path = '/已保存目录';
    state.channels[0].sync_since = '';
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.equal(document.activeElement.id, 'telegramChannelSyncSince');
    state.channels[0].sync_since = '2026-10-21T06:15:16Z';
    fillSettings(context, document, { targetPath: '/未保存目录' });
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.equal(document.activeElement.id, 'telegramChannelTargetPath');
    assert.equal(requests, 0);
    assert.equal(state.drafts.get('-1001').dirty, true);
    assert.equal(state.channels[0].auto_sync, false);
});

test('the left control starts and pauses both flags, including legacy metadata-only channels', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel()];
    state.channelId = '-1001';
    const payloads = [];
    context.fetch = async (url, options) => {
        assert.equal(url, '/api/t2td/channels/-1001');
        const payload = JSON.parse(options.body);
        payloads.push(payload);
        return response({ channel: { ...state.channels[0], ...payload, sync_status: payload.auto_sync ? 'watching' : 'manual' } });
    };
    context.renderTelegramChannels();
    assert.equal(context.telegramChannelSyncStatus(state.channels[0])[0], '未启动');
    assert.match(document.getElementById('telegramChannelsList').innerHTML, />启动监控<\/button>/);
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.deepEqual(payloads[0], { enabled: true, auto_sync: true });
    assert.equal(state.channels[0].auto_sync && state.channels[0].enabled, true);
    assert.match(document.getElementById('telegramChannelsList').innerHTML, />暂停监控<\/button>/);
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.deepEqual(payloads[1], { enabled: false, auto_sync: false });
    assert.equal(state.channels[0].auto_sync || state.channels[0].enabled, false);
    state.channels[0] = settingsChannel(-1001, { enabled: false, auto_sync: true, sync_status: 'paused' });
    context.renderTelegramChannels();
    assert.match(document.getElementById('telegramChannelsList').innerHTML, />继续监控<\/button>/);
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.deepEqual(payloads[2], { enabled: true, auto_sync: true });
});

test('starting another channel with missing settings selects its inputs and preserves the previous draft', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(-1001, { target_path: '' }), settingsChannel(-1002)];
    state.channelId = '-1002';
    fillSettings(context, document, { targetPath: '/B未保存目录' });
    let writes = 0;
    context.fetch = async (url, options) => {
        if (options.method) writes += 1;
        return response({ videos: [video(1)] });
    };
    await context.changeTelegramChannel(-1001, 'toggle');
    assert.equal(state.channelId, '-1001');
    assert.equal(document.activeElement.id, 'telegramChannelTargetPath');
    assert.equal(document.getElementById('telegramChannelSettingsStatus').textContent, '请先保存目标文件夹和同步时间，再启动监控');
    assert.equal(state.drafts.get('-1002').targetPath, '/B未保存目录');
    assert.equal(state.drafts.get('-1002').dirty, true);
    assert.equal(writes, 0);
});

test('a completed start survives A to B to A and stale list and video snapshots', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(), settingsChannel(-1002)];
    state.channelId = '-1001';
    state.videos = [video(1)];
    const starting = deferred();
    const oldList = deferred();
    const oldVideos = deferred();
    const initialChannels = state.channels.map(channel => ({ ...channel }));
    let writes = 0;
    context.fetch = (url, options) => {
        if (options.method === 'PATCH') { writes += 1; return starting.promise; }
        if (url === '/api/t2td/channels') return oldList.promise;
        if (url.includes('/-1001/')) return oldVideos.promise;
        return Promise.resolve(response({ videos: [video(2)] }));
    };
    const changing = context.changeTelegramChannel(-1001, 'toggle');
    await context.changeTelegramChannel(-1001, 'toggle');
    const listing = context.loadTelegramChannelsPage(true);
    await context.selectTelegramChannel(-1002);
    const selecting = context.selectTelegramChannel(-1001);
    starting.resolve(response({ channel: settingsChannel(-1001, { auto_sync: true, enabled: true, sync_status: 'watching' }) }));
    await changing;
    oldList.resolve(response({ channels: initialChannels }));
    oldVideos.resolve(response({ channel: settingsChannel(), videos: [video(3)] }));
    await Promise.all([listing, selecting]);
    assert.equal(writes, 1);
    assert.equal(state.channelId, '-1001');
    assert.equal(state.channels[0].enabled && state.channels[0].auto_sync, true);
    assert.equal(state.channels[1].auto_sync, false);
    assert.equal(state.videos[0].message_id, 3);
    assert.match(document.getElementById('telegramChannelsList').innerHTML, />暂停监控<\/button>/);
});

test('saving a paused automatic channel says it remains paused', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(-1001, { enabled: false, auto_sync: true, sync_status: 'paused' })];
    state.channelId = '-1001';
    fillSettings(context, document, { targetPath: '/暂停时修改目录' });
    context.fetch = async (url, options) => {
        const payload = JSON.parse(options.body);
        assert.equal('auto_sync' in payload, false);
        assert.equal('enabled' in payload, false);
        return response({ channel: settingsChannel(-1001, { ...payload, enabled: false, auto_sync: true, sync_status: 'paused' }) });
    };
    await context.saveTelegramChannelSettings();
    assert.equal(document.getElementById('telegramChannelSettingsStatus').textContent, '已保存，监控仍暂停');
    assert.equal(state.channels[0].enabled, false);
    assert.equal(state.channels[0].auto_sync, true);
});

test('clearing an unused start time omits it instead of sending invalid empty ISO dates', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel()];
    state.channelId = '-1001';
    fillSettings(context, document, { targetPath: '/手动目录', localDate: '' });
    let payload;
    context.fetch = async (url, options) => {
        payload = JSON.parse(options.body);
        return response({ channel: settingsChannel(-1001, { target_path: payload.target_path }) });
    };
    await context.saveTelegramChannelSettings();
    assert.equal('sync_since' in payload, false);
    assert.equal('auto_sync' in payload, false);
    assert.equal('enabled' in payload, false);
});

test('failed settings save retains the draft for retry and never changes the effective destination', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel()];
    state.channelId = '-1001';
    fillSettings(context, document, { targetPath: '/不存在' });
    context.fetch = async () => response({ detail: 'TelDrive 文件夹不存在' }, 400);
    await context.saveTelegramChannelSettings();
    assert.equal(state.drafts.get('-1001').dirty, true);
    assert.equal(document.getElementById('telegramChannelTargetPath').value, '/不存在');
    assert.equal(document.getElementById('telegramVideosDownloadBtn').title, '上传到 TelDrive：/原目录');
    assert.match(document.getElementById('telegramChannelSettingsStatus').textContent, /文件夹不存在/);
    assert.equal(document.getElementById('telegramChannelSettingsSaveBtn').disabled, false);
});

test('a late settings save for A cannot overwrite B settings or B draft', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(), settingsChannel(-1002, { target_path: '/B目录' })];
    state.channelId = '-1001';
    fillSettings(context, document, { targetPath: '/A已保存' });
    const pending = deferred();
    context.fetch = (url, options) => options.method === 'PATCH' ? pending.promise : Promise.resolve(response({ videos: [video(1)] }));
    const saving = context.saveTelegramChannelSettings();
    await context.selectTelegramChannel(-1002);
    fillSettings(context, document, { targetPath: '/B未保存' });
    pending.resolve(response({ channel: settingsChannel(-1001, { target_path: '/A已保存' }) }));
    await saving;
    assert.equal(state.channelId, '-1002');
    assert.equal(document.getElementById('telegramChannelTargetPath').value, '/B未保存');
    assert.equal(document.getElementById('telegramVideosDownloadBtn').title, '上传到 TelDrive：/B目录');
    assert.equal(state.drafts.get('-1002').dirty, true);
});

test('saving after A to B to A invalidates old embedded settings without losing current videos', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(), settingsChannel(-1002)];
    state.channelId = '-1001';
    state.videos = [video(1)];
    fillSettings(context, document, { targetPath: '/A新目录' });
    const pendingSave = deferred();
    const pendingVideos = deferred();
    context.fetch = (url, options) => options.method === 'PATCH' ? pendingSave.promise : url.includes('/-1001/') ? pendingVideos.promise : Promise.resolve(response({ videos: [video(2)] }));
    const saving = context.saveTelegramChannelSettings();
    await context.selectTelegramChannel(-1002);
    const selectingA = context.selectTelegramChannel(-1001);
    pendingSave.resolve(response({ channel: settingsChannel(-1001, { target_path: '/A新目录' }) }));
    await saving;
    pendingVideos.resolve(response({ channel: settingsChannel(), videos: [video(3)] }));
    await selectingA;
    assert.equal(state.channelId, '-1001');
    assert.equal(document.getElementById('telegramChannelTargetPath').value, '/A新目录');
    assert.equal(document.getElementById('telegramVideosDownloadBtn').title, '上传到 TelDrive：/A新目录');
    assert.equal(state.videos[0].message_id, 3);
});

test('zero-second datetime values do not create an unsaved draft merely by leaving a field', () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(-1001, { sync_since: '2026-10-21T06:15:00Z' })];
    state.channelId = '-1001';
    context.renderTelegramChannelSettings();
    assert.doesNotMatch(document.getElementById('telegramChannelSyncSince').value, /:\d{2}:00$/);
    context.editTelegramChannelSettings();
    assert.equal(state.drafts.get('-1001').dirty, false);
});

test('switching a channel closes its folder picker and rejects any stale selection context', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(), settingsChannel(-1002)];
    state.channelId = '-1001';
    state.mode = 'channels';
    let opened = '';
    let closed = 0;
    context.openTelDriveFolderPicker = id => { opened = id; };
    context.closeTelDriveFolderPicker = () => { closed += 1; };
    context.fetch = async () => response({ videos: [video(1)] });
    context.openTelegramChannelFolderPicker(document.getElementById('telegramChannelBrowseBtn'));
    assert.equal(opened, 'telegramChannelTargetPath');
    assert.equal(context.canApplyTelegramChannelFolder(), true);
    await context.selectTelegramChannel(-1002);
    assert.equal(closed, 1);
    assert.equal(context.canApplyTelegramChannelFolder(), false);
    await context.selectTelegramChannel(-1001);
    assert.equal(context.canApplyTelegramChannelFolder(), false);
});

test('an automatic upload ledger entry remains nonselectable after its task row was cleared', () => {
    const { context } = setup();
    assert.equal(context.telegramVideoSelectable(video(1, { auto_status: 'submitted', auto_job_id: 'old-task' })), false);
    assert.match(context.renderTelegramVideoRow(video(1, { auto_status: 'submitted' })), /已提交/);
    assert.doesNotMatch(context.renderTelegramVideoRow(video(1, { auto_status: 'submitted' })), /已提交自动同步/);
    assert.match(context.renderTelegramVideoRow(video(1, { auto_status: 'submitted', auto_job_status: 'completed' })), /已上传 · 任务记录已清理/);
    assert.match(context.renderTelegramVideoRow(video(2, { auto_status: 'failed', auto_error: '<文件不可用>' })), /&lt;文件不可用&gt;/);
});

test('live sync errors and counts remain visible after a successful settings save', async () => {
    const { context, state, document } = setup();
    state.channels = [settingsChannel(-1001, { auto_sync: true, sync_status: 'watching', auto_pending_count: 4 })];
    state.channelId = '-1001';
    const draft = context.getTelegramChannelDraft();
    draft.message = '已保存';
    draft.messageKind = 'success';
    context.renderTelegramChannelSettings();
    assert.equal(document.getElementById('telegramChannelSettingsStatus').textContent, '已保存');
    context.fetch = async () => response({ channels: state.channels });
    await context.loadTelegramChannelsPage(true);
    assert.equal(document.getElementById('telegramChannelSettingsStatus').textContent, '待处理 4');
    state.channels[0].last_sync_error = 'Telegram 读取失败';
    state.channels[0].sync_status = 'error';
    context.renderTelegramChannelSettings();
    assert.equal(document.getElementById('telegramChannelSettingsStatus').textContent, 'Telegram 读取失败');
    state.channels[0].enabled = false;
    state.channels[0].sync_status = 'manual';
    assert.equal(context.telegramChannelSyncStatus(state.channels[0])[0], '监控已暂停');
    state.channels[0].last_sync_error = '';
    state.channels[0].auto_pending_count = 0;
    context.renderTelegramChannelSettings();
    assert.equal(document.getElementById('telegramChannelSettingsStatus').hidden, true);
});
