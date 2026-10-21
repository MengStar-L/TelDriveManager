const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

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
