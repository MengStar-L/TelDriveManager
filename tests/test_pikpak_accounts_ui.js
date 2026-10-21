const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

const source = fs.readFileSync(path.join(__dirname, '../app/static/app.js'), 'utf8');

function extract(startMarker, endMarker) {
    const start = source.indexOf(startMarker);
    const end = source.indexOf(endMarker, start);
    assert.ok(start >= 0 && end > start, `Cannot extract ${startMarker}`);
    return source.slice(start, end);
}

function setup() {
    const nodes = {};
    const document = {
        activeElement: null,
        getElementById: id => nodes[id] || null,
        addEventListener() {},
        removeEventListener() {},
    };
    const createNode = id => {
        const classes = new Set();
        const attributes = new Map();
        return nodes[id] = {
            id, innerHTML: '', textContent: '', value: '', disabled: false,
            isConnected: true, style: {}, dataset: {},
            classList: {
                add: (...values) => values.forEach(value => classes.add(value)),
                remove: (...values) => values.forEach(value => classes.delete(value)),
                contains: value => classes.has(value),
                toggle(value, force) {
                    const enabled = force === undefined ? !classes.has(value) : force;
                    if (enabled) classes.add(value); else classes.delete(value);
                    return enabled;
                },
            },
            setAttribute: (name, value) => attributes.set(name, String(value)),
            getAttribute: name => attributes.get(name) ?? null,
            removeAttribute: name => attributes.delete(name),
            focus() { document.activeElement = this; },
            contains(element) { return element === this; },
            addEventListener() {},
            removeEventListener() {},
            querySelectorAll() { return []; },
        };
    };
    [
        'pikpakErrorsModal', 'pikpakErrorsTitle', 'pikpakErrorsAccount',
        'pikpakErrorsList', 'pikpakErrorsSummary', 'pikpakClearErrorsBtn',
        'pikpakErrorsCloseBtn', 'pikpakAccountsRefreshBtn', 'errorTrigger',
    ].forEach(createNode);
    nodes.errorTrigger.focus();
    const toasts = [];
    const context = vm.createContext({
        document,
        window: { requestAnimationFrame: callback => callback() },
        requestAnimationFrame: callback => callback(),
        setTimeout, clearTimeout, AbortController,
        normalizePikpakLoginMode: value => value,
        syncPikpakLoginModeButtons() {},
        formatProgressLogTime: value => String(value || ''),
        showA2TDToast: (...args) => toasts.push(args),
        readJsonSafe: async response => response.data || {},
        fetch: async () => ({ ok: true, data: { success: true, errors: [] } }),
    });
    vm.runInContext(extract('function escapeA2TDHtml(', 'function getA2TDTaskStatusLabel('), context);
    vm.runInContext(extract('let pikpakAccountsState =', 'function collectSettingsConfig('), context);
    vm.runInContext(`pikpakAccountsState.accounts = [
        {id: 'a', name: '账号 A', error_count: 1},
        {id: 'b', name: '账号 B', error_count: 1}
    ]; globalThis.accountState = pikpakAccountsState;`, context);
    return { context, nodes, document, toasts };
}

function deferred() {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return { promise, resolve };
}

function okErrors(message, extra = {}) {
    return { ok: true, data: { success: true, errors: [
        { stage: 'magnet_parse', created_at: '2026-09-25T13:16:09', message, ...extra },
    ] } };
}

test('a late error response cannot replace another account or update a closed dialog', async () => {
    const { context, nodes } = setup();
    const slow = deferred();
    context.fetch = url => url.includes('/a/') ? slow.promise : Promise.resolve(okErrors('B 的记录'));
    const first = context.openPikpakAccountErrorsModal('a');
    await context.openPikpakAccountErrorsModal('b');
    slow.resolve(okErrors('A 的旧记录'));
    await first;
    assert.match(nodes.pikpakErrorsList.innerHTML, /B 的记录/);
    assert.doesNotMatch(nodes.pikpakErrorsList.innerHTML, /A 的旧记录/);
    assert.equal(nodes.pikpakErrorsAccount.textContent, '账号 B');

    const afterClose = deferred();
    context.fetch = () => afterClose.promise;
    const pending = context.openPikpakAccountErrorsModal('a');
    context.closePikpakAccountErrorsModal();
    const closedContents = nodes.pikpakErrorsList.innerHTML;
    afterClose.resolve(okErrors('关闭后的旧记录'));
    await pending;
    assert.equal(nodes.pikpakErrorsModal.classList.contains('show'), false);
    assert.equal(nodes.pikpakErrorsList.innerHTML, closedContents);
});

test('a stale failed request cannot replace the current account error list', async () => {
    const { context, nodes } = setup();
    const slow = deferred();
    context.fetch = url => url.includes('/a/') ? slow.promise : Promise.resolve(okErrors('B 正常加载'));
    const first = context.openPikpakAccountErrorsModal('a');
    await context.openPikpakAccountErrorsModal('b');
    slow.resolve({ ok: false, data: { message: 'A 加载失败' } });
    await first;
    assert.match(nodes.pikpakErrorsList.innerHTML, /B 正常加载/);
    assert.doesNotMatch(nodes.pikpakErrorsList.innerHTML, /A 加载失败/);
});

test('closing restores focus and clearing errors cannot reopen a closed dialog', async () => {
    const { context, nodes, document } = setup();
    const removal = deferred();
    let deleteRequests = 0;
    context.fetch = (url, options = {}) => {
        if (options.method === 'DELETE') { deleteRequests += 1; return removal.promise; }
        return Promise.resolve(okErrors('已有记录'));
    };
    await context.openPikpakAccountErrorsModal('a');
    const clearing = context.clearPikpakAccountErrors();
    await context.clearPikpakAccountErrors();
    assert.equal(deleteRequests, 1, 'Repeated clicks must not duplicate the delete request');
    context.closePikpakAccountErrorsModal();
    assert.equal(document.activeElement, nodes.errorTrigger);
    removal.resolve({ ok: true, data: { success: true, count: 1 } });
    await clearing;
    assert.equal(nodes.pikpakErrorsModal.classList.contains('show'), false);
    assert.equal(context.accountState.activeErrorAccountId, '');
});

test('clearing one account cannot replace a subsequently opened account', async () => {
    const { context, nodes } = setup();
    const removal = deferred();
    context.fetch = (url, options = {}) => {
        if (options.method === 'DELETE') return removal.promise;
        return Promise.resolve(okErrors(url.includes('/b/') ? 'B 保留的记录' : 'A 的记录'));
    };
    await context.openPikpakAccountErrorsModal('a');
    const clearing = context.clearPikpakAccountErrors();
    context.closePikpakAccountErrorsModal();
    await context.openPikpakAccountErrorsModal('b');
    removal.resolve({ ok: true, data: { success: true, count: 1 } });
    await clearing;
    assert.equal(context.accountState.activeErrorAccountId, 'b');
    assert.equal(nodes.pikpakErrorsAccount.textContent, '账号 B');
    assert.match(nodes.pikpakErrorsList.innerHTML, /B 保留的记录/);
    assert.equal(nodes.pikpakErrorsModal.classList.contains('show'), true);
});

test('untrusted names, messages, stages and links render as text', async () => {
    const { context, nodes } = setup();
    const payload = '<img src=x onerror="alert(1)"> & "quote"';
    context.fetch = async () => okErrors(payload, { stage: payload, link: payload });
    await context.openPikpakAccountErrorsModal('a', payload);
    assert.equal(nodes.pikpakErrorsAccount.textContent, payload);
    assert.doesNotMatch(nodes.pikpakErrorsList.innerHTML, /<img|<script|<a\s/i);
    assert.match(nodes.pikpakErrorsList.innerHTML, /&lt;img/);
    assert.match(nodes.pikpakErrorsList.innerHTML, /&amp;/);

    context.fetch = async () => okErrors('解析超时');
    await context.openPikpakAccountErrorsModal('a');
    assert.match(nodes.pikpakErrorsList.innerHTML, /磁链/);
    assert.doesNotMatch(nodes.pikpakErrorsList.innerHTML, />magnet_parse</);
});

test('empty and failed loads disable clearing and preserve specific error details', async () => {
    const { context, nodes } = setup();
    const loading = deferred();
    let deletes = 0;
    context.fetch = (url, options = {}) => {
        if (options.method === 'DELETE') { deletes += 1; return Promise.resolve({ ok: true }); }
        return loading.promise;
    };
    const pending = context.openPikpakAccountErrorsModal('a');
    assert.equal(nodes.pikpakClearErrorsBtn.disabled, true);
    loading.resolve({ ok: true, data: { errors: [] } });
    await pending;
    assert.equal(nodes.pikpakClearErrorsBtn.disabled, true);
    await context.clearPikpakAccountErrors();
    assert.equal(deletes, 0);

    context.fetch = async () => ({ ok: false, data: { message: '网络连接已断开 <retry>' } });
    await context.openPikpakAccountErrorsModal('b');
    assert.equal(nodes.pikpakClearErrorsBtn.disabled, true);
    assert.match(nodes.pikpakErrorsList.innerHTML, /网络连接已断开/);
    assert.match(nodes.pikpakErrorsList.innerHTML, /&lt;retry&gt;/);
});
