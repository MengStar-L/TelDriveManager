const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function setup() {
    const source = fs.readFileSync(path.join(__dirname, '../app/static/app.js'), 'utf8');
    const nodes = Object.fromEntries(['telegramRelayTargetPath', 'telegramRelaySettingEnabled',
        'telegramRelaySocksUrl', 'telegramRelaySettingsStatus', 'cfgTelegramRelayTargetPath',
        'teldriveFolderPickerList', 'teldriveFolderSelectBtn', 'teldriveFolderPickerPath', 'teldriveFolderParentBtn']
        .map(id => [id, {value: '', checked: false, addEventListener() {}, replaceChildren() {}}]));
    const context = vm.createContext({
        document: {getElementById: id => nodes[id]},
        window: {currentConfig: {telegram_relay: {target_path: '/old', download_dir: './keep', concurrency: 3}}},
        normalizeTelegramRelayProxyType: value => value,
        escapeA2TDHtml: value => value,
        showFieldCheck() {}, clearTimeout() {},
        readJsonSafe: async response => response.data || {success: true},
        URL, URLSearchParams, AbortController,
        fetch: async () => ({ok: true}),
    });
    vm.runInContext('let telegramRelaySettingsSaveTimer = null; let telegramRelaySettingsSaveQueue = Promise.resolve();', context);
    vm.runInContext(source.slice(source.indexOf('function getTelegramRelaySettingsEls()'),
        source.indexOf('function normalizeT2TDRelayJob(')), context);
    return {context, nodes};
}

test('cloud target is loaded, saved, and reflected in global settings without overwriting server storage', async () => {
    const {context, nodes} = setup();
    context.fillTelegramRelaySettingsForm({target_path: '/old', enabled: true});
    assert.equal(nodes.telegramRelayTargetPath.value, '/old');
    nodes.telegramRelayTargetPath.value = ' /cloud//new relay/ ';
    let request;
    context.fetch = async (_, options) => { request = JSON.parse(options.body); return {ok: true}; };
    await context.saveTelegramRelaySocksSettings();
    assert.equal(request.telegram_relay.target_path, '/cloud/new relay');
    assert.equal(request.telegram_relay.download_dir, undefined);
    assert.equal(request.telegram_relay.concurrency, undefined);
    assert.equal(context.window.currentConfig.telegram_relay.concurrency, 3);
    assert.equal(context.window.currentConfig.telegram_relay.download_dir, './keep');
    assert.equal(nodes.cfgTelegramRelayTargetPath.value, '/cloud/new relay');
    nodes.telegramRelayTargetPath.value = '   ';
    assert.equal(context.collectTelegramRelaySettingsPayload().target_path, '');
});

test('directory save errors remain visible and keep the previously saved value', async () => {
    const {context, nodes} = setup();
    nodes.telegramRelayTargetPath.value = '/new';
    context.fetch = async () => ({ok: false, data: {detail: 'Permission denied'}});
    assert.equal(await context.saveTelegramRelaySocksSettings(), null);
    assert.equal(context.window.currentConfig.telegram_relay.target_path, '/old');
    assert.match(nodes.telegramRelaySettingsStatus.innerHTML, /Permission denied/);
});

test('directory saves are serialized so a slow older request cannot overwrite the latest one', async () => {
    const {context, nodes} = setup();
    const requests = [];
    let release;
    context.fetch = async (_, options) => {
        requests.push(JSON.parse(options.body).telegram_relay.target_path);
        if (requests.length === 1) await new Promise(resolve => { release = resolve; });
        return {ok: true};
    };
    nodes.telegramRelayTargetPath.value = '/first';
    const first = context.saveTelegramRelaySocksSettings();
    await new Promise(setImmediate);
    nodes.telegramRelayTargetPath.value = '/second';
    const second = context.saveTelegramRelaySocksSettings();
    await new Promise(setImmediate);
    assert.deepEqual(requests, ['/first']);
    release();
    await Promise.all([first, second]);
    assert.deepEqual(requests, ['/first', '/second']);
    assert.equal(context.window.currentConfig.telegram_relay.target_path, '/second');
});

test('invalid cloud paths stop saving and show an error', async () => {
    const {context, nodes} = setup();
    let requested = false;
    context.fetch = async () => { requested = true; return {ok: true}; };
    nodes.telegramRelayTargetPath.value = '/a/../b';
    assert.equal(await context.saveTelegramRelaySocksSettings(), null);
    assert.equal(requested, false);
    assert.equal(context.window.currentConfig.telegram_relay.target_path, '/old');
    assert.match(nodes.telegramRelaySettingsStatus.className, /error/);
});

test('late folder responses cannot replace the most recently opened directory', async () => {
    const {context, nodes} = setup();
    let release;
    context.fetch = async url => {
        const folder = new URL(url, 'http://localhost').searchParams.get('path');
        if (folder === '/slow') await new Promise(resolve => { release = resolve; });
        return {ok: true, data: {path: folder, parent_path: '/', folders: []}};
    };
    const slow = context.loadTelDrivePickerFolders('/slow');
    await context.loadTelDrivePickerFolders('/fast');
    release();
    await slow;
    assert.equal(nodes.teldriveFolderPickerPath.textContent, '/fast');
    assert.equal(nodes.teldriveFolderSelectBtn.disabled, false);
    context.fetch = async () => ({ok: false, data: {detail: 'TelDrive unavailable'}});
    await context.loadTelDrivePickerFolders('/failed');
    assert.equal(nodes.teldriveFolderSelectBtn.disabled, true);
    assert.match(nodes.teldriveFolderPickerList.innerHTML, /TelDrive unavailable/);
});
