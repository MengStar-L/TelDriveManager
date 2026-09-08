const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function setup() {
    const source = fs.readFileSync(path.join(__dirname, '../app/static/app.js'), 'utf8');
    const nodes = Object.fromEntries(['telegramRelayDownloadDir', 'telegramRelaySettingEnabled',
        'telegramRelaySocksUrl', 'telegramRelaySettingsStatus', 'cfgTelegramRelayDownloadDir']
        .map(id => [id, {value: '', checked: false, addEventListener() {}}]));
    const context = vm.createContext({
        document: {getElementById: id => nodes[id]},
        window: {currentConfig: {telegram_relay: {download_dir: './old', concurrency: 3}}},
        normalizeTelegramRelayProxyType: value => value,
        escapeA2TDHtml: value => value,
        showFieldCheck() {}, clearTimeout() {},
        readJsonSafe: async response => response.data || {success: true},
        URL,
        fetch: async () => ({ok: true}),
    });
    vm.runInContext('let telegramRelaySettingsSaveTimer = null; let telegramRelaySettingsSaveQueue = Promise.resolve();', context);
    vm.runInContext(source.slice(source.indexOf('function getTelegramRelaySettingsEls()'),
        source.indexOf('function normalizeT2TDRelayJob(')), context);
    return {context, nodes};
}

test('relay directory is loaded, saved, and reflected in global settings without overwriting hidden fields', async () => {
    const {context, nodes} = setup();
    context.fillTelegramRelaySettingsForm({download_dir: '/mnt/old', enabled: true});
    assert.equal(nodes.telegramRelayDownloadDir.value, '/mnt/old');
    nodes.telegramRelayDownloadDir.value = ' /mnt/new relay ';
    let request;
    context.fetch = async (_, options) => { request = JSON.parse(options.body); return {ok: true}; };
    await context.saveTelegramRelaySocksSettings();
    assert.equal(request.telegram_relay.download_dir, '/mnt/new relay');
    assert.equal(request.telegram_relay.concurrency, undefined);
    assert.equal(context.window.currentConfig.telegram_relay.concurrency, 3);
    assert.equal(nodes.cfgTelegramRelayDownloadDir.value, '/mnt/new relay');
    nodes.telegramRelayDownloadDir.value = '   ';
    assert.equal(context.collectTelegramRelaySettingsPayload().download_dir, './telegram_relay');
});

test('directory save errors remain visible and keep the previously saved value', async () => {
    const {context, nodes} = setup();
    nodes.telegramRelayDownloadDir.value = '/not-writable';
    context.fetch = async () => ({ok: false, data: {detail: 'Permission denied'}});
    assert.equal(await context.saveTelegramRelaySocksSettings(), null);
    assert.equal(context.window.currentConfig.telegram_relay.download_dir, './old');
    assert.match(nodes.telegramRelaySettingsStatus.innerHTML, /Permission denied/);
});

test('directory saves are serialized so a slow older request cannot overwrite the latest one', async () => {
    const {context, nodes} = setup();
    const requests = [];
    let release;
    context.fetch = async (_, options) => {
        requests.push(JSON.parse(options.body).telegram_relay.download_dir);
        if (requests.length === 1) await new Promise(resolve => { release = resolve; });
        return {ok: true};
    };
    nodes.telegramRelayDownloadDir.value = '/first';
    const first = context.saveTelegramRelaySocksSettings();
    await new Promise(setImmediate);
    nodes.telegramRelayDownloadDir.value = '/second';
    const second = context.saveTelegramRelaySocksSettings();
    await new Promise(setImmediate);
    assert.deepEqual(requests, ['/first']);
    release();
    await Promise.all([first, second]);
    assert.deepEqual(requests, ['/first', '/second']);
    assert.equal(context.window.currentConfig.telegram_relay.download_dir, '/second');
});
