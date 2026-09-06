const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function setup() {
    const source = fs.readFileSync(path.join(__dirname, '../app/static/app.js'), 'utf8');
    const start = source.indexOf('let updateStatusTimer');
    const end = source.indexOf('async function toggleMonitorSerialMode', start);
    const nodes = Object.fromEntries(['updateReleaseNotes', 'updateApplyBtn'].map(id => [id, {style: {}}]));
    const context = vm.createContext({
        document: {getElementById: id => nodes[id]},
        window: {location: {reload: () => context.reloads++}},
        reloads: 0,
        response: {state: 'up_to_date', current_version: '1.0.0'},
        fetch: async () => ({ok: true}),
        readJsonSafe: async () => context.response,
        setInterval: () => 1,
        clearInterval: () => {},
    });
    vm.runInContext(source.slice(start, end), context);
    return {context, nodes};
}

test('update failure renders its error and permits retry', () => {
    const {context, nodes} = setup();
    context.renderUpdateStatus({state: 'error', error: 'rollback failed', update_available: true});
    assert.equal(nodes.updateReleaseNotes.textContent, 'rollback failed');
    assert.equal(nodes.updateApplyBtn.disabled, false);
});

test('browser reloads only after a new version is serving', async () => {
    const {context} = setup();
    await context.loadUpdateStatus();
    context.response = {state: 'restarting', current_version: '2.0.0'};
    await context.loadUpdateStatus();
    assert.equal(context.reloads, 0);
    context.response = {state: 'up_to_date', current_version: '2.0.0'};
    await context.loadUpdateStatus();
    assert.equal(context.reloads, 1);
});
