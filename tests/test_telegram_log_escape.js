const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

test('Telegram log renders untrusted fields as text', () => {
    const source = fs.readFileSync(path.join(__dirname, '../app/static/app.js'), 'utf8');
    const start = source.indexOf('function appendT2TDLog(');
    const end = source.indexOf('\nfunction ', start + 1);
    const entries = [];
    const container = {appendChild: entry => entries.push(entry)};
    const context = vm.createContext({
        t2tdPanelMode: 'logs',
        document: {
            getElementById: id => id === 't2tdLogContainer' ? container : null,
            createElement: () => ({style: {}, set innerHTML(value) {throw new Error('HTML sink used');}}),
        },
    });
    vm.runInContext(source.slice(start, end), context);
    const injected = '<img src=x onerror=alert(1)>';
    context.appendT2TDLog({message: injected, level: injected, time: injected}, {skipStore: true});
    assert.equal(entries.length, 1);
    assert.equal(entries[0].textContent, `[${injected}] [${injected}] ${injected}`);
});
