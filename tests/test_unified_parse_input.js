const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const appJs = fs.readFileSync(path.join(__dirname, '..', 'app', 'static', 'app.js'), 'utf8');
const start = appJs.indexOf('function cleanPikPakShareLink');
const end = appJs.indexOf('// === Magnet Parsing ===', start);

assert.ok(start >= 0, 'cleanPikPakShareLink helper not found');
assert.ok(end > start, 'unified parse helper block not found');

const sandbox = {};
vm.runInNewContext(appJs.slice(start, end), sandbox);

const magnet = sandbox.analyzeUnifiedParseInput([
    'magnet:?xt=urn:btih:aaa',
    'magnet:?xt=urn:btih:bbb',
].join('\n'));
assert.strictEqual(magnet.type, 'magnet');
assert.deepStrictEqual(Array.from(magnet.magnets), ['magnet:?xt=urn:btih:aaa', 'magnet:?xt=urn:btih:bbb']);

const share = sandbox.analyzeUnifiedParseInput('https://mypikpak.com/s/abc?act=play');
assert.strictEqual(share.type, 'share');
assert.strictEqual(share.shareLink, 'https://mypikpak.com/s/abc');

const mixed = sandbox.analyzeUnifiedParseInput([
    'magnet:?xt=urn:btih:aaa',
    'https://mypikpak.com/s/abc?act=play',
].join('\n'));
assert.strictEqual(mixed.type, 'mixed');

console.log('unified parse input tests passed');
