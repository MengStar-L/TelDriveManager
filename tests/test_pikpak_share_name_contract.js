const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const appJs = fs.readFileSync(path.join(__dirname, '..', 'app', 'static', 'app.js'), 'utf8');

function extract(startMarker, endMarker) {
    const start = appJs.indexOf(startMarker);
    const end = appJs.indexOf(endMarker, start);
    assert.ok(start >= 0, `${startMarker} not found`);
    assert.ok(end > start, `${endMarker} not found after ${startMarker}`);
    return appJs.slice(start, end);
}

const nameHelpers = extract(
    'function getPickerItemName',
    '// === Jellyfin Rename (Auto_Bangumi port) start ===',
);
const jellyfinBlock = extract(
    '// === Jellyfin Rename (Auto_Bangumi port) start ===',
    '// === Jellyfin Rename (Auto_Bangumi port) end ===',
);
const pickerContract = extract(
    'function getPickerDataArray',
    'function getPickerContainerId',
);

const originalName = '[SweetSub][进击的巨人][Shingeki][01][1080p].mkv';
const originalPath = `Season 1/${originalName}`;
const sourceItem = {
    id: 'source-1',
    source_file_id: 'source-1',
    name: originalName,
    source_name: originalName,
    path: originalPath,
    source_path: originalPath,
};

const sandbox = {
    __items: [sourceItem],
    isPickerFolder() { return false; },
    showA2TDToast() {},
    getPickerFileCheckboxes() { return []; },
    renderPickerTree() {},
    getPickerContainerId() { return 'fileList'; },
    syncPickerFolderStates() {},
    updatePickerSelection() {},
};

vm.runInNewContext(`
    let magnetFileData = [];
    let shareFileData = globalThis.__items;
    let rssFileData = [];
    ${nameHelpers}
    ${jellyfinBlock}
    ${pickerContract}
    globalThis.api = {
        formatPickerNamesJellyfin,
        buildJellyfinOverrides,
        getPickerItemName,
    };
`, sandbox);

sandbox.api.formatPickerNamesJellyfin('share');

assert.strictEqual(sourceItem.name, originalName);
assert.strictEqual(sourceItem.path, originalPath);
assert.strictEqual(sourceItem.source_name, originalName);
assert.strictEqual(sourceItem.source_path, originalPath);
assert.strictEqual(sourceItem.output_name, '进击的巨人 S01E01.mkv');
assert.strictEqual(sandbox.api.getPickerItemName(sourceItem), '进击的巨人 S01E01.mkv');
assert.deepStrictEqual(
    JSON.parse(JSON.stringify(sandbox.api.buildJellyfinOverrides('share'))),
    { 'source-1': '进击的巨人 S01E01.mkv' },
);

console.log('pikpak share name contract tests passed');
