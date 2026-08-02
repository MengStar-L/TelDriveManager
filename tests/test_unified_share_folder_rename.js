const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const root = path.join(__dirname, '..');
const indexHtml = fs.readFileSync(path.join(root, 'app', 'static', 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'app', 'static', 'app.js'), 'utf8');

assert.match(indexHtml, /id="magnetKeepStructure"[\s\S]{0,500}id="magnetRenameByFolderControl" hidden/);
assert.match(indexHtml, /id="magnetRenameByFolder"/);

function extractFunction(startMarker, endMarker) {
    const start = appJs.indexOf(startMarker);
    const end = appJs.indexOf(endMarker, start);
    assert.ok(start >= 0, `${startMarker} not found`);
    assert.ok(end > start, `${endMarker} not found after ${startMarker}`);
    return appJs.slice(start, end);
}

const visibilitySource = extractFunction(
    'function setUnifiedShareRenameVisibility',
    'function renderMagnetParseResult',
);
const magnetResultSource = extractFunction(
    'function renderMagnetParseResult',
    'function renderUnifiedShareParseResult',
);
const shareResultSource = extractFunction(
    'function renderUnifiedShareParseResult',
    'function renderShareParseResult',
);
const downloadSource = extractFunction(
    'async function downloadUnifiedShareFiles()',
    '// === Share Parsing ===',
);

assert.match(magnetResultSource, /setUnifiedShareRenameVisibility\(false\)/);
assert.match(shareResultSource, /setUnifiedShareRenameVisibility\(true\)/);

const renameControl = { hidden: true };
const renameCheckbox = { checked: false };
const keepStructureCheckbox = { checked: true };
const downloadButton = { disabled: false, innerHTML: '' };
const elements = new Map([
    ['magnetRenameByFolderControl', renameControl],
    ['magnetRenameByFolder', renameCheckbox],
    ['magnetKeepStructure', keepStructureCheckbox],
    ['magnetDownloadBtn', downloadButton],
]);
const requests = [];
const sandbox = {
    shareCurrentData: {
        share_id: 'share-1',
        pass_code_token: 'token-1',
    },
    shareFileData: [{
        id: 'file-1',
        source_file_id: 'file-1',
        name: 'original.mp4',
        source_name: 'original.mp4',
        path: 'parent-folder/Series S01E01.mp4',
        source_path: 'parent-folder/original.mp4',
        output_name: 'Series S01E01.mp4',
    }],
    shareDownloadSubmitting: false,
    document: {
        getElementById(id) {
            return elements.get(id) || null;
        },
        querySelectorAll(selector) {
            assert.strictEqual(selector, '#magnetFileList input[data-role="file"]:checked');
            return [{ value: 'file-1' }];
        },
    },
    getTelDriveTargetPath() {
        return '/target';
    },
    getPikPakShareSourceId(item = {}) {
        return String(item.source_file_id || item.id || '').trim();
    },
    buildJellyfinOverrides() {
        return { 'file-1': 'Series S01E01.mp4' };
    },
    async fetch(url, options) {
        requests.push({ url, body: JSON.parse(options.body) });
        return {
            ok: true,
            async json() {
                return {};
            },
        };
    },
    switchPage() {},
    alert(message) {
        throw new Error(message);
    },
};

vm.runInNewContext(`${visibilitySource}\n${downloadSource}`, sandbox);

assert.strictEqual(renameControl.hidden, true);
sandbox.setUnifiedShareRenameVisibility(true);
assert.strictEqual(renameControl.hidden, false);
assert.strictEqual(renameCheckbox.checked, false);

(async () => {
    renameCheckbox.checked = true;
    await sandbox.downloadUnifiedShareFiles();
    assert.strictEqual(requests[0].url, '/api/pikpak/share/download');
    assert.strictEqual(requests[0].body.rename_by_folder, true);
    assert.deepStrictEqual(requests[0].body.file_ids, ['file-1']);
    assert.deepStrictEqual(requests[0].body.file_paths, {
        'file-1': 'parent-folder/original.mp4',
    });
    assert.deepStrictEqual(requests[0].body.name_overrides, {
        'file-1': 'Series S01E01.mp4',
    });

    renameCheckbox.checked = false;
    await sandbox.downloadUnifiedShareFiles();
    assert.strictEqual(requests[1].body.rename_by_folder, false);

    renameCheckbox.checked = true;
    sandbox.setUnifiedShareRenameVisibility(false);
    assert.strictEqual(renameControl.hidden, true);
    assert.strictEqual(renameCheckbox.checked, false);

    console.log('unified share folder rename tests passed');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
