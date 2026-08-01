# Unified Share Folder Rename Implementation Plan

**Goal:** Restore the "层级重命名" control in the unified PikPak share-result toolbar and send its value to the existing share-download API.

**Architecture:** The unified parse page owns a new, initially hidden checkbox. Result rendering toggles its visibility based on whether the result is a PikPak share or a magnet and resets it for every newly rendered result. The unified share submit function reads that checkbox instead of hard-coding `rename_by_folder: false`; the existing backend remains unchanged.

**Tech Stack:** Static HTML, browser JavaScript, Node.js `assert`/`vm` regression test.

---

## File Structure

- Modify: `D:\Code\TelDriveManager\app\static\index.html` - add the unified-page checkbox immediately after `magnetKeepStructure`.
- Modify: `D:\Code\TelDriveManager\app\static\app.js` - synchronize the checkbox visibility with unified result mode and include its value in the existing share download POST body.
- Create: `D:\Code\TelDriveManager\tests\test_unified_share_folder_rename.js` - execute the focused frontend behavior in a minimal Node VM harness.

### Task 1: Add a failing unified-share UI/request regression test

**Files:**
- Create: `D:\Code\TelDriveManager\tests\test_unified_share_folder_rename.js`

- **Step 1: Write the failing test**

```js
assert.match(indexHtml, /id="magnetRenameByFolderControl" hidden/);
assert.match(indexHtml, /id="magnetRenameByFolder"/);

assert.strictEqual(renameControl.hidden, true);
setUnifiedShareRenameVisibility(true);
assert.strictEqual(renameControl.hidden, false);
assert.strictEqual(renameCheckbox.checked, false);

await downloadUnifiedShareFiles();
assert.strictEqual(requestBody.rename_by_folder, true);

renameCheckbox.checked = false;
await downloadUnifiedShareFiles();
assert.strictEqual(requestBody.rename_by_folder, false);
```

The VM harness will provide only the DOM nodes, selected file checkbox, `fetch`, `getTelDriveTargetPath`, `buildJellyfinOverrides`, and `switchPage` needed by the extracted frontend functions. It will also call the visibility helper with `false` after a checked share state to verify that the control hides and resets.

- **Step 2: Run test to verify it fails**

Run:

```powershell
node tests\test_unified_share_folder_rename.js
```

Expected: FAIL because the unified toolbar lacks `magnetRenameByFolderControl` and `downloadUnifiedShareFiles()` still fixes `rename_by_folder` to `false`.

### Task 2: Restore the unified-toolbar control and wire its state

**Files:**
- Modify: `D:\Code\TelDriveManager\app\static\index.html:1372-1376`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:1992-2045`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:5448-5481`

- **Step 1: Add the initially hidden control after the structure toggle**

```html
<label class="toggle-row" id="magnetRenameByFolderControl" hidden>
    <div class="toggle-switch"><input type="checkbox" id="magnetRenameByFolder"><span class="toggle-slider"></span></div>
    层级重命名
</label>
```

Keep the existing `magnetKeepStructure` markup unchanged and do not alter the hidden legacy `shareRenameByFolder` control.

- **Step 2: Add one focused visibility helper and call it from both result renderers**

```js
function setUnifiedShareRenameVisibility(visible) {
    const control = document.getElementById('magnetRenameByFolderControl');
    const checkbox = document.getElementById('magnetRenameByFolder');
    if (control) control.hidden = !visible;
    if (checkbox) checkbox.checked = false;
}

// renderMagnetParseResult
setUnifiedShareRenameVisibility(false);

// renderUnifiedShareParseResult
setUnifiedShareRenameVisibility(true);
```

This makes every new result start with the documented default off state. A magnet result both hides and clears any previous share setting.

- **Step 3: Replace the hard-coded share request value**

```js
const renameByFolder = document.getElementById('magnetRenameByFolder')?.checked ?? false;

// in the existing JSON body
rename_by_folder: renameByFolder,
```

Leave `file_paths`, `keep_structure`, `teldrive_path`, and `name_overrides` unchanged so the pre-existing backend mapping and Jellyfin override precedence continue to apply.

- **Step 4: Run the focused regression test**

Run:

```powershell
node tests\test_unified_share_folder_rename.js
```

Expected: PASS with captured `/api/pikpak/share/download` bodies containing `rename_by_folder: true` when enabled and `false` when disabled, followed by a visibility/reset assertion for magnet mode.

### Task 3: Run affected regression coverage and inspect the final diff

**Files:**
- Verify: `D:\Code\TelDriveManager\tests\test_unified_parse_input.js`
- Verify: `D:\Code\TelDriveManager\tests\test_unified_share_folder_rename.js`
- Verify: `D:\Code\TelDriveManager\app\static\index.html`
- Verify: `D:\Code\TelDriveManager\app\static\app.js`

- **Step 1: Run existing unified-input coverage**

Run:

```powershell
node tests\test_unified_parse_input.js
```

Expected: PASS, confirming the unified parser still recognizes magnets and PikPak shares as before.

- **Step 2: Run Python PikPak isolation coverage**

Run:

```powershell
python -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: PASS, confirming the untouched explicit restore-target safeguards still pass.

- **Step 3: Review only task-owned changes**

Run:

```powershell
git diff --check -- app/static/index.html app/static/app.js tests/test_unified_share_folder_rename.js
git diff -- app/static/index.html app/static/app.js tests/test_unified_share_folder_rename.js
```

Expected: no whitespace errors; only the new unified control, its result-mode visibility, request field binding, and focused test are present.

- **Step 4: Commit**

Run:

```powershell
git add app/static/index.html app/static/app.js tests/test_unified_share_folder_rename.js .openteams/plans/2026-08-01-unified-share-folder-rename.md .openteams/specs/2026-08-01-unified-share-folder-rename-design.html
git commit -m "fix: restore unified share folder rename"
```

Expected: a commit containing only this feature's files. Do not stage pre-existing unrelated Telegram, configuration, or deployment changes.
