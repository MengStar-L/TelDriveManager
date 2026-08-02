# PikPak Source Identity and Output Naming Implementation Plan

**Goal:** Make PikPak deep-share parsing, isolated restore correlation, Jellyfin formatting, directory mapping, and final output naming use one explicit ID-based contract so correct files are never rejected or replaced because their names changed.

**Architecture:** Parse a PikPak URL into an explicit `share_id` and optional `target_id`, then preserve immutable source metadata through the frontend. Correlate restored files with selected files through PikPak's `source_file_id -> destination_file_id` task mapping; bind paths and output overrides by `source_file_id`, while keeping names out of all identity checks.

**Tech Stack:** Python 3.11+, FastAPI, `pikpakapi`, vanilla browser JavaScript, Python `unittest`, Node.js `assert`/`vm` tests.

---

## File Structure

- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\client.py` - own share URL parsing, request explicit target scope, reject root fallback, emit immutable source metadata, and attach source/destination IDs to resolved restore results.
- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\routes.py` - bind restored results and output overrides by source ID instead of filename and keep structure mapping based on source paths.
- Modify: `D:\Code\TelDriveManager\app\static\app.js` - normalize immutable share records, render `output_name` without mutating source fields, and submit paths/overrides keyed by source ID.
- Modify: `D:\Code\TelDriveManager\app\static\index.html` - increment the `app.js` cache-busting query so deployed browsers load the contract fix.
- Modify: `D:\Code\TelDriveManager\tests\test_pikpak_share_restore_isolation.py` - cover deep-link scope, restore ID correlation, duplicate names, output overrides, and zero-push failures.
- Create: `D:\Code\TelDriveManager\tests\test_pikpak_share_name_contract.js` - execute the frontend naming contract in a Node VM and verify source fields remain immutable.
- Modify: `D:\Code\TelDriveManager\tests\test_unified_share_folder_rename.js` - verify unified download requests send original paths and ID-keyed output overrides.

### Task 1: Make share-link scope explicit and fail closed

**Files:**
- Modify: `D:\Code\TelDriveManager\tests\test_pikpak_share_restore_isolation.py`
- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\client.py:524-554`

- **Step 1: Add failing deep-link scope tests**

Add a `ShareListScopeTests` class using `build_client()` and a raw client that records `_request_get()` parameters:

```python
class ShareListScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_deep_link_requests_explicit_target_and_preserves_source_metadata(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.calls = []

            async def _request_get(self, url, params=None):
                self.calls.append((url, params))
                return {
                    "pass_code_token": "pass-token",
                    "files": [{
                        "id": "source-1",
                        "parent_id": "target-folder",
                        "name": "original.mkv",
                        "kind": "drive#file",
                        "size": "100",
                        "mime_type": "video/x-matroska",
                    }],
                }

        raw = RawClient()
        client = build_client(raw)
        result = await client.get_share_file_list(
            "https://mypikpak.com/s/share-1/target-folder"
        )

        self.assertEqual(raw.calls[0][1]["share_id"], "share-1")
        self.assertEqual(raw.calls[0][1]["parent_id"], "target-folder")
        self.assertEqual(result["target_id"], "target-folder")
        self.assertEqual(result["files"][0]["source_file_id"], "source-1")
        self.assertEqual(result["files"][0]["source_name"], "original.mkv")
        self.assertEqual(result["files"][0]["source_path"], "original.mkv")

    async def test_deep_link_rejects_share_root_fallback(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            async def _request_get(self, url, params=None):
                return {
                    "pass_code_token": "pass-token",
                    "files": [{
                        "id": "unrelated-folder",
                        "parent_id": "",
                        "name": "Unrelated",
                        "kind": "drive#folder",
                    }],
                }

        with self.assertRaisesRegex(RuntimeError, "链接目标节点不一致"):
            await build_client(RawClient()).get_share_file_list(
                "https://mypikpak.com/s/share-1/target-folder"
            )
```

- **Step 2: Run the two tests and verify they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.ShareListScopeTests -v
```

Expected: FAIL because `get_share_file_list()` delegates URL parsing to `pikpakapi`, returns no `target_id`/source fields, and does not reject a root fallback.

- **Step 3: Parse the URL with the standard URL parser**

Add the import and helper in `client.py`:

```python
from urllib.parse import urlsplit


def _parse_pikpak_share_location(share_link: str) -> tuple[str, Optional[str]]:
    value = str(share_link or "").strip()
    parsed = urlsplit(value if "://" in value else f"https://{value}")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) not in {2, 3} or parts[0].lower() != "s":
        raise ValueError("无效的 PikPak 分享链接格式")
    share_id = parts[1].strip()
    target_id = parts[2].strip() if len(parts) == 3 else None
    if not share_id or (len(parts) == 3 and not target_id):
        raise ValueError("无效的 PikPak 分享链接格式")
    return share_id, target_id
```

Keep query/fragment removal in the route, but do not use a regex or the SDK's internal regex to decide the target scope.

- **Step 4: Request the share endpoint with explicit IDs and validate the first response**

Replace the existing `get_share_info(share_link, ...)` call with:

```python
share_id, target_id = _parse_pikpak_share_location(share_link)
result = await self.client._request_get(
    url=f"https://{self.client.PIKPAK_API_HOST}/drive/v1/share",
    params={
        "limit": "100",
        "thumbnail_size": "SIZE_LARGE",
        "order": "3",
        "share_id": share_id,
        "parent_id": target_id,
        "pass_code": pass_code or None,
    },
)
if not isinstance(result, dict):
    raise RuntimeError("PikPak 分享接口响应格式无效")
roots = list(result.get("files", []) or [])
if target_id:
    target_is_returned = len(roots) == 1 and str(roots[0].get("id") or "") == target_id
    target_is_parent = bool(roots) and all(
        str(item.get("parent_id") or "").strip() == target_id
        for item in roots
    )
    if not (target_is_returned or target_is_parent):
        raise RuntimeError("PikPak 返回的分享内容与链接目标节点不一致")
```

Build the response with `target_id` and let `_collect_share_files()` recurse only from the validated roots.

- **Step 5: Emit immutable source fields for every leaf file**

Change the leaf record in `_collect_share_files()` to:

```python
files.append({
    "id": file_id,
    "source_file_id": file_id,
    "name": name,
    "source_name": name,
    "path": full_path,
    "source_path": full_path,
    "size": int(file_info.get("size", 0)),
    "file_type": file_info.get("mime_type", ""),
    "icon_link": file_info.get("icon_link", ""),
})
```

Return:

```python
return {
    "share_id": share_id,
    "target_id": target_id,
    "pass_code_token": result.get("pass_code_token", ""),
    "files": files,
}
```

- **Step 6: Run the focused tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.ShareListScopeTests -v
```

Expected: PASS; the recorded request carries `parent_id=target-folder`, source metadata is present, and unrelated root content raises before recursion.

- **Step 7: Commit the scope contract**

Run:

```powershell
git add app/modules/pikpak/client.py tests/test_pikpak_share_restore_isolation.py .openteams/specs/2026-08-02-pikpak-source-output-contract-design.html .openteams/plans/2026-08-02-pikpak-source-output-contract.md
git commit -m "fix: make pikpak share scope explicit"
```

Expected: one commit containing the written contract, explicit share scope request, and focused tests.

### Task 2: Carry source IDs through isolated restore results

**Files:**
- Modify: `D:\Code\TelDriveManager\tests\test_pikpak_share_restore_isolation.py:145-360`
- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\client.py:245-343`

- **Step 1: Extend the successful restore-resolution test**

In the test whose task map is `{"selected-1": "restored-1"}`, assert both IDs:

```python
self.assertEqual(result[0]["source_file_id"], "selected-1")
self.assertEqual(result[0]["destination_file_id"], "restored-1")
self.assertEqual(result[0]["file_id"], "restored-1")
```

Retain the existing assertions for URL, name, size, task mapping, parent ID, and direct-file membership.

- **Step 2: Run the successful resolver test and verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.IsolatedShareRestoreClientTests.test_restore_task_mapping_resolves_direct_children_in_selected_order -v
```

Expected: FAIL with missing `source_file_id` and `destination_file_id` keys.

- **Step 3: Add both IDs when constructing each resolved result**

Inside the existing loop over `receipt.selected_ids`, keep the task-map lookup and add explicit correlation fields:

```python
resolved.append({
    "source_file_id": selected_id,
    "destination_file_id": destination_id,
    "file_id": destination_id,
    "name": name,
    "url": url,
    "path": name,
    "size": int(child.get("size") or 0),
})
```

Do not derive `source_file_id` from file names or list position. The only source is the validated `trace_file_ids` map.

- **Step 4: Run all client isolation tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.IsolatedShareRestoreClientTests -v
```

Expected: PASS; existing target ownership, mapping, direct-child, URL, timeout, and cleanup tests remain green.

- **Step 5: Commit ID correlation**

Run:

```powershell
git add app/modules/pikpak/client.py tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: preserve source ids through share restore"
```

Expected: one commit limited to restore result correlation and its tests.

### Task 3: Replace filename-based backend binding with source-ID binding

**Files:**
- Modify: `D:\Code\TelDriveManager\tests\test_pikpak_share_restore_isolation.py:460-523`
- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\routes.py:225-250`

- **Step 1: Replace the old filename-mismatch test with ID-contract tests**

Add the following cases to the route helper test class:

```python
def test_selected_paths_bind_by_source_id_even_when_names_match_or_change(self):
    files = [{
        "source_file_id": "selected-1",
        "destination_file_id": "restored-1",
        "file_id": "restored-1",
        "name": "original.mkv",
        "url": "https://download/selected",
    }]
    result = pikpak_routes._bind_selected_share_paths(
        files,
        ["selected-1"],
        {"selected-1": "Series/original.mkv"},
    )
    self.assertEqual(result[0]["path"], "Series/original.mkv")
    self.assertEqual(result[0]["source_path"], "Series/original.mkv")

def test_duplicate_basenames_bind_to_their_own_source_ids(self):
    files = [
        {"source_file_id": "source-b", "name": "same.mkv"},
        {"source_file_id": "source-a", "name": "same.mkv"},
    ]
    result = pikpak_routes._bind_selected_share_paths(
        files,
        ["source-a", "source-b"],
        {
            "source-a": "Season A/same.mkv",
            "source-b": "Season B/same.mkv",
        },
    )
    self.assertEqual([item["source_file_id"] for item in result], ["source-a", "source-b"])
    self.assertEqual([item["path"] for item in result], ["Season A/same.mkv", "Season B/same.mkv"])

def test_unknown_or_duplicate_source_ids_fail_closed(self):
    files = [
        {"source_file_id": "selected-1", "name": "one.mkv"},
        {"source_file_id": "selected-1", "name": "two.mkv"},
    ]
    with self.assertRaisesRegex(RuntimeError, "源文件 ID"):
        pikpak_routes._bind_selected_share_paths(
            files,
            ["selected-1", "selected-2"],
            {"selected-1": "one.mkv", "selected-2": "two.mkv"},
        )
```

Remove the obsolete expectation that different names alone must fail.

- **Step 2: Run the helper tests and verify they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.ShareSelectionContractTests -v
```

Expected: FAIL because `_bind_selected_share_paths()` still compares sorted basenames and cannot distinguish duplicate names by ID.

- **Step 3: Implement strict source-ID binding**

Replace the name buckets in `_bind_selected_share_paths()` with:

```python
def _bind_selected_share_paths(
    files: List[dict], file_ids: List[str], file_paths: Dict[str, str]
) -> List[dict]:
    expected_ids = [str(file_id or "").strip() for file_id in file_ids]
    files_by_source_id: Dict[str, dict] = {}
    for item in files:
        source_id = str(item.get("source_file_id") or "").strip()
        if not source_id or source_id in files_by_source_id:
            raise RuntimeError(
                "PikPak 隔离解析结果的源文件 ID 无效，本次未推送任何下载链接"
            )
        files_by_source_id[source_id] = item
    if set(files_by_source_id) != set(expected_ids):
        raise RuntimeError(
            "PikPak 隔离解析结果与勾选文件 ID 不一致，本次未推送任何下载链接"
        )

    rebound = []
    for source_id in expected_ids:
        source_path = str(file_paths.get(source_id) or "").strip().replace("\\", "/")
        copy = dict(files_by_source_id[source_id])
        copy["source_file_id"] = source_id
        copy["source_name"] = posixpath.basename(source_path)
        copy["source_path"] = source_path
        copy["path"] = source_path
        rebound.append(copy)
    return rebound
```

This preserves the selected-ID order without trusting the restore API's list order.

- **Step 4: Run the path contract tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.ShareSelectionContractTests -v
```

Expected: PASS for changed names and duplicate basenames; missing/duplicate source IDs fail before any push.

- **Step 5: Commit backend binding**

Run:

```powershell
git add app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: bind restored share files by source id"
```

Expected: one commit replacing only the filename identity contract.

### Task 4: Make frontend formatting preserve source metadata

**Files:**
- Create: `D:\Code\TelDriveManager\tests\test_pikpak_share_name_contract.js`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:2030-2064`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:5518-5537`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:5930-6045`

- **Step 1: Create a failing Node VM contract test**

Extract the picker-name helpers, Jellyfin pure block, and `getPickerDataArray()` through `buildJellyfinOverrides()` from `app.js`. Evaluate them with one share record and minimal renderer stubs:

```js
const sourceItem = {
    id: 'source-1',
    source_file_id: 'source-1',
    name: '[Group][Show][01][1080p].mkv',
    source_name: '[Group][Show][01][1080p].mkv',
    path: 'Season 1/[Group][Show][01][1080p].mkv',
    source_path: 'Season 1/[Group][Show][01][1080p].mkv',
};

sandbox.api.formatPickerNamesJellyfin('share');

assert.strictEqual(sourceItem.name, '[Group][Show][01][1080p].mkv');
assert.strictEqual(sourceItem.path, 'Season 1/[Group][Show][01][1080p].mkv');
assert.strictEqual(sourceItem.source_name, '[Group][Show][01][1080p].mkv');
assert.strictEqual(sourceItem.source_path, 'Season 1/[Group][Show][01][1080p].mkv');
assert.strictEqual(sourceItem.output_name, 'Show S01E01.mkv');
assert.strictEqual(sandbox.api.getPickerItemName(sourceItem), 'Show S01E01.mkv');
assert.deepStrictEqual(
    JSON.parse(JSON.stringify(sandbox.api.buildJellyfinOverrides('share'))),
    { 'source-1': 'Show S01E01.mkv' },
);
```

The VM must stub `isPickerFolder`, `renderPickerTree`, `getPickerFileCheckboxes`, `syncPickerFolderStates`, `updatePickerSelection`, `getPickerContainerId`, and `showA2TDToast`; no browser or network is required.

- **Step 2: Run the Node test and verify it fails**

Run:

```powershell
node tests\test_pikpak_share_name_contract.js
```

Expected: FAIL because the formatter mutates `name/path`, has no `output_name`, and builds share overrides by original basename.

- **Step 3: Normalize parsed share records once**

Add:

```js
function normalizePikPakShareFile(item = {}) {
    const sourceFileId = String(item.source_file_id || item.id || '').trim();
    const sourceName = String(item.source_name || item.name || '').trim();
    const sourcePath = String(item.source_path || item.path || sourceName).replace(/\\/g, '/').trim();
    return {
        ...item,
        id: sourceFileId,
        source_file_id: sourceFileId,
        name: sourceName,
        source_name: sourceName,
        path: sourcePath,
        source_path: sourcePath,
        output_name: String(item.output_name || '').trim(),
    };
}
```

Apply it in both `renderUnifiedShareParseResult()` and `renderShareParseResult()` before sorting:

```js
shareFileData = sortPickerItemsByName(
    (Array.isArray(result.files) ? result.files : []).map(normalizePikPakShareFile)
);
```

- **Step 4: Render output names without changing source paths**

Change the picker helper and leaf renderer to use:

```js
function getPickerItemName(item = {}) {
    return String(item.output_name || item.name || item.title || '').trim();
}

const title = escapeA2TDHtml(getPickerItemName(item) || '未命名文件');
const fullPathRaw = getPickerItemPath(item);
```

Keep `getPickerPathSegments()` based on `item.path`, so folder structure and checkbox tree paths remain source paths.

- **Step 5: Make Jellyfin formatting write only `output_name`**

For non-RSS items, replace `_jfOriginalName/_jfOriginalPath` mutation with:

```js
const sourceName = String(item.source_name || item.name || '');
const sourcePath = String(item.source_path || item.path || sourceName);
let next = formatJellyfinFileName(sourceName);
if (!next) {
    item.output_name = '';
    skipped++;
    return;
}
const dir = jfDirname(sourcePath);
const set = usedByDir.get(dir) || new Set();
if (set.has(next)) {
    let n = 2;
    while (set.has(jfWithDedupeSuffix(next, n))) n++;
    next = jfWithDedupeSuffix(next, n);
}
set.add(next);
usedByDir.set(dir, set);
item.output_name = next;
renamed++;
```

Do not assign `item.name` or `item.path`. Leave the RSS title branch unchanged.

- **Step 6: Build share overrides by source ID**

Use:

```js
} else if (item.output_name && item.output_name !== (item.source_name || item.name)) {
    const key = prefix === 'share'
        ? String(item.source_file_id || item.id || '')
        : String(item.id || '');
    if (key) overrides[key] = item.output_name;
}
```

This keeps magnet overrides keyed by file ID and changes share overrides from basename to source ID.

- **Step 7: Run the frontend contract test**

Run:

```powershell
node tests\test_pikpak_share_name_contract.js
```

Expected: PASS; source fields are byte-for-byte unchanged, the visible name is formatted, and the override key is `source-1`.

- **Step 8: Commit immutable frontend naming**

Run:

```powershell
git add app/static/app.js tests/test_pikpak_share_name_contract.js
git commit -m "fix: separate pikpak source names from output names"
```

Expected: one commit containing only the frontend data-model change and focused Node test.

### Task 5: Submit original paths and apply output names by source ID

**Files:**
- Modify: `D:\Code\TelDriveManager\tests\test_unified_share_folder_rename.js`
- Modify: `D:\Code\TelDriveManager\tests\test_pikpak_share_restore_isolation.py:586-745`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:5457-5491`
- Modify: `D:\Code\TelDriveManager\app\static\app.js:5797-5833`
- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\routes.py:1413-1456`
- Modify: `D:\Code\TelDriveManager\app\modules\pikpak\routes.py:1811-1940`

- **Step 1: Make the unified request test represent a formatted file**

Change the sandbox record and override stub:

```js
shareFileData: [{
    id: 'file-1',
    source_file_id: 'file-1',
    name: 'original.mp4',
    source_name: 'original.mp4',
    path: 'parent-folder/original.mp4',
    source_path: 'parent-folder/original.mp4',
    output_name: 'Series S01E01.mp4',
}],
buildJellyfinOverrides() {
    return { 'file-1': 'Series S01E01.mp4' };
},
```

After submission, assert:

```js
assert.deepStrictEqual(requests[0].body.file_ids, ['file-1']);
assert.deepStrictEqual(requests[0].body.file_paths, {
    'file-1': 'parent-folder/original.mp4',
});
assert.deepStrictEqual(requests[0].body.name_overrides, {
    'file-1': 'Series S01E01.mp4',
});
```

- **Step 2: Add a failing backend Jellyfin/restore integration test**

Update the successful route fake so `wait_for_isolated_share_urls()` returns:

```python
return [{
    "source_file_id": "selected-1",
    "destination_file_id": "restored-1",
    "file_id": "restored-1",
    "name": "original.mp4",
    "path": "original.mp4",
    "url": "https://download/selected",
    "size": 100,
}]
```

Call `_process_share_download()` with:

```python
await pikpak_routes._process_share_download(
    "share-1",
    ["selected-1"],
    "pass-token",
    file_paths={"selected-1": "Season/original.mp4"},
    name_overrides={"selected-1": "Series S01E01.mp4"},
)
```

Assert exactly one aria2 call and:

```python
self.assertEqual(aria2.added[0][1]["out"], "Series S01E01.mp4")
```

- **Step 3: Run both tests and verify they fail**

Run:

```powershell
node tests\test_unified_share_folder_rename.js
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.ShareDownloadIsolationRouteTests.test_one_selected_file_pushes_one_isolated_url -v
```

Expected: the Node request still derives paths from mutable `item.path`, and the Python route still looks up `name_overrides` using the restored filename.

- **Step 4: Build both share download requests from immutable fields**

In `downloadUnifiedShareFiles()` and the legacy `downloadShareFiles()` use:

```js
const getSourceId = item => String(item.source_file_id || item.id || '');
const orderedSelectedItems = shareFileData.filter(
    item => selectedSet.has(getSourceId(item))
);
const selectedIds = orderedSelectedItems.map(getSourceId);
const filePaths = Object.fromEntries(
    orderedSelectedItems.map(item => [
        getSourceId(item),
        item.source_path || item.path || item.source_name || item.name || '',
    ])
);
```

The checkbox value remains the source ID because normalized share records keep `id=source_file_id`.

- **Step 5: Normalize and validate ID-keyed overrides at the API boundary**

After `_normalize_selected_ids(file_ids)`, normalize `name_overrides` to stripped string IDs/names and reject unknown IDs:

```python
name_overrides = {
    str(file_id or "").strip(): str(name or "").strip()
    for file_id, name in name_overrides.items()
    if str(file_id or "").strip() and str(name or "").strip()
}
unknown_override_ids = set(name_overrides) - set(file_ids)
if unknown_override_ids:
    return JSONResponse(
        {"error": "文件名覆盖包含未勾选的源文件 ID"},
        status_code=400,
    )
```

- **Step 6: Remove name buckets from `_process_share_download()`**

Delete `orig_paths_by_name`. After `_bind_selected_share_paths()`, every `url_info` already has its own source path. Use:

```python
original_path = str(url_info.get("source_path") or url_info.get("path") or "")
output_name = _maybe_rename_by_folder(
    url_info,
    rename_by_folder,
    original_path,
)
source_file_id = str(url_info.get("source_file_id") or "").strip()
override_name = (name_overrides or {}).get(source_file_id)
if override_name:
    output_name = override_name
```

Keep the priority `Jellyfin override > layer rename > original restored name` and keep directory calculation based on `url_info["path"]`, which is now the source path.

- **Step 7: Run the focused end-to-end contract tests**

Run:

```powershell
node tests\test_pikpak_share_name_contract.js
node tests\test_unified_share_folder_rename.js
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: all pass; the formatted file retains its original request path, one restored source ID maps to one destination ID, and aria2 receives `out=Series S01E01.mp4`.

- **Step 8: Commit the request/output integration**

Run:

```powershell
git add app/static/app.js app/modules/pikpak/routes.py tests/test_unified_share_folder_rename.js tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: apply share output names by source id"
```

Expected: one commit for frontend request serialization and backend output application.

### Task 6: Bust browser cache and run the complete regression suite

**Files:**
- Modify: `D:\Code\TelDriveManager\app\static\index.html:2149`
- Verify: all files listed in this plan

- **Step 1: Increment the static script version**

Change:

```html
<script src="/static/app.js?v=20260731a"></script>
```

to:

```html
<script src="/static/app.js?v=20260802a"></script>
```

This prevents the deployed browser from retaining the mutating formatter/request code.

- **Step 2: Run JavaScript syntax and focused frontend tests**

Run:

```powershell
node --check app\static\app.js
node tests\test_jellyfin_rename.js
node tests\test_pikpak_share_name_contract.js
node tests\test_unified_share_folder_rename.js
node tests\test_unified_parse_input.js
```

Expected: syntax check exits 0 and every script prints its success message.

- **Step 3: Compile and run focused Python coverage**

Run:

```powershell
.\.venv\Scripts\python.exe -m py_compile app\modules\pikpak\client.py app\modules\pikpak\routes.py tests\test_pikpak_share_restore_isolation.py
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation tests.test_pikpak_multi_account -v
```

Expected: compilation exits 0 and all PikPak tests pass.

- **Step 4: Run the complete Python suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

Expected: the full suite passes with no regression in magnet parsing, serial transfer, Telegram safety, TelDrive upload, or account health behavior.

- **Step 5: Inspect task-owned changes and whitespace**

Run:

```powershell
git diff --check
git status --short
git log --oneline -6
```

Expected: no whitespace errors; only the plan/spec and the files listed above are changed or committed. Runtime tokens, databases, sessions, logs, downloads, and server data remain untracked and unstaged.

- **Step 6: Commit the cache version if it is not already included**

Run:

```powershell
git add app/static/index.html
git commit -m "chore: refresh pikpak frontend assets"
```

Expected: a final small commit containing only the static version change. If the file was committed with Task 5, verify the working tree instead of creating an empty commit.

### Task 7: Perform a no-push real-chain acceptance test before delivery

**Files:**
- Verify: `D:\Code\TelDriveManager\app\modules\pikpak\client.py`
- Verify: `D:\Code\TelDriveManager\app\modules\pikpak\routes.py`

- **Step 1: Parse the reported deep share through the application client**

Use the existing configured PikPak account and call only `get_share_file_list()` for the reported URL. Record, without printing credentials or tokens:

```text
share_id
target_id
source_file_id count
source_path list
```

Expected: the returned files belong only to the URL's target scope. A response that cannot prove the target scope raises and produces no download job.

- **Step 2: Exercise isolated restore without aria2**

For the selected 12 source IDs, call `start_isolated_share_restore()` and `wait_for_isolated_share_urls()` directly. Assert:

```python
assert {item["source_file_id"] for item in resolved} == set(selected_ids)
assert len({item["destination_file_id"] for item in resolved}) == len(selected_ids)
assert all(item["url"] for item in resolved)
```

Expected: exactly 12 correlated results; no aria2 or serial-queue method is called.

- **Step 3: Clean only the owned scope**

Delete only `receipt.scope_id` in a `finally` block and record whether cleanup succeeded.

Expected: the temporary `.teldrive-share-*` directory is removed. No root folder, `Pack From Shared`, unrelated file, or user download is deleted.

- **Step 4: Report acceptance evidence**

Report the parsed target ID, selected/resolved counts, source-ID equality, destination-ID uniqueness, and cleanup result. Do not include account credentials, pass-code tokens, signed download URLs, or session data.

Expected: delivery evidence demonstrates that the corrected contract works before code is pushed or deployed.
