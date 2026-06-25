const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const appJs = fs.readFileSync(path.join(__dirname, '..', 'app', 'static', 'app.js'), 'utf8');
const START = '// === Jellyfin Rename (Auto_Bangumi port) start ===';
const END = '// === Jellyfin Rename (Auto_Bangumi port) end ===';
const start = appJs.indexOf(START);
const end = appJs.indexOf(END, start);

assert.ok(start >= 0, 'Jellyfin rename block start marker not found');
assert.ok(end > start, 'Jellyfin rename block end marker not found');

const sandbox = {};
vm.runInNewContext(appJs.slice(start, end + END.length), sandbox);

const { formatJellyfinFileName, formatJellyfinBaseName, parseAnimeEpisode } = sandbox;
assert.strictEqual(typeof formatJellyfinFileName, 'function', 'formatJellyfinFileName not exported');
assert.strictEqual(typeof formatJellyfinBaseName, 'function', 'formatJellyfinBaseName not exported');
assert.strictEqual(typeof parseAnimeEpisode, 'function', 'parseAnimeEpisode not exported');

// ── 完整文件名（含扩展名）→ 中文标题优先 ──
assert.strictEqual(
    formatJellyfinFileName('[SweetSub][进击的巨人][Shingeki][01][1080p].mkv'),
    '进击的巨人 S01E01.mkv',
    '中文标题 + [01] 集数',
);

// ── 无中文标题时回退英文，破折号集数，无显式季默认 S01 ──
assert.strictEqual(
    formatJellyfinBaseName('[动漫国字幕组&LoliHouse] THE MARGINAL SERVICE - 08 [WebRip 1080p HEVC-10bit AAC][简繁内封字幕]'),
    'THE MARGINAL SERVICE S01E08',
    '英文回退 + " - 08 " 集数',
);

// ── 显式第二季 S2 → S02 ──
const s2 = parseAnimeEpisode('[Group] Show S2 - 03 [1080p]');
assert.ok(s2, 'S2 样例应可解析');
assert.strictEqual(s2.season, 2, 'S2 → season 2');
assert.strictEqual(s2.episode, 3, 'S2 样例 episode 3');
assert.strictEqual(formatJellyfinBaseName('[Group] Show S2 - 03 [1080p]'), 'Show S02E03');

// ── 中文「第N话」集数标记 ──
assert.strictEqual(
    formatJellyfinBaseName('[某字幕组] 葬送的芙莉莲 第05话 [1080p]'),
    '葬送的芙莉莲 S01E05',
    '第05话 → E05',
);

// ── 字幕语言后缀，避免简/繁同名碰撞 ──
assert.strictEqual(
    formatJellyfinFileName('[Group][进击的巨人][01][CHT].ass'),
    '进击的巨人 S01E01.cht.ass',
    '繁体字幕 → .cht.ass',
);
assert.strictEqual(
    formatJellyfinFileName('[Group][进击的巨人][01][简体].srt'),
    '进击的巨人 S01E01.chs.srt',
    '简体字幕 → .chs.srt',
);

// ── 非剧集文件保持原名（返回 null）──
assert.strictEqual(formatJellyfinFileName('[Group] Fonts.7z'), null, '字体压缩包非剧集 → null');
assert.strictEqual(formatJellyfinFileName('poster.jpg'), null, '海报非剧集 → null');
assert.strictEqual(parseAnimeEpisode(''), null, '空串 → null');

// ── 扩展名保留：mp4 ──
assert.strictEqual(
    formatJellyfinFileName('[Sakurato] Boku no Kokoro - 11 [AVC-8bit 1080p].mp4'),
    'Boku no Kokoro S01E11.mp4',
    '保留 .mp4 扩展名',
);

console.log('jellyfin rename tests passed');
