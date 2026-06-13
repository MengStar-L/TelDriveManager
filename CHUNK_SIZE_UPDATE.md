# 上传分块大小配置更新说明

## 问题分析

### 关于500M分块上传失败是否会重传整个块的问题

**答案：不会立即重传整个500M**

代码实现了智能重试机制（`app/modules/aria2teldrive/teldrive_client.py`）：

1. **客户端超时/断连时**（第693-707行）：
   - 先轮询确认服务端是否已成功接收并落块
   - 如果服务端已落块，直接复用，不重传
   - 只有确认服务端没有接收到时，才会重传整个分块

2. **轮询窗口计算**（第506-512行）：
   - 根据分块大小动态计算等待时间
   - 500M块最多等待300秒确认服务端状态
   - 避免了因客户端超时导致的重复上传

### 改为250M的影响

**优点**：
- ✅ 重传成本更低（如果确实需要重传）
- ✅ 网络不稳定时更容易完成单个分块
- ✅ 进度更新更频繁，用户体验更好
- ✅ 超时时间更短，问题发现更快

**缺点**：
- ❌ 分块数量翻倍（1GB文件：4块变8块）
- ❌ API调用次数增加
- ❌ Telegram频道消息数量增加
- ❌ 元数据开销略微增加

**建议**：
- 网络稳定：保持500M或使用1G
- 网络一般：使用250M（新默认值）
- 网络不稳定：使用100M-200M

## 实施的修改

### 1. 后端配置默认值更新

**文件：`app/config.py`**
- 默认值从 `"500M"` 改为 `"250M"`（第62行）

**文件：`config.example.toml`**
- 示例配置从 `chunk_size = "500M"` 改为 `chunk_size = "250M"`（第48行）

### 2. 前端界面新增配置选项

**文件：`app/static/index.html`**

在"TelDrive 持久化上传"配置卡片中新增下拉选择框：

```html
<div class="form-group">
    <label>上传分块大小</label>
    <select class="form-input" id="cfgTeldriveChunkSize" onchange="handleChunkSizeChange()">
        <option value="100M">100 MB（网络极不稳定）</option>
        <option value="200M">200 MB（网络较差）</option>
        <option value="250M" selected>250 MB（推荐）</option>
        <option value="500M">500 MB（网络稳定）</option>
        <option value="1G">1 GB（高速网络）</option>
        <option value="2G">2 GB（极速网络）</option>
    </select>
</div>
```

添加了说明文本：
- 解释了分块大小对重传成本和网络要求的影响
- 提供了不同网络状况的推荐配置

**文件：`app/static/app.js`**

1. **配置加载**（第2450行）：
   ```javascript
   document.getElementById('cfgTeldriveChunkSize').value = cfg.teldrive?.chunk_size || '250M';
   ```

2. **配置保存**（第2385行）：
   ```javascript
   chunk_size: document.getElementById('cfgTeldriveChunkSize')?.value || currentConfig?.teldrive?.chunk_size || '250M'
   ```

3. **实时保存处理**（第2587-2593行）：
   ```javascript
   function handleChunkSizeChange() {
       const select = document.getElementById('cfgTeldriveChunkSize');
       if (!select) return;
       
       // 触发自动保存（500ms防抖）
       if (_autoSaveTimer) clearTimeout(_autoSaveTimer);
       _autoSaveTimer = setTimeout(() => doAutoSave(select), 500);
   }
   ```

### 3. 用户体验优化

- ✅ **即时生效**：用户选择后自动保存，无需点击"保存"按钮
- ✅ **视觉反馈**：保存成功后显示绿色对号提示（复用现有的auto-save机制）
- ✅ **合理默认值**：新默认250M在网络稳定性和性能间取得平衡
- ✅ **清晰说明**：每个选项都标注了适用的网络状况

## 兼容性说明

- ✅ 已有配置文件会保留原有的chunk_size值
- ✅ 未设置的新安装默认使用250M
- ✅ 前端界面会正确显示当前配置的值
- ✅ 所有chunk_size选项（100M/200M/250M/500M/1G/2G）均已在`teldrive_client.py`的`CHUNK_SIZE_MAP`中定义

## 测试建议

1. 清空浏览器缓存后访问设置页面
2. 确认"上传分块大小"下拉框显示为"250 MB（推荐）"
3. 切换选择不同的值，观察是否出现绿色对号
4. 刷新页面，确认选择的值被正确保存和加载
5. 检查`config.toml`文件中的`chunk_size`值是否与界面一致

## 文件清单

修改的文件：
- `app/config.py` - 更新默认配置
- `config.example.toml` - 更新示例配置
- `app/static/index.html` - 添加配置UI
- `app/static/app.js` - 添加加载/保存/实时更新逻辑

未修改（无需修改）：
- `app/modules/aria2teldrive/teldrive_client.py` - CHUNK_SIZE_MAP已支持所有选项
- 测试文件 - 使用500M不影响测试逻辑
