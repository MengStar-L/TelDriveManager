import os
path = r'd:\Code\TelDriveManager\app\static\index.html'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

css_block = '''
        /* ── Setup Wizard ── */
        .wizard-overlay { position: fixed; inset: 0; background: rgba(255,255,255,0.7); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); z-index: 1000; display: flex; align-items: center; justify-content: center; opacity: 0; pointer-events: none; transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1); }
        .wizard-overlay.show { opacity: 1; pointer-events: auto; }
        .wizard-card { width: 500px; background: white; border-radius: 24px; box-shadow: 0 24px 80px rgba(0,0,0,0.1); padding: 40px; display: flex; flex-direction: column; gap: 24px; transform: translateY(20px); transition: transform 0.4s cubic-bezier(0.4, 0, 0.2, 1); border: 1px solid rgba(0,0,0,0.05); }
        .wizard-overlay.show .wizard-card { transform: translateY(0); }
        .wizard-header { margin-bottom: 8px; text-align: center; }
        .wizard-title { font-size: 24px; font-weight: 700; color: #1e293b; margin-bottom: 8px; }
        .wizard-subtitle { font-size: 14px; color: #64748b; }
        .wizard-step { display: none; flex-direction: column; gap: 16px; animation: wizardFadeIn 0.3s ease forwards; }
        .wizard-step.active { display: flex; }
        @keyframes wizardFadeIn { from { opacity: 0; transform: translateX(10px); } to { opacity: 1; transform: translateX(0); } }
        .wizard-footer { display: flex; justify-content: space-between; margin-top: 16px; }
        .wizard-dots { display: flex; gap: 8px; align-items: center; justify-content: center; margin-bottom: 24px; }
        .wizard-dot { width: 8px; height: 8px; border-radius: 50%; background: #e2e8f0; transition: all 0.3s; }
        .wizard-dot.active { background: #0066ff; width: 24px; border-radius: 4px; }
'''

if '.wizard-overlay' not in text:
    text = text.replace('/* ── Pages ── */', css_block + '\n        /* ── Pages ── */')

html_block = '''
    <!-- Setup Wizard Overlay -->
    <div id="setupWizard" class="wizard-overlay">
        <div class="wizard-card">
            <div class="wizard-header">
                <div class="wizard-title">欢迎使用 TelDriveManager</div>
                <div class="wizard-subtitle">看起来是第一次运行，让我们完成基础配置</div>
            </div>
            
            <div class="wizard-dots">
                <div class="wizard-dot active" id="dot1"></div>
                <div class="wizard-dot" id="dot2"></div>
                <div class="wizard-dot" id="dot3"></div>
            </div>

            <!-- Step 1: PikPak -->
            <div class="wizard-step active" id="wStep1">
                <div class="field-grid cols-1">
                    <div class="form-group">
                        <label>PikPak 账号 / 邮箱 (可选)</label>
                        <input class="form-input" type="text" id="wPikUser" placeholder="如果不使用离线下载可留空">
                    </div>
                    <div class="form-group">
                        <label>PikPak 密码</label>
                        <input class="form-input" type="password" id="wPikPass" placeholder="留空跳过">
                    </div>
                </div>
                <div class="wizard-footer" style="justify-content: flex-end;">
                    <button class="btn btn-primary" onclick="wizardNext(1, 2)">下一步 <i class="ph ph-arrow-right"></i></button>
                </div>
            </div>

            <!-- Step 2: TelDrive & Aria2 -->
            <div class="wizard-step" id="wStep2">
                <div class="field-grid cols-1">
                    <div class="form-group">
                        <label>TelDrive API 地址 (必选)</label>
                        <input class="form-input" type="text" id="wTdUrl" placeholder="如 http://localhost:8080">
                    </div>
                    <div class="form-group">
                        <label>TelDrive Token (必选)</label>
                        <input class="form-input" type="password" id="wTdToken" placeholder="抓包获取的 Bearer Token">
                    </div>
                    <div class="form-group">
                        <label>Aria2 RPC (选填)</label>
                        <input class="form-input" type="text" id="wAriaUrl" placeholder="http://localhost:6800/jsonrpc">
                    </div>
                </div>
                <div class="wizard-footer">
                    <button class="btn btn-ghost" onclick="wizardNext(2, 1)"><i class="ph ph-arrow-left"></i> 上一步</button>
                    <button class="btn btn-primary" onclick="wizardNext(2, 3)">下一步 <i class="ph ph-arrow-right"></i></button>
                </div>
            </div>

            <!-- Step 3: Telegram -->
            <div class="wizard-step" id="wStep3">
                <div class="field-grid cols-1" style="position: relative;">
                    <div class="form-group">
                        <label>Telegram API ID (必填)</label>
                        <input class="form-input" type="number" id="wTgId" placeholder="前往 my.telegram.org 获取">
                    </div>
                    <div class="form-group">
                        <label>Telegram API Hash (必填)</label>
                        <input class="form-input" type="text" id="wTgHash" placeholder="如 abcdef1234567890">
                    </div>
                </div>
                <div class="wizard-footer">
                    <button class="btn btn-ghost" onclick="wizardNext(3, 2)"><i class="ph ph-arrow-left"></i> 上一步</button>
                    <button class="btn btn-primary" onclick="wizardFinish()"><i class="ph ph-check"></i> 完成配置</button>
                </div>
            </div>
        </div>
    </div>
'''

if 'id="setupWizard"' not in text:
    text = text.replace('</body>', html_block + '\n</body>')

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)

print('Injected HTML/CSS to index.html')
