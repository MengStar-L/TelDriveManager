import os
path = r'd:\Code\TelDriveManager\app\static\app.js'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

wizard_js = '''
// ── Setup Wizard ──
async function checkSetupRequired() {
    try {
        const resp = await fetch('/api/settings');
        const cfg = await resp.json();
        if (cfg._meta && cfg._meta.needs_setup) {
            document.getElementById('setupWizard').classList.add('show');
        }
    } catch (e) { console.error('Failed to check setup', e); }
}

function wizardNext(current, next) {
    document.getElementById('wStep' + current).classList.remove('active');
    document.getElementById('wStep' + next).classList.add('active');
    document.querySelectorAll('.wizard-dot').forEach(d => d.classList.remove('active'));
    document.getElementById('dot' + next).classList.add('active');
}

async function wizardFinish() {
    const btn = event.currentTarget;
    const oldHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> 部署中...';

    const cfg = {
        pikpak: {
            username: document.getElementById('wPikUser').value.trim(),
            password: document.getElementById('wPikPass').value.trim()
        },
        aria2: {
            rpc_url: document.getElementById('wAriaUrl').value.trim() || 'http://localhost:6800/jsonrpc'
        },
        teldrive: {
            api_host: document.getElementById('wTdUrl').value.trim(),
            access_token: document.getElementById('wTdToken').value.trim()
        },
        telegram: {
            api_id: parseInt(document.getElementById('wTgId').value) || 0,
            api_hash: document.getElementById('wTgHash').value.trim()
        }
    };

    try {
        await fetch('/api/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
        document.getElementById('setupWizard').classList.remove('show');
        setTimeout(() => location.reload(), 1500);
    } catch(e) {
        alert('保存失败: ' + e.message);
        btn.disabled = false;
        btn.innerHTML = oldHtml;
    }
}
'''

if 'checkSetupRequired' not in text:
    text = wizard_js + '\n' + text
    
    # Inject call in window.onload
    text = text.replace('window.onload = () => {', 'window.onload = () => {\n    checkSetupRequired();')

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)

print('Injected Setup Wizard JS logic to app.js')
