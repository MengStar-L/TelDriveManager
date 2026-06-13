#!/usr/bin/env bash
# debug_network_traffic.sh - 实时监控 TelDriveManager 进程及相关服务的网络流量
# 用法：在云端服务器执行 bash debug_network_traffic.sh

set -euo pipefail

echo "=== TelDriveManager 网络流量调试 ==="
echo "按 Ctrl+C 停止监控"
echo ""

# 1. 找 Python 主进程（运行 main.py / uvicorn 的进程）
MAIN_PID=$(pgrep -f "python.*main.py|uvicorn.*main:app" | head -1 || echo "")
if [[ -z "$MAIN_PID" ]]; then
    echo "❌ 未找到 TelDriveManager 主进程（搜索 python.*main.py | uvicorn）"
    echo "   请检查服务是否在运行，或手动指定 PID："
    echo "   MAIN_PID=<pid> bash $0"
    exit 1
fi

echo "✓ 找到主进程: PID=$MAIN_PID"
ps -p "$MAIN_PID" -o pid,comm,cmd --no-headers
echo ""

# 2. 找 aria2c 子进程（如果有托管 aria2）
ARIA2_PID=$(pgrep -P "$MAIN_PID" aria2c 2>/dev/null || pgrep aria2c 2>/dev/null | head -1 || echo "")
if [[ -n "$ARIA2_PID" ]]; then
    echo "✓ 找到 aria2c 进程: PID=$ARIA2_PID"
    ps -p "$ARIA2_PID" -o pid,comm,cmd --no-headers
else
    echo "⚠ 未找到 aria2c 进程（可能未启用托管模式或外部 aria2）"
fi
echo ""

# 3. 检查活跃网络连接
echo "--- 活跃 TCP 连接（本项目相关端口 8888/6822 及对外连接）---"
if command -v ss >/dev/null 2>&1; then
    ss -tnp 2>/dev/null | grep -E "pid=$MAIN_PID|:8888|:6822|aria2" | head -20 || echo "（无活跃连接）"
elif command -v netstat >/dev/null 2>&1; then
    netstat -tnp 2>/dev/null | grep -E "$MAIN_PID|:8888|:6822|aria2" | head -20 || echo "（无活跃连接）"
else
    echo "⚠ ss/netstat 不可用，跳过连接检查"
fi
echo ""

# 4. 实时流量监控（基于 /proc/[pid]/net/dev 或 nethogs）
monitor_proc_net() {
    local pid=$1
    local name=$2
    local net_dev="/proc/$pid/net/dev"

    if [[ ! -r "$net_dev" ]]; then
        echo "⚠ 无法读取 $net_dev (权限不足或进程已退出)"
        return 1
    fi

    # 读取初始值（所有接口的 RX/TX 字节累计）
    local rx0 tx0
    rx0=$(awk 'NR>2 {rx+=$2} END {printf "%.0f", rx+0}' "$net_dev")
    tx0=$(awk 'NR>2 {tx+=$10} END {printf "%.0f", tx+0}' "$net_dev")

    echo "$name (PID=$pid) 初始累计: RX=${rx0} bytes, TX=${tx0} bytes"

    while true; do
        sleep 2
        if [[ ! -r "$net_dev" ]]; then
            echo "⚠ 进程 $pid 已退出"
            break
        fi

        local rx1 tx1
        rx1=$(awk 'NR>2 {rx+=$2} END {printf "%.0f", rx+0}' "$net_dev")
        tx1=$(awk 'NR>2 {tx+=$10} END {printf "%.0f", tx+0}' "$net_dev")

        local rx_delta=$((rx1 - rx0))
        local tx_delta=$((tx1 - tx0))
        local rx_rate=$((rx_delta / 2))  # bytes/s
        local tx_rate=$((tx_delta / 2))

        # 转换为人类可读（MB/s）
        local rx_mb=$(awk "BEGIN {printf \"%.2f\", $rx_rate/1024/1024}")
        local tx_mb=$(awk "BEGIN {printf \"%.2f\", $tx_rate/1024/1024}")

        printf "[%s] %-25s  ↓ %8s MB/s  ↑ %8s MB/s  (Δ: ↓%'d ↑%'d bytes/2s)\n" \
            "$(date +%H:%M:%S)" "$name" "$rx_mb" "$tx_mb" "$rx_delta" "$tx_delta"

        rx0=$rx1
        tx0=$tx1
    done
}

# 5. 如果有 nethogs，优先用它（更精确，按进程实时抓包统计）
if command -v nethogs >/dev/null 2>&1 && [[ $EUID -eq 0 ]]; then
    echo "✓ 使用 nethogs 实时监控（需 root）"
    echo "   格式: [进程] 发送速率 / 接收速率"
    echo ""
    PIDS="$MAIN_PID"
    [[ -n "$ARIA2_PID" ]] && PIDS="$PIDS $ARIA2_PID"
    exec nethogs -t -d 2 -p "$PIDS" 2>/dev/null || {
        echo "⚠ nethogs 执行失败，降级使用 /proc 方案"
        sleep 1
    }
fi

# 6. 降级：读 /proc/[pid]/net/dev（仅统计进程所在 netns 的流量，不如 nethogs 精确但无需 root）
echo "--- 使用 /proc/[pid]/net/dev 监控（2秒采样间隔）---"
echo "注意：此方案统计进程所在网络命名空间的*全部*流量，若容器/虚拟网卡复用则不够精确"
echo ""

if [[ -n "$ARIA2_PID" ]]; then
    echo "开始监控 2 个进程..."
    monitor_proc_net "$MAIN_PID" "TelDriveManager" &
    MAIN_MON_PID=$!
    monitor_proc_net "$ARIA2_PID" "aria2c" &
    ARIA2_MON_PID=$!

    trap "kill $MAIN_MON_PID $ARIA2_MON_PID 2>/dev/null; exit" INT TERM
    wait
else
    echo "开始监控 TelDriveManager 主进程..."
    monitor_proc_net "$MAIN_PID" "TelDriveManager"
fi
