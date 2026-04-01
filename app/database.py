"""数据库模块 — SQLite 异步操作（连接池模式）"""

import aiosqlite
from pathlib import Path
from typing import Optional
import logging
import asyncio

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "tasks.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    filename TEXT,
    status TEXT DEFAULT 'pending',
    download_progress REAL DEFAULT 0.0,
    upload_progress REAL DEFAULT 0.0,
    download_speed TEXT DEFAULT '',
    upload_speed TEXT DEFAULT '',
    file_size TEXT DEFAULT '',
    error TEXT,
    teldrive_path TEXT DEFAULT '/',
    aria2_gid TEXT,
    local_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# 全局连接实例
_db_conn: Optional[aiosqlite.Connection] = None


async def _get_conn() -> aiosqlite.Connection:
    """获取或创建全局数据库连接"""
    global _db_conn
    if _db_conn is None:
        _db_conn = await aiosqlite.connect(str(DB_PATH))
        _db_conn.row_factory = aiosqlite.Row
        await _db_conn.execute("PRAGMA journal_mode=WAL")
        await _db_conn.execute("PRAGMA synchronous=NORMAL")
    return _db_conn


async def reconnect_db():
    """强制重建数据库连接"""
    global _db_conn
    if _db_conn is not None:
        try:
            await _db_conn.close()
        except Exception:
            pass
        _db_conn = None
    return await _get_conn()


async def init_db():
    """初始化数据库"""
    conn = await _get_conn()
    await conn.execute(CREATE_TABLE_SQL)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tasks_gid ON tasks(aria2_gid)")
    await conn.commit()


async def close_db():
    """关闭数据库连接"""
    global _db_conn
    if _db_conn is not None:
        await _db_conn.close()
        _db_conn = None


async def add_task(task_id: str, url: str, filename: str = None,
                   teldrive_path: str = "/") -> dict:
    """添加新任务"""
    conn = await _get_conn()
    await conn.execute(
        """INSERT OR IGNORE INTO tasks (task_id, url, filename, teldrive_path)
           VALUES (?, ?, ?, ?)""",
        (task_id, url, filename, teldrive_path)
    )
    await conn.commit()
    return await get_task(task_id)


async def get_task(task_id: str) -> Optional[dict]:
    """获取单个任务"""
    conn = await _get_conn()
    async with conn.execute(
        "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            return dict(row)
    return None


async def get_all_tasks() -> list:
    """获取所有任务"""
    conn = await _get_conn()
    async with conn.execute(
        "SELECT * FROM tasks ORDER BY created_at DESC"
    ) as cursor:
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def update_task(task_id: str, **kwargs) -> None:
    """更新任务字段"""
    if not kwargs:
        return
    fields = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values())
    values.append(task_id)
    conn = await _get_conn()
    await conn.execute(
        f"UPDATE tasks SET {fields}, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?",
        values
    )
    await conn.commit()


async def delete_task(task_id: str) -> bool:
    """删除任务记录"""
    conn = await _get_conn()
    cursor = await conn.execute(
        "DELETE FROM tasks WHERE task_id = ?", (task_id,)
    )
    await conn.commit()
    return cursor.rowcount > 0


async def get_active_tasks() -> list:
    """获取所有活跃任务"""
    conn = await _get_conn()
    async with conn.execute(
        "SELECT * FROM tasks WHERE status IN ('pending', 'downloading', 'uploading')"
    ) as cursor:
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_task_by_gid(gid: str) -> Optional[dict]:
    """按 aria2 GID 查询任务"""
    conn = await _get_conn()
    async with conn.execute(
        "SELECT * FROM tasks WHERE aria2_gid = ?", (gid,)
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            return dict(row)
    return None
