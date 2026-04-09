"""数据库模块 — SQLite 异步操作（连接池模式）"""

import json
import aiosqlite
from pathlib import Path
from typing import Any, Optional
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

CREATE_PROGRESS_LOGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS progress_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream TEXT NOT NULL DEFAULT 'pikpak',
    job_id TEXT,
    message_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_PARSE_JOBS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS parse_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    request_payload TEXT NOT NULL DEFAULT '{}',
    result_payload TEXT,
    error TEXT,
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
    await conn.execute(CREATE_PROGRESS_LOGS_TABLE_SQL)
    await conn.execute(CREATE_PARSE_JOBS_TABLE_SQL)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tasks_gid ON tasks(aria2_gid)")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_progress_logs_stream_id ON progress_logs(stream, id DESC)")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_parse_jobs_type_created ON parse_jobs(job_type, created_at DESC)")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_parse_jobs_status_updated ON parse_jobs(status, updated_at DESC)")
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


async def _fetchone_dict(query: str, params: tuple = ()) -> Optional[dict]:
    conn = await _get_conn()
    async with conn.execute(query, params) as cursor:
        row = await cursor.fetchone()
        return dict(row) if row else None


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _json_loads(value: Any, default=None):
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _deserialize_parse_job(row: dict | None) -> Optional[dict]:
    if not row:
        return None
    item = dict(row)
    item["request_payload"] = _json_loads(item.get("request_payload"), {}) or {}
    item["result_payload"] = _json_loads(item.get("result_payload"), None)
    return item


def _deserialize_progress_log(row: dict | None) -> Optional[dict]:
    if not row:
        return None
    item = dict(row)
    item["payload"] = _json_loads(item.get("payload"), {}) or {}
    return item


async def add_progress_log(message_type: str, payload: dict, stream: str = "pikpak",
                           job_id: str | None = None, limit: int | None = None) -> dict:
    conn = await _get_conn()
    cursor = await conn.execute(
        "INSERT INTO progress_logs (stream, job_id, message_type, payload) VALUES (?, ?, ?, ?)",
        (stream, job_id, message_type, _json_dumps(payload)),
    )
    await conn.commit()
    if limit is not None:
        await prune_progress_logs(limit, stream=stream)
    row = await _fetchone_dict("SELECT * FROM progress_logs WHERE id = ?", (cursor.lastrowid,))
    return _deserialize_progress_log(row) or {}


async def get_progress_logs(stream: str | None = None, limit: int | None = None) -> list:
    conn = await _get_conn()
    params: list[Any] = []
    where_sql = ""
    if stream:
        where_sql = "WHERE stream = ?"
        params.append(stream)

    if limit is not None:
        sql = (
            "SELECT * FROM (SELECT * FROM progress_logs "
            f"{where_sql} ORDER BY id DESC LIMIT ?) ORDER BY id ASC"
        )
        params.append(max(1, int(limit)))
    else:
        sql = f"SELECT * FROM progress_logs {where_sql} ORDER BY id ASC"

    async with conn.execute(sql, tuple(params)) as cursor:
        rows = await cursor.fetchall()
        return [item for item in (_deserialize_progress_log(dict(row)) for row in rows) if item]


async def clear_progress_logs(stream: str | None = None, message_type: str | None = None) -> int:
    conn = await _get_conn()
    params: list[Any] = []
    where_parts: list[str] = []
    if stream:
        where_parts.append("stream = ?")
        params.append(stream)
    if message_type:
        where_parts.append("message_type = ?")
        params.append(message_type)

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    cursor = await conn.execute(f"DELETE FROM progress_logs{where_sql}", tuple(params))
    await conn.commit()
    return cursor.rowcount or 0


async def prune_progress_logs(limit: int, stream: str | None = None) -> None:
    normalized_limit = max(1, int(limit or 1))
    conn = await _get_conn()
    if stream:
        await conn.execute(
            "DELETE FROM progress_logs WHERE stream = ? AND id NOT IN (SELECT id FROM progress_logs WHERE stream = ? ORDER BY id DESC LIMIT ?)",
            (stream, stream, normalized_limit),
        )
    else:
        await conn.execute(
            "DELETE FROM progress_logs WHERE id NOT IN (SELECT id FROM progress_logs ORDER BY id DESC LIMIT ?)",
            (normalized_limit,),
        )
    await conn.commit()


async def create_parse_job(job_id: str, job_type: str, request_payload: dict,
                           status: str = "pending") -> dict:
    conn = await _get_conn()
    await conn.execute(
        "INSERT OR REPLACE INTO parse_jobs (job_id, job_type, status, request_payload, result_payload, error, updated_at) VALUES (?, ?, ?, ?, NULL, NULL, CURRENT_TIMESTAMP)",
        (job_id, job_type, status, _json_dumps(request_payload)),
    )
    await conn.commit()
    return await get_parse_job(job_id)


async def get_parse_job(job_id: str) -> Optional[dict]:
    row = await _fetchone_dict("SELECT * FROM parse_jobs WHERE job_id = ?", (job_id,))
    return _deserialize_parse_job(row)


async def get_active_parse_job() -> Optional[dict]:
    row = await _fetchone_dict(
        "SELECT * FROM parse_jobs WHERE status IN ('pending', 'running') ORDER BY updated_at DESC, created_at DESC LIMIT 1"
    )
    return _deserialize_parse_job(row)


async def get_latest_parse_job(job_type: str) -> Optional[dict]:
    row = await _fetchone_dict(
        "SELECT * FROM parse_jobs WHERE job_type = ? ORDER BY updated_at DESC, created_at DESC LIMIT 1",
        (job_type,),
    )
    return _deserialize_parse_job(row)


async def update_parse_job(job_id: str, **kwargs) -> Optional[dict]:
    if not kwargs:
        return await get_parse_job(job_id)

    normalized: dict[str, Any] = {}
    for key, value in kwargs.items():
        if key in {"request_payload", "result_payload"}:
            normalized[key] = None if value is None else _json_dumps(value)
        else:
            normalized[key] = value

    fields = ", ".join(f"{k} = ?" for k in normalized)
    values = list(normalized.values())
    values.append(job_id)
    conn = await _get_conn()
    await conn.execute(
        f"UPDATE parse_jobs SET {fields}, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
        values,
    )
    await conn.commit()
    return await get_parse_job(job_id)


async def fail_active_parse_jobs(reason: str) -> int:
    conn = await _get_conn()
    cursor = await conn.execute(
        "UPDATE parse_jobs SET status = 'failed', error = ?, updated_at = CURRENT_TIMESTAMP WHERE status IN ('pending', 'running')",
        (reason,),
    )
    await conn.commit()
    return cursor.rowcount or 0
