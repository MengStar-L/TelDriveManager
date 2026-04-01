"""数据模型 — Pydantic 模型定义"""

from pydantic import BaseModel
from typing import Optional


class TaskAddRequest(BaseModel):
    """添加任务请求"""
    url: str
    filename: Optional[str] = None
    teldrive_path: Optional[str] = "/"


class TaskResponse(BaseModel):
    """任务响应"""
    task_id: str
    url: str
    filename: Optional[str] = None
    status: str = "pending"
    download_progress: float = 0.0
    upload_progress: float = 0.0
    download_speed: str = ""
    upload_speed: str = ""
    file_size: str = ""
    error: Optional[str] = None
    teldrive_path: str = "/"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TestResult(BaseModel):
    """连接测试结果"""
    success: bool
    message: str
    version: Optional[str] = None
