"""Shared remaining-write reservations for local transfers in this process."""

import shutil
import threading
from pathlib import Path


class DiskSpaceUnavailable(RuntimeError):
    pass


class DiskBudget:
    def __init__(self):
        self._lock = threading.RLock()
        self._reservations = {}

    @staticmethod
    def filesystem(path):
        target = Path(path).resolve()
        while not target.exists():
            parent = target.parent
            if parent == target:
                raise OSError(f"Cannot inspect download filesystem: {path}")
            target = parent
        return target.stat().st_dev, target

    def reserve(self, owner, path, remaining, reserve_bytes):
        remaining = max(0, int(remaining))
        with self._lock:
            volume, target = self.filesystem(path)
            free = shutil.disk_usage(target).free
            peers = [entry for key, entry in self._reservations.items()
                     if key != owner and entry[0] == volume]
            floor = max([int(reserve_bytes)] + [entry[2] for entry in peers])
            reserved = sum(entry[1] for entry in peers)
            if free - reserved - remaining < floor:
                raise DiskSpaceUnavailable(
                    f"磁盘空间不足，等待上传释放空间: 可用 {free}, 已预留 {reserved}, "
                    f"本任务还需 {remaining}, 保留空间 {floor} bytes"
                )
            self._reservations[owner] = (volume, remaining, int(reserve_bytes))

    def release(self, owner):
        with self._lock:
            self._reservations.pop(owner, None)

    def available(self, owner, path, reserve_bytes):
        """Bytes available to this owner after other writers and the safety floor."""
        with self._lock:
            volume, target = self.filesystem(path)
            peers = [entry for key, entry in self._reservations.items()
                     if key != owner and entry[0] == volume]
            floor = max([int(reserve_bytes)] + [entry[2] for entry in peers])
            return shutil.disk_usage(target).free - floor - sum(entry[1] for entry in peers)

    def track(self, owner, path, remaining, reserve_bytes):
        """Account for an existing writer until its pause has been confirmed."""
        with self._lock:
            volume, _ = self.filesystem(path)
            self._reservations[owner] = (volume, max(0, int(remaining)), int(reserve_bytes))

    def retain(self, prefix, owners):
        with self._lock:
            for owner in list(self._reservations):
                if owner.startswith(prefix) and owner not in owners:
                    self._reservations.pop(owner, None)


disk_budget = DiskBudget()
