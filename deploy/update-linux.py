"""Bootstrap the transactional updater on existing Linux/systemd installations."""

import argparse
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys
import time
import uuid


def main():
    parser = argparse.ArgumentParser()
    for name in ("project", "service", "version", "stage", "source"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("This updater requires Linux and root")
    if not re.fullmatch(r"[A-Za-z0-9_.@:-]+\.service", args.service):
        raise RuntimeError("Invalid systemd service name")
    project, stage, source = (Path(value).resolve() for value in (args.project, args.stage, args.source))
    if project not in stage.parents or stage not in source.parents or not stage.name.startswith(".tdm-update-stage-"):
        raise RuntimeError("Invalid staging directory")
    def service_property(name):
        return subprocess.check_output(["systemctl", "show", args.service, "--property=" + name, "--value"], text=True).strip()
    if service_property("User") not in ("", "root", "0") or Path(service_property("WorkingDirectory")).resolve() != project:
        raise RuntimeError("The service must run as root in the selected project")
    pid = int(service_property("MainPID"))
    if pid <= 0:
        raise RuntimeError("The existing service is not running")
    command = [os.fsdecode(value) for value in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0") if value]
    if len(command) < 2 or Path(command[1]).resolve() != project / "main.py" or not Path(command[0]).is_file():
        raise RuntimeError("The service MainPID must run python /path/to/main.py")
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    config = tomllib.loads((project / "config.toml").read_text(encoding="utf-8"))
    api = runpy.run_path(str(source / "update_worker.py"))
    token = uuid.uuid4().hex
    saved_worker = stage / "worker.py"
    api["copy_atomic"](source / "update_worker.py", saved_worker)
    files = [p.relative_to(source).as_posix() for p in source.rglob("*") if p.is_file()
             and not p.is_symlink() and not api["protected"](p.relative_to(source).as_posix())]
    manifest = {"version": args.version, "source_root": str(source), "files": files}
    state = dict(token=token, phase="preparing", version=args.version, stage=str(stage), worker=str(saved_worker),
                 backup=str(project / (".tdm-update-backup-" + token)), service=args.service,
                 parent_pid=pid, parent_identity=api["process_identity"](pid), restart_args=command,
                 port=int(config.get("server", {}).get("port", 8888)))
    api["ensure_install_space"](project, source, files)
    api["write_json"](stage / "manifest.json", manifest)
    api["acquire_update"](project, state)
    unit = "tdm-update-" + token
    # Persist the transaction before launching; an ambiguous launch failure keeps it recoverable.
    subprocess.run(["systemd-run", "--quiet", "--collect", "--unit=" + unit,
                    "--property=Type=exec", "--property=Restart=on-failure", "--property=RestartSec=15",
                    "--property=WorkingDirectory=" + str(project), sys.executable, str(saved_worker),
                    "--project", str(project)], check=True, timeout=30)
    print(f"Updating to {args.version}. Log: journalctl -fu {unit}", flush=True)
    deadline = time.monotonic() + 1800
    while (project / ".tdm-update-lock").exists():
        if time.monotonic() > deadline:
            raise RuntimeError(f"Update is still pending; inspect journalctl -u {unit}. Backups have been retained.")
        time.sleep(1)
    result = api["read_json"](project / ".tdm-update-result")
    if result.get("token") != token or result.get("outcome") != "committed":
        raise RuntimeError(result.get("error") or "Update failed; inspect the update log")
    print(f"Updated to {args.version}; service is ready. Configuration and data retained.")


if __name__ == "__main__":
    main()
