#!/usr/bin/env bash
set -euo pipefail

project=${1:-/opt/TelDriveManager}
service=${2:-teldrive-manager.service}
version=${3:-v1.1.0}

if [[ $EUID -ne 0 ]]; then
    echo 'Run this script with sudo.' >&2
    exit 1
fi
[[ $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Invalid release version.' >&2; exit 1; }
for tool in curl sha256sum python3 systemctl systemd-run; do
    command -v "$tool" >/dev/null || { echo "Missing required command: $tool" >&2; exit 1; }
done
project=$(realpath -e "$project")
[[ -f "$project/main.py" && -f "$project/config.toml" ]] || { echo 'Existing installation not found.' >&2; exit 1; }
python3 -c 'import shutil,sys; sys.exit(0 if shutil.disk_usage(sys.argv[1]).free >= 6 * 1024**3 else "At least 6 GiB free space is required")' "$project"
[[ ! -e "$project/.tdm-update-lock" ]] || { echo 'An update or recovery is already pending.' >&2; exit 1; }
systemctl is-active --quiet "$service" || { echo 'Start the existing service before upgrading.' >&2; exit 1; }

python="$project/venv/bin/python"
[[ -x "$python" ]] || python="$project/.venv/bin/python"
[[ -x "$python" ]] || { echo 'Expected venv/bin/python or .venv/bin/python in the installation.' >&2; exit 1; }
stage=$(mktemp -d "$project/.tdm-update-stage-linux-XXXXXXXX")
cleanup() {
    if [[ ! -e "$project/.tdm-update-lock" ]]; then
        rm -rf -- "$stage"
    fi
}
trap cleanup EXIT

archive="TelDriveManager-$version.zip"
base="https://github.com/MengStar-L/TelDriveManager/releases/download/$version"
curl --fail --location --retry 3 --max-time 300 --max-filesize 536870912 "$base/$archive" -o "$stage/$archive"
curl --fail --location --retry 3 --max-time 60 "$base/SHA256SUMS" -o "$stage/SHA256SUMS"
(cd "$stage" && sha256sum --check --ignore-missing SHA256SUMS)

"$python" - "$stage/$archive" "$stage" <<'PY'
import shutil
import stat
import sys
import zipfile
from pathlib import Path, PurePosixPath
archive, destination = map(Path, sys.argv[1:])
with zipfile.ZipFile(archive) as source:
    if sum(item.file_size for item in source.infolist()) > 1024 ** 3:
        raise RuntimeError('Release exceeds extraction limit')
    if shutil.disk_usage(destination).free < 6 * 1024 ** 3:
        raise RuntimeError('At least 6 GiB free space is required before extraction')
    for item in source.infolist():
        name = PurePosixPath(item.filename)
        if name.is_absolute() or '..' in name.parts or stat.S_ISLNK(item.external_attr >> 16):
            raise RuntimeError('Unsafe release path')
        source.extract(item, destination)
PY

"$python" "$stage/TelDriveManager/deploy/update-linux.py" \
    --project "$project" --service "$service" --version "$version" \
    --stage "$stage" --source "$stage/TelDriveManager"
