#!/usr/bin/env python3
"""Validate the exact Canvas archive/checksum pair before release publication."""
import hashlib
import re
import sys
from pathlib import Path


def validate(root: Path, version: str, commit: str) -> str:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", version):
        raise ValueError("invalid Canvas release version")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Canvas source must be a full commit ID")
    name = f"mitsuro-canvas-{version}-alpha-x86_64-{commit[:12]}.tar.gz"
    archives = list(root.rglob(name))
    manifests = list(root.rglob(name + ".sha256"))
    if len(archives) != 1 or len(manifests) != 1:
        raise ValueError("expected exactly one Canvas archive and checksum for this version/commit")
    archive, manifest = archives[0], manifests[0]
    if archive.is_symlink() or manifest.is_symlink() or not archive.is_file() or not manifest.is_file():
        raise ValueError("Canvas assets must be regular files")
    if archive.parent != manifest.parent or manifest.stat().st_size > 1024:
        raise ValueError("Canvas checksum must be bounded and alongside its archive")
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if manifest.read_bytes() != f"{digest.hexdigest()}  {name}\n".encode():
        raise ValueError("Canvas checksum or archive filename does not match")
    return name


if __name__ == "__main__":
    try:
        if len(sys.argv) != 4:
            raise ValueError("usage: check-canvas-release.py ARTIFACT_DIR VERSION FULL_COMMIT")
        print(validate(Path(sys.argv[1]), sys.argv[2], sys.argv[3]))
    except (ValueError, OSError) as error:
        sys.exit(str(error))
