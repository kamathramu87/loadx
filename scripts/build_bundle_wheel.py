"""Build one wheel for bundle deployment without retaining older versions."""
# ruff: noqa: S603 -- executes only the fixed build command below
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
output = root / "dist" / "bundle"
output.mkdir(parents=True, exist_ok=True)
for wheel in output.glob("loadx-*.whl"):
    wheel.unlink()
subprocess.run(
    [
        sys.executable,
        "-m",
        "build",
        "--wheel",
        "--outdir",
        str(output),
        str(root),
    ],
    check=True,
)
