"""Host-environment probe for the installed uni-db wheel variant.

Run via the CLI:

    python -m uni_db check       # verify host has the runtime deps for this wheel
    python -m uni_db recommend   # suggest the best uni-db wheel for this host

Or programmatically:

    from uni_db import probe, recommend
    result = probe()
    print(result["variant"])
    for check in result["checks"]:
        print(check["name"], check["status"], check.get("detail", ""))

The probe is intentionally pure-Python and dependency-free so it can run
even when the underlying ML stack fails to load. It does not exercise any
model — it only inspects host paths and tries `ctypes.CDLL` on relevant
shared libraries.
"""

from __future__ import annotations

import ctypes
import platform
import subprocess
import sys
from typing import Any

try:
    from uni_db._variant import VARIANT
except ImportError:
    # Defensive: if _variant.py somehow isn't shipped (developer build,
    # corrupt install), fall back to a sentinel that prevents host-check
    # decisions from being silently wrong.
    VARIANT = "uni-db-unknown"


def _detect_nvidia_driver() -> dict[str, Any]:
    """Try running nvidia-smi to detect the NVIDIA driver version."""
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
            stderr=subprocess.DEVNULL,
            timeout=5,
            text=True,
        )
        version = out.strip().splitlines()[0]
        return {
            "name": "NVIDIA driver",
            "status": "ok",
            "detail": f"driver {version}",
        }
    except (FileNotFoundError, subprocess.SubprocessError, OSError):
        return {
            "name": "NVIDIA driver",
            "status": "missing",
            "detail": (
                "nvidia-smi not found or not runnable. Install the NVIDIA driver "
                "or pick a non-CUDA wheel (e.g. uni-db instead of uni-db-cuda)."
            ),
        }


def _detect_cudnn() -> dict[str, Any]:
    """Try to dlopen libcudnn (any version >= 9)."""
    candidates = ("libcudnn.so.9", "libcudnn.9.dylib", "cudnn64_9.dll")
    for libname in candidates:
        try:
            ctypes.CDLL(libname)
            return {
                "name": "cuDNN >= 9",
                "status": "ok",
                "detail": f"{libname} loaded",
            }
        except OSError:
            continue
    return {
        "name": "cuDNN >= 9",
        "status": "missing",
        "detail": (
            "libcudnn.so.9 (or platform equivalent) not found on the loader path. "
            "Install cuDNN 9 from https://developer.nvidia.com/cudnn or set "
            "LD_LIBRARY_PATH to the directory that contains it (typically "
            "/usr/local/cuda-X.X/targets/<arch>/lib/)."
        ),
    }


def _detect_metal() -> dict[str, Any]:
    if platform.system() != "Darwin":
        return {
            "name": "Apple Metal",
            "status": "n/a",
            "detail": "Metal is macOS-only — this wheel will not work here",
        }
    return {
        "name": "Apple Metal",
        "status": "ok",
        "detail": "Metal framework is part of macOS; no host setup needed",
    }


def _check_extension_loads() -> dict[str, Any]:
    """Verify the Rust extension imports."""
    try:
        from uni_db import _uni_db  # noqa: F401

        return {
            "name": "uni_db._uni_db extension",
            "status": "ok",
            "detail": "imports successfully",
        }
    except ImportError as e:
        return {
            "name": "uni_db._uni_db extension",
            "status": "error",
            "detail": str(e),
        }


def _variant_needs_cuda(variant: str) -> bool:
    return variant.endswith("-cuda")


def _variant_needs_metal(variant: str) -> bool:
    return variant.endswith("-metal")


def probe() -> dict[str, Any]:
    """Return a host-environment report for the installed wheel variant.

    Always reports:
      - variant   the wheel package name (e.g. "uni-db-cuda")
      - platform  e.g. "Linux x86_64"
      - python_version
      - checks    list of dicts with "name", "status", optional "detail"

    "status" is one of: "ok", "missing", "error", "n/a".

    Extra checks run conditionally based on the variant's suffix:
      *-cuda variants check for NVIDIA driver and cuDNN >= 9
      *-metal variants confirm running on macOS
    """
    checks = [_check_extension_loads()]
    if _variant_needs_cuda(VARIANT):
        checks.append(_detect_nvidia_driver())
        checks.append(_detect_cudnn())
    if _variant_needs_metal(VARIANT):
        checks.append(_detect_metal())

    return {
        "variant": VARIANT,
        "platform": f"{platform.system()} {platform.machine()}",
        "python_version": sys.version.split()[0],
        "checks": checks,
    }


def recommend() -> str:
    """Suggest the best uni-db wheel for this host.

    Pure host-level reasoning — does not depend on which variant is
    currently installed.
    """
    sys_name = platform.system()
    machine = platform.machine().lower()
    has_nvidia = _detect_nvidia_driver()["status"] == "ok"

    if sys_name == "Darwin" and machine in ("arm64", "aarch64"):
        return "uni-db-metal"
    if sys_name == "Darwin":
        # Intel Macs are not a wheel-matrix target for the GPU variants.
        return "uni-db"
    if sys_name in ("Linux", "Windows") and has_nvidia:
        return "uni-db-cuda"
    if sys_name in ("Linux", "Windows"):
        return "uni-db"
    return "uni-db"


_STATUS_MARKER = {
    "ok": "[ OK ]",
    "missing": "[FAIL]",
    "error": "[FAIL]",
    "n/a": "[ -- ]",
}


def format_report(result: dict[str, Any]) -> str:
    """Pretty-print the probe result."""
    lines = [
        f"uni-db variant: {result['variant']}",
        f"Platform:       {result['platform']}",
        f"Python:         {result['python_version']}",
        "",
        "Host checks:",
    ]
    for check in result["checks"]:
        marker = _STATUS_MARKER.get(check["status"], "[ ?? ]")
        detail = check.get("detail", "")
        lines.append(f"  {marker} {check['name']}")
        if detail:
            for detail_line in detail.split("\n"):
                lines.append(f"         {detail_line}")
    return "\n".join(lines)
