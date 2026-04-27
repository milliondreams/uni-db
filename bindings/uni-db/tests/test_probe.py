"""Tests for the uni-db host probe (`uni_db._probe`).

The probe is intentionally pure-Python and dependency-free, so these
tests don't need the `_uni_db` Rust extension to run. They exercise:

  - VARIANT identification
  - probe() report shape
  - recommend() reasoning per (system, machine, has_nvidia) tuple
  - format_report rendering

CUDA / Metal / NVIDIA-driver detection delegate to OS facilities
(`subprocess`, `ctypes.CDLL`); we don't mock them here — the probe is
explicitly designed to give honest answers about the actual host.
"""

from __future__ import annotations

import platform
from unittest.mock import patch

import pytest

from uni_db._probe import (
    _detect_metal,
    _variant_needs_cuda,
    _variant_needs_metal,
    format_report,
    probe,
    recommend,
)


def test_variant_classifier_cuda():
    assert _variant_needs_cuda("uni-db-fastembed-cuda")
    assert _variant_needs_cuda("uni-db-all-cuda")
    assert _variant_needs_cuda("uni-db-mistralrs-cuda")
    assert not _variant_needs_cuda("uni-db")
    assert not _variant_needs_cuda("uni-db-fastembed")
    assert not _variant_needs_cuda("uni-db-all-metal")


def test_variant_classifier_metal():
    assert _variant_needs_metal("uni-db-fastembed-metal")
    assert _variant_needs_metal("uni-db-all-metal")
    assert _variant_needs_metal("uni-db-mistralrs-metal")
    assert not _variant_needs_metal("uni-db")
    assert not _variant_needs_metal("uni-db-fastembed-cuda")


def test_probe_returns_required_keys():
    result = probe()
    assert "variant" in result
    assert "platform" in result
    assert "python_version" in result
    assert "checks" in result
    assert isinstance(result["checks"], list)
    assert len(result["checks"]) >= 1


def test_probe_always_checks_extension():
    result = probe()
    names = [c["name"] for c in result["checks"]]
    assert "uni_db._uni_db extension" in names


def test_probe_checks_have_required_keys():
    result = probe()
    for check in result["checks"]:
        assert "name" in check
        assert "status" in check
        assert check["status"] in ("ok", "missing", "error", "n/a")


def test_format_report_includes_variant_and_platform():
    result = probe()
    out = format_report(result)
    assert result["variant"] in out
    assert result["python_version"] in out
    assert "Host checks:" in out


def test_format_report_marks_each_check_with_status():
    result = probe()
    out = format_report(result)
    for check in result["checks"]:
        assert check["name"] in out


def test_detect_metal_off_macos_returns_na():
    if platform.system() == "Darwin":
        pytest.skip("only meaningful on non-macOS hosts")
    result = _detect_metal()
    assert result["status"] == "n/a"
    assert "macOS" in result["detail"]


@pytest.mark.parametrize(
    "system, machine, has_nvidia, expected",
    [
        ("Darwin", "arm64", False, "uni-db-all-metal"),
        ("Darwin", "aarch64", False, "uni-db-all-metal"),
        ("Darwin", "x86_64", False, "uni-db-all"),
        ("Linux", "x86_64", True, "uni-db-all-cuda"),
        ("Linux", "x86_64", False, "uni-db-all"),
        ("Linux", "aarch64", False, "uni-db-all"),
        ("Windows", "x86_64", True, "uni-db-all-cuda"),
        ("Windows", "x86_64", False, "uni-db-all"),
    ],
)
def test_recommend_per_host_profile(system, machine, has_nvidia, expected):
    nvidia_result = {
        "name": "NVIDIA driver",
        "status": "ok" if has_nvidia else "missing",
        "detail": "",
    }
    with (
        patch("uni_db._probe.platform.system", return_value=system),
        patch("uni_db._probe.platform.machine", return_value=machine),
        patch("uni_db._probe._detect_nvidia_driver", return_value=nvidia_result),
    ):
        assert recommend() == expected
