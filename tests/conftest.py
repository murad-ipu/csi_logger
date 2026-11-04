"""Test setup: import csi_logger modules and the ESPectre repository."""

import sys
from pathlib import Path

import pytest

TOOL_DIR = Path(__file__).resolve().parent.parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

import espectre_path  # noqa: E402

try:
    REPO = espectre_path.setup()
except SystemExit as exc:  # repo not cloned next to csi_logger and no ESPECTRE_REPO
    pytest.exit(str(exc), returncode=2)


@pytest.fixture
def repo_root():
    return REPO
