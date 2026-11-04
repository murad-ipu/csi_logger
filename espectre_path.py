"""Locate the ESPectre repository and make its Python tools importable.

Everything ESPectre-specific (CSI V8 parser, Direct raw-CSI client, detectors,
temporal replay) is imported from the repository instead of being copied here.
"""

import os
import sys
from pathlib import Path

# Default layout: D:\...\ESPectre\csi_logger next to D:\...\ESPectre\espectre
DEFAULT_REPO = Path(__file__).resolve().parent.parent / "espectre"


def setup(repo=None) -> Path:
    """Put the repository on sys.path and run its own path bootstrap.

    Resolution order: explicit ``repo`` argument, ESPECTRE_REPO environment
    variable, then ../espectre relative to this folder.
    """
    root = Path(repo or os.environ.get("ESPECTRE_REPO") or DEFAULT_REPO).resolve()
    if not (root / "tools" / "lib" / "csi_io.py").is_file():
        raise SystemExit(
            f"ESPectre repository not found at {root}.\n"
            "Pass --repo <path to espectre clone> or set ESPECTRE_REPO."
        )
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    # The repo's bootstrap adds src/python and src/python/micro_espectre, which
    # the detectors need for `import config` and `import detector_interface`.
    import tools.lib.bootstrap  # noqa: F401
    return root
