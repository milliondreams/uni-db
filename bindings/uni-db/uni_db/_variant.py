"""Identifies which uni-db wheel variant this is.

This module is rewritten per-variant by `scripts/bootstrap-wheel-variants.sh`
when a wheel is built. The canonical value below is for the base `uni-db`
wheel. The probe (`uni_db._probe`) reads `VARIANT` to decide which host
checks apply.
"""

VARIANT = "uni-db"
