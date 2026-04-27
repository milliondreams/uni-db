"""CLI entry point for the uni-db host probe.

Usage:

    python -m uni_db check       # verify host has the runtime deps
    python -m uni_db recommend   # suggest the best uni-db wheel
    python -m uni_db help        # show this help

Exit codes for `check`:
    0   all checks passed
    1   one or more checks reported `missing` or `error`
    2   unknown subcommand
"""

from __future__ import annotations

import sys

from uni_db._probe import format_report, probe, recommend


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]

    if not args or args[0] in ("check", "probe"):
        result = probe()
        print(format_report(result))
        any_failed = any(c["status"] in ("missing", "error") for c in result["checks"])
        return 1 if any_failed else 0

    if args[0] == "recommend":
        print(recommend())
        return 0

    if args[0] in ("-h", "--help", "help"):
        print(__doc__)
        return 0

    print(f"Unknown subcommand: {args[0]}", file=sys.stderr)
    print(
        "Try `python -m uni_db check` or `python -m uni_db recommend`.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
