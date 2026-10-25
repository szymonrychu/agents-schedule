"""argparse front end with a command registry."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence

from agents_schedule import __version__

CommandFunc = Callable[[argparse.Namespace], int | None]

ConfigureFunc = Callable[[argparse.ArgumentParser], None]

_COMMANDS: dict[str, tuple[CommandFunc, str, ConfigureFunc | None]] = {}


def register(
    name: str,
    func: CommandFunc,
    help: str,  # noqa: A002
    configure: ConfigureFunc | None = None,
) -> None:
    """Register a subcommand; func receives the parsed args and returns the exit code.

    `configure` adds the subcommand's own arguments to its parser.
    """
    _COMMANDS[name] = (func, help, configure)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sched", description="agents-schedule")
    parser.add_argument("--version", action="version", version=f"sched {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, (func, help_text, configure) in _COMMANDS.items():
        cmd = sub.add_parser(name, help=help_text)
        cmd.set_defaults(func=func)
        if configure is not None:
            configure(cmd)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.command is None:
        parser.print_help(sys.stderr)
        raise SystemExit(2)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
