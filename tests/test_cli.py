import pytest

from agents_schedule import __version__, cli


def test_version_prints_package_version(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_registered_command_dispatches():
    seen = []

    def func(args):
        seen.append(args.value)
        return 7

    def configure(parser):
        parser.add_argument("--value", default="x")

    cli.register("probe", func, "probe command", configure)
    assert cli.main(["probe", "--value", "v"]) == 7
    assert seen == ["v"]


@pytest.mark.parametrize("argv", [[], ["nope"]])
def test_unknown_command_exits_2(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "usage" in (captured.out + captured.err).lower()
