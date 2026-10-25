"""Unit tests of the kind e2e's polling helpers (tests/e2e/test_e2e_kind.py), run without kind."""

from typing import Any

from e2e.test_e2e_kind import BASE, complete_sent_turns


class _Response:
    def __init__(self, body: Any) -> None:
        self.status_code = 200
        self.text = ""
        self._body = body

    def json(self) -> Any:
        return self._body


class _Api:
    """Answers GET /schedules/{id}/runs from a dict of runs per schedule."""

    def __init__(self, runs: dict[str, list[dict[str, Any]]]) -> None:
        self.runs = runs

    def get(self, path: str, **kw: Any) -> _Response:
        prefix, suffix = f"{BASE}/schedules/", "/runs"
        assert path.startswith(prefix) and path.endswith(suffix), path
        return _Response(self.runs[path.removeprefix(prefix).removesuffix(suffix)])


class _Fake:
    """Records completions; like the fake orchestrator's control route, a message id must be one."""

    def __init__(self) -> None:
        self.completed: list[str] = []

    def complete(self, message_id: str) -> None:
        assert message_id is not None, "completed a run whose prompt is not posted yet"
        self.completed.append(message_id)


def test_complete_sent_turns_waits_for_the_message_id():
    """A claimed run (in-progress) has no message_id until the dispatcher posts its prompt: it is
    completed on the poll after it gets one, and every turn is completed once."""
    claimed = {"status": "in-progress", "message_id": None}
    sent = {"status": "in-progress", "message_id": "7"}
    done = {"status": "handled", "message_id": "3"}
    api, fake, completed = _Api({"a": [claimed, sent], "b": [done]}), _Fake(), set()

    every = complete_sent_turns(api, fake, ["a", "b"], completed)
    assert every == [[claimed, sent], [done]]
    assert fake.completed == ["7"]

    claimed["message_id"] = "8"
    complete_sent_turns(api, fake, ["a", "b"], completed)
    assert fake.completed == ["7", "8"]
    assert completed == {"7", "8"}
