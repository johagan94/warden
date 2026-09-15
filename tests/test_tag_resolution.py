"""Tests for deferred, fail-safe tag resolution.

Tag IDs must be resolved only after connectivity is verified — never in __init__,
where a still-booting *arr would fail the fetch and silently disable tag filtering
(letting tag-excluded media through). Style matches test_core.py: real clients with a
monkeypatched session, plain asserts.
"""

import requests

from warden import main
from warden.clients.arr import SonarrClient


class _TagResponse:
    def __init__(self, tags: list[dict]) -> None:
        self._tags = tags

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict]:
        return self._tags


def _sonarr(**search: object) -> SonarrClient:
    return SonarrClient("sonarr", "http://sonarr:8989", "abc123", {"stagger_interval_seconds": 0, **search}, {})


class TestDeferredResolution:
    def test_init_does_not_resolve_tags(self) -> None:
        # Even with filters configured, construction must not touch the network.
        client = _sonarr(exclude_tags=["anime"])
        assert client.tags_configured is True
        assert client._tags_resolved is False
        assert client._exclude_tag_ids == set()

    def test_resolve_tags_populates_ids(self) -> None:
        client = _sonarr(exclude_tags=["anime"])
        client.session.get = lambda url, *, timeout: _TagResponse([{"id": 7, "label": "Anime"}])

        assert client.resolve_tags() is True
        assert client._exclude_tag_ids == {7}
        assert client._tags_resolved is True

    def test_resolve_tags_is_noop_when_unconfigured(self) -> None:
        client = _sonarr()

        def _fail(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("must not fetch tags when none are configured")

        client.session.get = _fail
        assert client.resolve_tags() is True
        assert client.tags_configured is False

    def test_failed_fetch_returns_false_and_is_retryable(self) -> None:
        client = _sonarr(exclude_tags=["anime"])

        def _down(url: str, *, timeout: int) -> object:
            raise requests.ConnectionError("connection refused")

        client.session.get = _down
        assert client.resolve_tags() is False
        assert client._tags_resolved is False
        assert client._exclude_tag_ids == set()

        # A later attempt (e.g. once the *arr has finished booting) still resolves.
        client.session.get = lambda url, *, timeout: _TagResponse([{"id": 7, "label": "Anime"}])
        assert client.resolve_tags() is True
        assert client._exclude_tag_ids == {7}


class _FakeClient:
    def __init__(self, name: str, *, connects: bool = True, resolves: bool = True) -> None:
        self.name = name
        self._connects = connects
        self._resolves = resolves
        self.resolve_calls = 0

    def check_connection(self) -> bool:
        return self._connects

    def resolve_tags(self) -> bool:
        self.resolve_calls += 1
        return self._resolves


class TestVerifyArrClients:
    def test_keeps_client_when_tags_resolve(self, monkeypatch) -> None:
        monkeypatch.setattr(main.time, "sleep", lambda *_: None)
        ok = _FakeClient("ok")
        assert main.verify_arr_clients([ok]) == [ok]
        assert ok.resolve_calls == 1

    def test_skips_client_with_unresolvable_tags(self, monkeypatch) -> None:
        # Fail closed: a reachable instance whose configured tags never load is dropped
        # rather than searched with filtering disabled.
        monkeypatch.setattr(main.time, "sleep", lambda *_: None)
        ok = _FakeClient("ok")
        bad = _FakeClient("bad", resolves=False)
        assert main.verify_arr_clients([ok, bad]) == [ok]
        assert bad.resolve_calls == main._MAX_CONNECTION_ATTEMPTS

    def test_unreachable_client_is_skipped_before_tag_resolution(self, monkeypatch) -> None:
        monkeypatch.setattr(main.time, "sleep", lambda *_: None)
        down = _FakeClient("down", connects=False)
        assert main.verify_arr_clients([down]) == []
        assert down.resolve_calls == 0
