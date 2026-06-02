"""Tests for Sonarr per-tag search limits and fetch_record_limit capping.

Matches the style of test_core.py: real clients with monkeypatched
_fetch_list / session, plain asserts, try/except for rejection cases.
"""

from warden.clients.arr import RadarrClient, SonarrClient
from warden.searcher import run_search_cycle
from warden.validators import _validate_tag_limits


def _series(series_id: int, *, tags: list[int], missing: bool = True, monitored: bool = True) -> dict:
    episode_count = 10
    file_count = 5 if missing else episode_count
    return {
        "id": series_id,
        "title": f"Series {series_id}",
        "sortTitle": f"series {series_id:04d}",
        "monitored": monitored,
        "tags": tags,
        "statistics": {"episodeCount": episode_count, "episodeFileCount": file_count},
    }


def _make_sonarr(tag_limit_ids: dict[int, int], **search) -> SonarrClient:
    # Construct without tag_limits in settings so __init__ does not attempt a tag
    # lookup over the network, then inject resolved tag-id limits directly.
    client = SonarrClient(
        "sonarr",
        "http://sonarr:8989",
        "abc123",
        {"search_type": "series", "stagger_interval_seconds": 0, **search},
        {},
    )
    client._tag_limit_ids = tag_limit_ids
    return client


class TestTagLimitProperty:
    def test_uses_tag_limits_false_without_limits(self) -> None:
        assert _make_sonarr({}).uses_tag_limits is False

    def test_uses_tag_limits_true_with_limits(self) -> None:
        assert _make_sonarr({3: 10}).uses_tag_limits is True


class TestSonarrTagLimits:
    def test_caps_series_per_tag(self) -> None:
        client = _make_sonarr({3: 10})
        client._fetch_list = lambda endpoint, params=None: [_series(i, tags=[3]) for i in range(1, 26)]

        items = client.get_media_to_search(50, 0)

        assert len(items) == 10
        assert all(str(item[0]).startswith("series:") for item in items)
        assert all(item[1] == "missing" for item in items)

    def test_per_tag_caps_are_independent(self) -> None:
        client = _make_sonarr({3: 2, 4: 3})
        series = [_series(i, tags=[3]) for i in range(1, 6)] + [_series(100 + i, tags=[4]) for i in range(1, 6)]
        client._fetch_list = lambda endpoint, params=None: series

        items = client.get_media_to_search(50, 0)

        assert len(items) == 5  # 2 from tag 3 + 3 from tag 4

    def test_multi_tag_series_deduped_other_tag_fills_distinct(self) -> None:
        # A series carrying two capped tags is searched once; the second tag fills its
        # remaining cap with a distinct series rather than re-searching the shared one.
        client = _make_sonarr({3: 1, 4: 1})
        client._fetch_list = lambda endpoint, params=None: [
            _series(1, tags=[3, 4]),
            _series(2, tags=[3]),
            _series(3, tags=[4]),
        ]

        items = client.get_media_to_search(50, 0)
        ids = [item[0] for item in items]

        assert "series:1" in ids
        assert len(ids) == len(set(ids))  # no series searched twice in one cycle
        assert set(ids) == {"series:1", "series:3"}

    def test_cursor_rotates_through_backlog_across_cycles(self) -> None:
        # 25 eligible series, cap 10: cycle1 -> 1..10, cycle2 -> 11..20,
        # cycle3 -> wraps (21..25 then 1..5). Walks the whole tag, not the same top-N.
        client = _make_sonarr({3: 10})
        client._fetch_list = lambda endpoint, params=None: [_series(i, tags=[3]) for i in range(1, 26)]

        first = [item[0] for item in client.get_media_to_search(50, 0)]
        second = [item[0] for item in client.get_media_to_search(50, 0)]
        third = [item[0] for item in client.get_media_to_search(50, 0)]

        assert first == [f"series:{i}" for i in range(1, 11)]
        assert second == [f"series:{i}" for i in range(11, 21)]
        assert third == [f"series:{i}" for i in range(21, 26)] + [f"series:{i}" for i in range(1, 6)]

    def test_skips_complete_unmonitored_and_unlisted(self) -> None:
        client = _make_sonarr({3: 10})
        client._fetch_list = lambda endpoint, params=None: [
            _series(1, tags=[3]),  # eligible
            _series(2, tags=[3], missing=False),  # complete -> skip
            _series(3, tags=[3], monitored=False),  # unmonitored -> skip
            _series(4, tags=[99]),  # tag not in limits -> skip
            _series(5, tags=[]),  # untagged -> skip
        ]

        items = client.get_media_to_search(50, 0)

        assert [item[0] for item in items] == ["series:1"]

    def test_exclude_tag_drops_series(self) -> None:
        client = _make_sonarr({3: 10})
        client._exclude_tag_ids = {7}
        client._fetch_list = lambda endpoint, params=None: [
            _series(1, tags=[3]),
            _series(2, tags=[3, 7]),  # excluded
        ]

        items = client.get_media_to_search(50, 0)

        assert [item[0] for item in items] == ["series:1"]

    def test_missing_batch_zero_returns_nothing(self) -> None:
        # On an upgrade-only tick the searcher passes missing_batch_size=0.
        client = _make_sonarr({3: 10})
        client._fetch_list = lambda endpoint, params=None: [_series(1, tags=[3])]

        assert client.get_media_to_search(0, 0) == []

    def test_tag_limits_do_not_disable_upgrade_searches(self) -> None:
        client = _make_sonarr({3: 10})
        client._fetch_list = lambda endpoint, params=None: []
        client._fetch_unlimited = lambda endpoint: (
            [
                {
                    "id": 100,
                    "episodeId": 100,
                    "series": {"id": 55, "title": "Upgradeable Show", "tags": [3]},
                }
            ]
            if "cutoff" in endpoint
            else []
        )

        items = client.get_media_to_search(0, 5)

        assert items == [("series:55", "upgrade", "Upgradeable Show")]

    def test_emitted_series_items_trigger_series_search(self) -> None:
        client = _make_sonarr({3: 10})
        client._fetch_list = lambda endpoint, params=None: [_series(55, tags=[3])]
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        items = client.get_media_to_search(50, 0)
        client.trigger_search(items)

        assert items == [("series:55", "missing", "Series 55")]
        assert posts == [("http://sonarr:8989/api/v3/command", {"name": "SeriesSearch", "seriesId": 55}, 120)]


class TestTagLimitedSearchCycle:
    def test_searcher_bypasses_allocation_for_tag_limited_client(self) -> None:
        # 40 candidates but the global missing_batch_size is only 5; a tag-limited
        # client must NOT be trimmed by the cross-instance allocator.
        searched = []

        class TagLimitedClient:
            name = "sonarr"
            weight = 1
            uses_tag_limits = True

            def is_queue_too_large(self) -> bool:
                return False

            def get_media_to_search(self, missing_batch_size, upgrade_batch_size):
                return [(i, "missing", f"S{i}") for i in range(40)]

            def trigger_search(self, items, *, index=None, total=None) -> None:
                searched.extend(items)

        run_search_cycle(
            [TagLimitedClient()],
            {"missing_batch_size": 5, "upgrade_batch_size": 0, "stagger_interval_seconds": 0},
        )

        assert len(searched) == 40


class TestFetchRecordLimit:
    def test_fetch_record_limit_caps_records(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"fetch_record_limit": 3, "stagger_interval_seconds": 0},
            {},
        )

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"records": [{"id": i} for i in range(10)]}

        client.session.get = lambda url, *, params, timeout: Response()

        records = client._fetch_unlimited("/api/v3/wanted/missing")

        assert len(records) == 3

    def test_fetch_record_limit_zero_is_unlimited(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"fetch_record_limit": 0, "stagger_interval_seconds": 0},
            {},
        )

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"records": [{"id": i} for i in range(10)]}

        client.session.get = lambda url, *, params, timeout: Response()

        records = client._fetch_unlimited("/api/v3/wanted/missing")

        assert len(records) == 10


class TestTagLimitsValidation:
    def test_accepts_valid(self) -> None:
        _validate_tag_limits("tag_limits", {"anime": 10, "live-action": 5})

    def test_accepts_empty(self) -> None:
        _validate_tag_limits("tag_limits", {})

    def test_rejects_invalid(self) -> None:
        invalid = [
            {"anime": 0},  # below 1
            {"anime": -1},  # negative
            {"anime": True},  # bool is not a valid int limit
            {"anime": "10"},  # not an int
            {"": 10},  # empty tag name
            ["anime"],  # not a mapping
        ]
        for value in invalid:
            try:
                _validate_tag_limits("tag_limits", value)
            except ValueError:
                continue
            raise AssertionError(f"Expected ValueError for {value!r}")


class TestTagLimitsConfig:
    def test_schema_has_tag_limits_and_fetch_record_limit(self) -> None:
        from warden.config import SEARCH_SETTINGS_SCHEMA

        assert SEARCH_SETTINGS_SCHEMA["tag_limits"]["default"] == {}
        assert SEARCH_SETTINGS_SCHEMA["fetch_record_limit"]["default"] == 0

    def test_parse_config_accepts_instance_tag_limits(self) -> None:
        from warden.config import parse_config

        config = parse_config(
            {
                "instances": {
                    "MySonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "series",
                        "tag_limits": {"anime": 10, "live-action": 5},
                    },
                }
            }
        )
        assert config["instances"]["sonarr"][0]["tag_limits"] == {"anime": 10, "live-action": 5}

    def test_parse_config_rejects_bad_instance_tag_limits(self) -> None:
        from warden.config import parse_config

        try:
            parse_config(
                {
                    "instances": {
                        "MySonarr": {
                            "type": "sonarr",
                            "host": "http://sonarr:8989",
                            "api_key": "abc123",
                            "enabled": True,
                            "tag_limits": {"anime": 0},
                        },
                    }
                }
            )
        except ValueError:
            return
        raise AssertionError("Expected invalid instance tag_limits to be rejected")
