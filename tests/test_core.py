"""Tests for Warden unified package."""

from warden.cleaner import run_cleaner_loop, run_removal_cycle
from warden.clients.arr import CircuitBreaker, LidarrClient, QueueItem, RadarrClient, SonarrClient
from warden.config import CLEANUP_SETTINGS_SCHEMA, SEARCH_SETTINGS_SCHEMA, parse_config
from warden.main import build_arr_clients
from warden.searcher import _allocate_slots as allocate_search_slots
from warden.searcher import run_search_cycle
from warden.validators import classify_stall


class TestConfigParsing:
    def test_minimal_config(self) -> None:
        config = parse_config(
            {
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                }
            }
        )
        assert config["search_settings"]["run_interval_minutes"] == 60
        assert config["search_settings"]["missing_batch_size"] == 20
        assert config["cleanup_settings"]["batch_size"] == 10
        assert config["cleanup_settings"]["interval"] == 3600
        assert len(config["instances"]["radarr"]) == 1

    def test_custom_circuit_breaker(self) -> None:
        config = parse_config(
            {
                "global": {"circuit_breaker_threshold": 5},
                "killarr": {},
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                },
            }
        )
        assert config["search_settings"]["circuit_breaker_threshold"] == 5

    def test_section_aliases_vigilance_defence(self) -> None:
        # Canonical section names match WARDEN_MODE: vigilance=search, defence=cleanup.
        config = parse_config(
            {
                "vigilance": {"max_queue_size": 321},
                "defence": {"batch_size": 7},
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                },
            }
        )
        assert config["search_settings"]["max_queue_size"] == 321
        assert config["cleanup_settings"]["batch_size"] == 7

    def test_section_alias_precedence(self) -> None:
        # When both the new name and a legacy alias are present, the new name wins.
        config = parse_config(
            {
                "vigilance": {"max_queue_size": 111},
                "global": {"max_queue_size": 999},
                "defence": {"batch_size": 11},
                "cleanup": {"batch_size": 99},
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                },
            }
        )
        assert config["search_settings"]["max_queue_size"] == 111
        assert config["cleanup_settings"]["batch_size"] == 11

    def test_max_queue_size(self) -> None:
        config = parse_config(
            {
                "global": {"max_queue_size": 500},
                "killarr": {},
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                },
            }
        )
        assert config["search_settings"]["max_queue_size"] == 500

    def test_fetch_timeout(self) -> None:
        config = parse_config(
            {
                "global": {"fetch_timeout_seconds": 180},
                "killarr": {},
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                },
            }
        )
        assert config["search_settings"]["fetch_timeout_seconds"] == 180

    def test_interval_backward_compat(self) -> None:
        config = parse_config(
            {
                "global": {"interval": 3600},
                "killarr": {},
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc123", "enabled": True},
                },
            }
        )
        assert config["search_settings"]["run_interval_minutes"] == 60

    def test_search_type_instance_config(self) -> None:
        config = parse_config(
            {
                "instances": {
                    "MySonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "series",
                    },
                    "MyLidarr": {
                        "type": "lidarr",
                        "host": "http://lidarr:8686",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "artist",
                    },
                }
            }
        )
        sonarr = config["instances"]["sonarr"][0]
        lidarr = config["instances"]["lidarr"][0]
        assert sonarr["search_type"] == "series"
        assert lidarr["search_type"] == "artist"

    def test_instance_search_type_reaches_client_settings(self) -> None:
        config = parse_config(
            {
                "instances": {
                    "MySonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "series",
                    },
                    "MyLidarr": {
                        "type": "lidarr",
                        "host": "http://lidarr:8686",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "artist",
                    },
                }
            }
        )

        clients = build_arr_clients(
            config["instances"], config["search_settings"], config["cleanup_settings"], CircuitBreaker(0)
        )

        assert {client.name: client.search_type for client in clients} == {
            "MySonarr": "series",
            "MyLidarr": "artist",
        }

    def test_instance_cleanup_settings_reach_client_settings(self) -> None:
        config = parse_config(
            {
                "killarr": {"max_removals_per_instance": 20},
                "instances": {
                    "MyLidarr": {
                        "type": "lidarr",
                        "host": "http://lidarr:8686",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "artist",
                        "max_removals_per_instance": 5,
                    },
                    "MySonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "series",
                    },
                },
            }
        )

        clients = build_arr_clients(
            config["instances"], config["search_settings"], config["cleanup_settings"], CircuitBreaker(0)
        )

        assert {client.name: client.cleanup_settings["max_removals_per_instance"] for client in clients} == {
            "MyLidarr": 5,
            "MySonarr": 20,
        }

    def test_instance_cleanup_action_override_reaches_client_settings(self) -> None:
        config = parse_config(
            {
                "killarr": {"manual_import": "blocklist"},
                "instances": {
                    "MyLidarr": {
                        "type": "lidarr",
                        "host": "http://lidarr:8686",
                        "api_key": "abc123",
                        "enabled": True,
                        "manual_import": "remove",
                    },
                },
            }
        )

        clients = build_arr_clients(
            config["instances"], config["search_settings"], config["cleanup_settings"], CircuitBreaker(0)
        )

        assert clients[0].cleanup_settings["manual_import"] == "remove"

    def test_instance_override_validation_rejects_invalid_cleanup_type(self) -> None:
        try:
            parse_config(
                {
                    "instances": {
                        "MyLidarr": {
                            "type": "lidarr",
                            "host": "http://lidarr:8686",
                            "api_key": "abc123",
                            "enabled": True,
                            "max_removals_per_instance": "many",
                        },
                    },
                }
            )
        except ValueError as error:
            assert "'instances.MyLidarr.max_removals_per_instance' must be of type int" in str(error)
        else:
            raise AssertionError("Expected invalid per-instance cleanup override to be rejected")

    def test_search_type_is_normalized_for_instance_overrides(self) -> None:
        config = parse_config(
            {
                "instances": {
                    "MySonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "Series",
                    },
                },
            }
        )

        assert config["instances"]["sonarr"][0]["search_type"] == "series"

    def test_invalid_search_after_cleanup_action_is_rejected(self) -> None:
        try:
            parse_config(
                {
                    "global": {"search_after_cleanup_actions": ["retry", "delete"]},
                    "instances": {
                        "MyRadarr": {
                            "type": "radarr",
                            "host": "http://radarr:7878",
                            "api_key": "abc123",
                            "enabled": True,
                        },
                    },
                }
            )
        except ValueError as error:
            assert "'global.search_after_cleanup_actions' entries must be one of" in str(error)
        else:
            raise AssertionError("Expected invalid search_after_cleanup_actions value to be rejected")

    def test_unset_instance_search_type_uses_client_default(self) -> None:
        config = parse_config(
            {
                "instances": {
                    "MyRadarr": {
                        "type": "radarr",
                        "host": "http://radarr:7878",
                        "api_key": "abc123",
                        "enabled": True,
                    },
                    "MySonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": True,
                    },
                    "MyLidarr": {
                        "type": "lidarr",
                        "host": "http://lidarr:8686",
                        "api_key": "abc123",
                        "enabled": True,
                    },
                },
            }
        )

        clients = build_arr_clients(
            config["instances"], config["search_settings"], config["cleanup_settings"], CircuitBreaker(0)
        )

        assert {client.name: client.search_type for client in clients} == {
            "MyRadarr": "movie",
            "MySonarr": "episode",
            "MyLidarr": "album",
        }

    def test_disabled_instance_skipped(self) -> None:
        config = parse_config(
            {
                "instances": {
                    "EnabledRadarr": {
                        "type": "radarr",
                        "host": "http://radarr:7878",
                        "api_key": "abc123",
                        "enabled": True,
                    },
                    "DisabledSonarr": {
                        "type": "sonarr",
                        "host": "http://sonarr:8989",
                        "api_key": "abc123",
                        "enabled": False,
                    },
                },
            }
        )
        assert len(config["instances"]["radarr"]) == 1
        assert len(config["instances"]["sonarr"]) == 0

    def test_env_var_expansion(self) -> None:
        import os
        import tempfile

        os.environ["TEST_API_KEY"] = "secret123"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("""
instances:
  MyRadarr:
    type: radarr
    host: "http://radarr:7878"
    api_key: "${TEST_API_KEY}"
    enabled: true
""")
            temp_path = f.name
        try:
            from warden.config import load_config

            config = load_config(temp_path)
            assert config["instances"]["radarr"][0]["api_key"] == "secret123"
        finally:
            os.unlink(temp_path)

    def test_config_example_is_valid(self) -> None:
        import os
        from pathlib import Path

        from warden.config import load_config

        os.environ["SONARR_API_KEY"] = "sonarr-key"
        os.environ["RADARR_API_KEY"] = "radarr-key"
        os.environ["LIDARR_API_KEY"] = "lidarr-key"

        config_path = Path(__file__).resolve().parents[1] / "config.example.yaml"
        config = load_config(str(config_path))

        assert len(config["instances"]["sonarr"]) == 1
        assert len(config["instances"]["radarr"]) == 1
        assert len(config["instances"]["lidarr"]) == 1

    def test_cleanup_stall_actions(self) -> None:
        config = parse_config(
            {
                "killarr": {
                    "stalled": "blocklist",
                    "no_upgrade": "ignore",
                    "manual_import": "remove",
                },
                "instances": {
                    "MyRadarr": {"type": "radarr", "host": "http://radarr:7878", "api_key": "abc", "enabled": True},
                },
            }
        )
        assert config["cleanup_settings"]["stalled"] == "blocklist"
        assert config["cleanup_settings"]["no_upgrade"] == "ignore"
        assert config["cleanup_settings"]["manual_import"] == "remove"


class TestStallClassifier:
    def test_classify_stalled(self) -> None:
        assert classify_stall(["the download is stalled with no seeds"]) == "stalled"

    def test_classify_no_upgrade(self) -> None:
        assert classify_stall(["already meets cutoff"]) == "no_upgrade"

    def test_classify_manual_import(self) -> None:
        assert classify_stall(["import failed, path does not exist"]) == "manual_import"

    def test_classify_no_files(self) -> None:
        assert classify_stall(["no files found are eligible for import"]) == "no_files"

    def test_classify_missing_items(self) -> None:
        assert classify_stall(["not imported or missing from the release"]) == "missing_items"

    def test_classify_tba_title(self) -> None:
        assert classify_stall(["tba title"]) == "tba_title"

    def test_classify_no_messages(self) -> None:
        assert classify_stall([]) == "no_messages"

    def test_classify_unknown(self) -> None:
        assert classify_stall(["some completely random message"]) == "unknown"

    def test_classify_dangerous_file(self) -> None:
        assert classify_stall(["potentially dangerous file extension"]) == "dangerous_file"

    def test_classify_lidarr_permissions_error(self) -> None:
        assert classify_stall(["Failed to import track, Permissions error"]) == "manual_import"

    def test_classify_lidarr_track_match_failure(self) -> None:
        assert classify_stall(["Worst track match: 0.0 % vs 60 % [track length, track title]"]) == "manual_import"

    def test_classify_lidarr_similar_album_failure(self) -> None:
        assert classify_stall(["Couldn't find similar album for [/data/decypharr/download]"]) == "manual_import"

    def test_classify_lidarr_track_completeness_as_missing_items(self) -> None:
        # Track-completeness mismatches: a re-search can find a complete release,
        # so these classify as missing_items (blocklist + search), not manual_import.
        assert classify_stall(["Has unmatched tracks"]) == "missing_items"
        assert classify_stall(["Has fewer tracks than existing release"]) == "missing_items"
        assert classify_stall(["Has missing tracks"]) == "missing_items"

    def test_classify_duplicate_artist_stays_unknown(self) -> None:
        # The duplicate-artist case must NOT be blocklisted/re-grabbed (a re-grab
        # can never fix a Lidarr metadata dupe); it stays unknown -> ignore on lidarr.
        assert classify_stall(["Unable to import automatically, found multiple artists"]) == "unknown"

    def test_unknown_ignored_items_do_not_warn(self, caplog) -> None:
        client = LidarrClient(
            "lidarr",
            "http://lidarr:8686",
            "abc123",
            {"stagger_interval_seconds": 0},
            {"unknown": "ignore"},
        )
        client.session.get = lambda url, *, params, timeout: type(
            "R",
            (),
            {
                "raise_for_status": lambda self: None,
                "json": lambda self: {
                    "records": [
                        {
                            "id": 1,
                            "status": "completed",
                            "trackedDownloadStatus": "warning",
                            "artist": {"artistName": "Unknown Artist", "tags": []},
                            "title": "Floating Soulseek Grab",
                            "statusMessages": [
                                {"messages": ["Download wasn't grabbed by Lidarr and not in a category, Skipping."]}
                            ],
                        },
                    ]
                },
            },
        )()

        items, stats = client.get_stalled_items()

        assert items == []
        assert stats["ignored"] == 1
        assert "Unrecognized status messages" not in caplog.text


class TestCleanupRemoval:
    def test_blocklist_removal_triggers_sonarr_episode_search(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"dry_run": False, "stagger_interval_seconds": 0},
            {},
        )

        class DeleteResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

        class PostResponse:
            def raise_for_status(self) -> None:
                return None

        delete_calls = []
        post_calls = []

        def delete(url: str, *, params: dict, timeout: int) -> DeleteResponse:
            delete_calls.append((url, params, timeout))
            return DeleteResponse()

        def post(url: str, *, json: dict, timeout: int) -> PostResponse:
            post_calls.append((url, json, timeout))
            return PostResponse()

        client.session.delete = delete
        client.session.post = post

        client.execute_removal(
            QueueItem(10, 20, "Example Show - S01E01", "blocklist", "manual_import", []),
            1,
            1,
        )

        assert delete_calls == [
            ("http://sonarr:8989/api/v3/queue/10", {"removeFromClient": "true", "blocklist": "true"}, 15)
        ]
        assert post_calls == [("http://sonarr:8989/api/v3/command", {"name": "EpisodeSearch", "episodeIds": [20]}, 120)]

    def test_sonarr_series_search_groups_missing_records_by_series(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"search_type": "series", "stagger_interval_seconds": 0},
            {},
        )
        client._fetch_unlimited = lambda endpoint: [
            {
                "id": 100,
                "episodeId": 100,
                "airDateUtc": "2020-01-01T00:00:00Z",
                "series": {"id": 55, "title": "Steel Jeeg", "tags": []},
            },
            {
                "id": 101,
                "episodeId": 101,
                "airDateUtc": "2020-01-02T00:00:00Z",
                "series": {"id": 55, "title": "Steel Jeeg", "tags": []},
            },
        ]
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        items = client.get_media_to_search(25, 0)
        client.trigger_search(items)

        assert items == [("series:55", "missing", "Steel Jeeg")]
        assert posts == [("http://sonarr:8989/api/v3/command", {"name": "SeriesSearch", "seriesId": 55}, 120)]

    def test_sonarr_cleanup_uses_series_id_when_series_search_enabled(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"search_type": "series", "stagger_interval_seconds": 0},
            {},
        )

        assert client._get_media_id({"episodeId": 20, "seriesId": 55}) == "series:55"

    def test_cleanup_search_scope_season_returns_season_id(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {"cleanup_search_scope": "season"},
        )

        assert client._get_media_id({"episodeId": 20, "seriesId": 55, "seasonNumber": 2}) == "season:55:2"

    def test_cleanup_search_scope_season_triggers_season_search(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {"cleanup_search_scope": "season"},
        )
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        client.trigger_search([("season:55:2", "blocklist", "Show - Season 02")])

        assert posts == [
            ("http://sonarr:8989/api/v3/command", {"name": "SeasonSearch", "seriesId": 55, "seasonNumber": 2}, 120)
        ]

    def test_cleanup_search_scope_series_returns_series_id(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {"cleanup_search_scope": "series"},
        )

        assert client._get_media_id({"episodeId": 20, "seriesId": 55, "seasonNumber": 2}) == "series:55"

    def test_cleanup_search_scope_episode_is_default(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {},
        )

        assert client._get_media_id({"episodeId": 20, "seriesId": 55, "seasonNumber": 2}) == 20

    def test_lidarr_cleanup_falls_back_to_queue_id_without_album_id(self) -> None:
        client = LidarrClient(
            "lidarr",
            "http://lidarr:8686",
            "abc123",
            {"stagger_interval_seconds": 0},
            {},
        )

        assert client._get_media_id({"id": 123}) == 123

    def test_cleanup_search_scope_series_type_takes_precedence_over_scope(self) -> None:
        # search_type="series" always wins regardless of cleanup_search_scope
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"search_type": "series", "stagger_interval_seconds": 0},
            {"cleanup_search_scope": "season"},
        )

        assert client._get_media_id({"episodeId": 20, "seriesId": 55, "seasonNumber": 2}) == "series:55"

    def test_mutating_calls_are_throttled_when_interval_configured(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"api_request_interval_seconds": 2, "search_jitter_seconds": 0, "stagger_interval_seconds": 0},
            {"delete_timeout_seconds": 15},
        )
        sleeps = []
        now = [10.0]
        client._time_func = lambda: now[0]

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        client._sleep_func = sleep
        client._last_mutating_request = 9.0

        client._throttle_mutating_request()

        assert sleeps == [1.0]

    def test_search_jitter_delays_before_search_command(self) -> None:
        from warden.clients import arr as arr_module

        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"search_jitter_seconds": 3, "stagger_interval_seconds": 0},
            {},
        )
        sleeps = []
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        original_uniform = arr_module.random.uniform
        arr_module.random.uniform = lambda start, end: 1.5
        client._sleep_func = lambda seconds: sleeps.append(seconds)
        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        try:
            client.trigger_search([(20, "missing", "Example Show - S01E01")])
        finally:
            arr_module.random.uniform = original_uniform

        assert sleeps == [1.5]
        assert posts == [("http://sonarr:8989/api/v3/command", {"name": "EpisodeSearch", "episodeIds": [20]}, 120)]

    def test_cleanup_queue_fetch_respects_page_size_and_record_cap(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {},
            {"cleanup_page_size": 2, "max_cleanup_queue_records": 3, "fetch_timeout_seconds": 30},
        )
        calls = []

        class Response:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"records": self._records}

        pages = [
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
        ]

        def get(url: str, *, params: dict, timeout: int) -> Response:
            calls.append((url, params, timeout))
            return Response(pages[len(calls) - 1])

        client.session.get = get

        records, fetch_failed = client._fetch_all_queue()

        assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert fetch_failed is False
        assert [call[1] for call in calls] == [
            {"page": 1, "pageSize": 2, "includeUnknownSeriesItems": "true"},
            {"page": 2, "pageSize": 2, "includeUnknownSeriesItems": "true"},
        ]

    def test_delete_queue_item_falls_back_to_keep_client_on_404(self) -> None:
        # downloadClientUnavailable orphans have no tracked download: removeFromClient=true
        # 404s every cycle, and the *arr only drops them when asked NOT to remove from the
        # client. Without the fallback they 404 forever and the queue never clears.
        import requests

        client = SonarrClient("sonarr", "http://sonarr:8989", "k", {}, {})
        calls: list[dict] = []

        class Resp:
            def __init__(self, code: int) -> None:
                self.status_code = code
                self.ok = 200 <= code < 300

            def raise_for_status(self) -> None:
                if not self.ok:
                    raise requests.HTTPError(str(self.status_code))

        def delete(url: str, *, params: dict, timeout: int) -> Resp:
            calls.append(params)
            return Resp(404) if params.get("removeFromClient") == "true" else Resp(200)

        client.session.delete = delete
        item = QueueItem(123, 5, "Orphan", "remove", "download_unavailable", [], "")
        assert client._delete_queue_item(item, 1, 1) is True
        assert any(p.get("removeFromClient") == "false" for p in calls)

    def test_delete_queue_item_keep_client_fallback_not_used_for_normal_removal(self) -> None:
        # A normal in-client item is removed with removeFromClient=true on the first try;
        # the keep-client fallback must NOT fire, so a real client download is never orphaned.
        client = SonarrClient("sonarr", "http://sonarr:8989", "k", {}, {})
        calls: list[dict] = []

        class Resp:
            status_code = 200
            ok = True

            def raise_for_status(self) -> None:
                return None

        def delete(url: str, *, params: dict, timeout: int) -> Resp:
            calls.append(params)
            return Resp()

        client.session.delete = delete
        item = QueueItem(1, 2, "Normal", "remove", "stalled", [], "")
        assert client._delete_queue_item(item, 1, 1) is True
        assert calls == [{"removeFromClient": "true"}]

    def test_cleanup_cycle_continues_after_item_failure(self) -> None:
        calls = []

        class Client:
            name = "client"
            weight = 1

            def get_stalled_items(self):
                return [
                    QueueItem(1, 10, "bad", "blocklist", "manual_import", []),
                    QueueItem(2, 20, "good", "blocklist", "manual_import", []),
                ], {"total_evaluated": 2, "ignored": 0, "tag_filtered": 0, "retry_interval": 0}

            def execute_removal(self, item: QueueItem, index: int, total: int) -> None:
                calls.append(item.title)
                if item.title == "bad":
                    raise RuntimeError("boom")

        run_removal_cycle([Client()], {"batch_size": -1, "stagger_interval_seconds": 0})

        assert calls == ["bad", "good"]

    def test_cleanup_respects_max_removals_per_instance(self) -> None:
        calls = []

        class Client:
            weight = 1

            def __init__(self, name: str) -> None:
                self.name = name

            def get_stalled_items(self):
                return [
                    QueueItem(1, 10, f"{self.name}-1", "blocklist", "manual_import", []),
                    QueueItem(2, 20, f"{self.name}-2", "blocklist", "manual_import", []),
                    QueueItem(3, 30, f"{self.name}-3", "blocklist", "manual_import", []),
                ], {"total_evaluated": 3, "ignored": 0, "tag_filtered": 0, "retry_interval": 0}

            def execute_removal(self, item: QueueItem, index: int, total: int) -> None:
                calls.append((self.name, item.title, total))

        run_removal_cycle(
            [Client("sonarr-a"), Client("sonarr-b")],
            {"batch_size": -1, "max_removals_per_instance": 2, "stagger_interval_seconds": 0},
        )

        assert calls == [
            ("sonarr-a", "sonarr-a-1", 4),
            ("sonarr-a", "sonarr-a-2", 4),
            ("sonarr-b", "sonarr-b-1", 4),
            ("sonarr-b", "sonarr-b-2", 4),
        ]

    def test_cleanup_uses_client_specific_removal_cap(self) -> None:
        calls = []

        class Client:
            weight = 1

            def __init__(self, name: str, max_removals: int) -> None:
                self.name = name
                self.cleanup_settings = {"max_removals_per_instance": max_removals}

            def get_stalled_items(self):
                return [
                    QueueItem(1, 10, f"{self.name}-1", "blocklist", "manual_import", []),
                    QueueItem(2, 20, f"{self.name}-2", "blocklist", "manual_import", []),
                    QueueItem(3, 30, f"{self.name}-3", "blocklist", "manual_import", []),
                ], {"total_evaluated": 3, "ignored": 0, "tag_filtered": 0, "retry_interval": 0}

            def execute_removal(self, item: QueueItem, index: int, total: int) -> None:
                calls.append((self.name, item.title, total))

        run_removal_cycle(
            [Client("lidarr", 1), Client("sonarr", 2)],
            {"batch_size": -1, "max_removals_per_instance": 3, "stagger_interval_seconds": 0},
        )

        assert calls == [
            ("lidarr", "lidarr-1", 3),
            ("sonarr", "sonarr-1", 3),
            ("sonarr", "sonarr-2", 3),
        ]

    def test_protect_downloading_series_skips_stalled_items_for_active_series(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {"protect_downloading_series": True},
        )
        # Series 10 has one active download and one stalled — should be fully protected.
        # Series 20 has only a stalled download — should proceed to cleanup.
        client.session.get = lambda url, *, params, timeout: type(
            "R",
            (),
            {
                "raise_for_status": lambda self: None,
                "json": lambda self: {
                    "records": [
                        {
                            "id": 1,
                            "seriesId": 10,
                            "trackedDownloadStatus": "ok",
                            "series": {"title": "Protected Show"},
                            "statusMessages": [],
                        },
                        {
                            "id": 2,
                            "seriesId": 10,
                            "trackedDownloadStatus": "warning",
                            "series": {"title": "Protected Show"},
                            "statusMessages": [{"messages": ["the download is stalled with no seeds"]}],
                        },
                        {
                            "id": 3,
                            "seriesId": 20,
                            "trackedDownloadStatus": "warning",
                            "series": {"title": "Unprotected Show"},
                            "seasonNumber": 1,
                            "episodeId": 99,
                            "statusMessages": [{"messages": ["the download is stalled with no seeds"]}],
                        },
                    ]
                },
            },
        )()
        client.cleanup_settings["stalled"] = "blocklist"

        items, stats = client.get_stalled_items()

        assert len(items) == 1
        assert items[0].title.startswith("Unprotected Show")
        assert stats["series_protected"] == 1

    def test_protect_downloading_series_disabled_by_default(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {},  # protect_downloading_series not set
        )
        assert client.protect_downloading_series is False
        assert client._get_skip_series_ids([]) == set()

    def test_cleanup_replacement_search_can_be_disabled(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"search_after_cleanup": False, "stagger_interval_seconds": 0},
            {"delete_timeout_seconds": 15},
        )

        class DeleteResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

        post_calls = []
        client.session.delete = lambda url, *, params, timeout: DeleteResponse()
        client.session.post = lambda url, *, json, timeout: post_calls.append((url, json, timeout))

        client.execute_removal(QueueItem(10, 20, "Example", "blocklist", "manual_import", []), 1, 1)

        assert post_calls == []


class TestCleanerLoop:
    def test_cleaner_start_log_uses_defence_name(self, caplog) -> None:
        import logging

        caplog.set_level(logging.INFO)

        class Shutdown:
            def __init__(self) -> None:
                self.calls = 0

            def is_set(self) -> bool:
                return self.calls > 0

            def wait(self, timeout: int) -> bool:
                self.calls += 1
                return True

        run_cleaner_loop(
            [],
            {"interval": 1, "batch_size": 0, "stagger_interval_seconds": 0},
            Shutdown(),
        )

        assert "Warden-Defence started" in caplog.text
        assert "Killarr" not in caplog.text


class TestStallDetection:
    def test_stalled_detects_tracked_download_warning(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {})
        assert client._is_stalled({"trackedDownloadStatus": "warning", "status": "completed"})

    def test_stalled_detects_download_client_unavailable(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {})
        assert client._is_stalled({"status": "downloadClientUnavailable", "trackedDownloadStatus": ""})

    def test_stalled_detects_failed_status(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {})
        assert client._is_stalled({"status": "failed", "trackedDownloadStatus": "ok"})

    def test_stalled_ignores_ok_items(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {})
        assert not client._is_stalled({"status": "completed", "trackedDownloadStatus": "ok"})

    def test_stalled_age_based_when_queue_max_age_exceeded(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {"queue_max_age_hours": 1})
        # Old + not completed + not healthily in-progress -> age-reaped.
        assert client._is_stalled(
            {
                "status": "",
                "trackedDownloadState": "importPending",
                "added": "2024-01-01T00:00:00Z",
            }
        )

    def test_stalled_age_based_protects_healthy_in_progress(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {"queue_max_age_hours": 1})
        # Old, but the *arr still reports it healthily downloading -> NOT stalled.
        # Slow is not the same as stalled; protects good downloads from blocklisting.
        assert not client._is_stalled(
            {
                "status": "downloading",
                "trackedDownloadStatus": "ok",
                "trackedDownloadState": "downloading",
                "added": "2024-01-01T00:00:00Z",
            }
        )

    def test_stalled_age_based_respects_completed_status(self) -> None:
        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {"queue_max_age_hours": 1})
        assert not client._is_stalled(
            {
                "status": "completed",
                "trackedDownloadStatus": "ok",
                "added": "2024-01-01T00:00:00Z",
            }
        )

    def test_stalled_age_based_ignores_recent_items(self) -> None:
        import datetime

        client = SonarrClient("test", "http://sonarr:8989", "abc123", {}, {"queue_max_age_hours": 24})
        # "recent" must be relative to now, not a hardcoded date — a fixed date
        # silently ages past the threshold and the test starts failing on wall-clock drift.
        recent = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=1)).isoformat()
        assert not client._is_stalled(
            {
                "status": "downloading",
                "trackedDownloadStatus": "ok",
                "added": recent,
            }
        )

    def test_download_unavailable_category_in_stall_categories(self) -> None:
        from warden.validators import STALL_CATEGORIES

        assert "download_unavailable" in STALL_CATEGORIES

    def test_get_stalled_items_processes_download_client_unavailable(self) -> None:
        client = SonarrClient(
            "test",
            "http://sonarr:8989",
            "abc123",
            {"stagger_interval_seconds": 0},
            {"download_unavailable": "blocklist"},
        )
        client.session.get = lambda url, *, params, timeout: type(
            "R",
            (),
            {
                "raise_for_status": lambda self: None,
                "json": lambda self: {
                    "records": [
                        {
                            "id": 1,
                            "status": "downloadClientUnavailable",
                            "trackedDownloadStatus": "",
                            "seriesId": 10,
                            "series": {"title": "Orphaned Show", "tags": []},
                            "seasonNumber": 1,
                            "episodeId": 99,
                            "episodeNumber": 1,
                            "added": "2026-05-22T00:00:00Z",
                            "statusMessages": [],
                        },
                    ]
                },
            },
        )()
        items, stats = client.get_stalled_items()
        assert len(items) == 1
        assert items[0].category == "download_unavailable"
        assert items[0].action == "blocklist"


class TestSearchCycle:
    def test_search_allocation_respects_client_weight(self) -> None:
        class Client:
            def __init__(self, name: str, weight: int) -> None:
                self.name = name
                self.weight = weight

        heavy = Client("heavy", 3)
        light = Client("light", 1)

        allocated = allocate_search_slots(
            4,
            {heavy: [(1, "missing", "h1"), (2, "missing", "h2")], light: [(3, "missing", "l1"), (4, "missing", "l2")]},
        )

        # heavy has weight=3 so gets up to 3 turns per round (exhausts its 2 items first),
        # then light fills the remaining slots
        assert [(client.name, item[2]) for client, item in allocated] == [
            ("heavy", "h1"),
            ("heavy", "h2"),
            ("light", "l1"),
            ("light", "l2"),
        ]

    def test_search_cycle_continues_after_client_fetch_failure(self) -> None:
        searches = []

        class BadClient:
            name = "bad"
            weight = 1

            def is_queue_too_large(self) -> bool:
                return False

            def get_media_to_search(self, missing_batch_size: int, upgrade_batch_size: int):
                raise RuntimeError("fetch failed")

        class GoodClient:
            name = "good"
            weight = 1

            def is_queue_too_large(self) -> bool:
                return False

            def get_media_to_search(self, missing_batch_size: int, upgrade_batch_size: int):
                return [(123, "missing", "Good Movie")]

            def trigger_search(self, items, *, index=None, total=None) -> None:
                searches.extend(items)

        run_search_cycle(
            [BadClient(), GoodClient()],
            {"missing_batch_size": 1, "upgrade_batch_size": 0, "stagger_interval_seconds": 0},
        )

        assert searches == [(123, "missing", "Good Movie")]

    def test_search_cycle_skips_client_when_queue_size_check_times_out(self) -> None:
        import requests

        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"max_queue_size": 500, "stagger_interval_seconds": 0},
            {},
        )
        searches = []

        def get(url: str, *, params: dict, timeout: int) -> None:
            raise requests.ReadTimeout("read timed out")

        client.session.get = get
        client.get_media_to_search = lambda missing_batch_size, upgrade_batch_size: (
            searches.append((missing_batch_size, upgrade_batch_size)) or [(123, "missing", "Should Not Search")]
        )
        client.trigger_search = lambda items, *, index=None, total=None: searches.extend(items)

        run_search_cycle(
            [client],
            {"missing_batch_size": 1, "upgrade_batch_size": 0, "stagger_interval_seconds": 0},
        )

        assert searches == []

    def test_queue_size_check_uses_short_timeout(self) -> None:
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"fetch_timeout_seconds": 120, "queue_check_timeout_seconds": 12, "max_queue_size": 500},
            {},
        )
        timeouts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"totalRecords": 1}

        def get(url: str, *, params: dict, timeout: int) -> Response:
            timeouts.append(timeout)
            return Response()

        client.session.get = get

        assert not client.is_queue_too_large()
        assert timeouts == [12]

    def test_queue_size_check_counts_unknown_series_items(self) -> None:
        # Regression: the queue-size safeguard must count the ENTIRE queue, including
        # items the *arr cannot map to a series (includeUnknownSeriesItems=true).
        # Counting only known-series items let unmapped/orphaned downloads pile up
        # without ever tripping max_queue_size, so Vigilance flooded the queue.
        client = SonarrClient(
            "sonarr-tv",
            "http://sonarr:8989",
            "abc123",
            {"max_queue_size": 500},
            {},
        )
        captured: dict = {}

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"totalRecords": 4000}

        def get(url: str, *, params: dict, timeout: int) -> "Response":
            captured.update(params)
            return Response()

        client.session.get = get

        assert client.is_queue_too_large()
        assert captured.get("includeUnknownSeriesItems") == "true"

    def test_queue_size_check_uses_client_specific_unknown_param(self) -> None:
        # The include-unknown query param is *arr-specific: Sonarr/Radarr/Lidarr each
        # name it differently. Using the wrong name silently undercounts the queue.
        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"totalRecords": 0}

        cases = [
            (SonarrClient, "includeUnknownSeriesItems"),
            (RadarrClient, "includeUnknownMovieItems"),
            (LidarrClient, "includeUnknownArtistItems"),
        ]
        for client_cls, expected_param in cases:
            client = client_cls("inst", "http://arr", "abc123", {"max_queue_size": 1}, {})
            captured: dict = {}

            def get(url: str, *, params: dict, timeout: int, _c: dict = captured) -> Response:
                _c.update(params)
                return Response()

            client.session.get = get
            client.is_queue_too_large()
            assert captured.get(expected_param) == "true", client_cls.__name__

    def test_get_media_id_falls_back_to_queue_id_for_unmapped_items(self) -> None:
        # Defence now fetches unmapped/unknown queue items; _get_media_id must not
        # KeyError on records lacking episodeId/movieId — fall back to the queue id.
        sonarr = SonarrClient("s", "http://s", "k", {}, {})
        assert sonarr._get_media_id({"id": 77}) == 77
        assert sonarr._get_media_id({"id": 77, "seriesId": 9, "episodeId": 5}) == 5
        radarr = RadarrClient("r", "http://r", "k", {}, {})
        assert radarr._get_media_id({"id": 88}) == 88
        assert radarr._get_media_id({"id": 88, "movieId": 6}) == 6


class TestRadarrCollection:
    def test_radarr_collection_search_falls_back_to_movie_search_when_off(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"search_type": "collection", "radarr_collection_search_mode": "off", "stagger_interval_seconds": 0},
            {},
        )
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        client.trigger_search([(44, "missing", "Movie")])

        assert posts == [("http://radarr:7878/api/v3/command", {"name": "MoviesSearch", "movieIds": [44]}, 120)]

    def test_radarr_collection_search_force_posts_collection_command(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"search_type": "collection", "radarr_collection_search_mode": "force", "stagger_interval_seconds": 0},
            {},
        )
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        client.trigger_search([("collection:77", "missing", "The Godfather Collection")])

        assert posts == [
            ("http://radarr:7878/api/v3/command", {"name": "CollectionSearch", "collectionIds": [77]}, 120)
        ]

    def test_radarr_force_collection_fetches_from_collection_endpoint(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"search_type": "collection", "radarr_collection_search_mode": "force", "stagger_interval_seconds": 0},
            {},
        )
        client._fetch_list = lambda endpoint, params=None: (
            [
                {
                    "id": 10,
                    "title": "The Godfather Collection",
                    "monitored": True,
                    "movies": [
                        {"monitored": True, "hasFile": False, "isAvailable": True},
                        {"monitored": True, "hasFile": True, "isAvailable": True},
                    ],
                },
                {
                    "id": 11,
                    "title": "Fully Collected",
                    "monitored": True,
                    "movies": [
                        {"monitored": True, "hasFile": True, "isAvailable": True},
                    ],
                },
                {
                    "id": 12,
                    "title": "Unmonitored Collection",
                    "monitored": False,
                    "movies": [
                        {"monitored": True, "hasFile": False, "isAvailable": True},
                    ],
                },
            ]
            if endpoint == "/api/v3/collection"
            else []
        )

        items = client.get_media_to_search(10, 0)

        assert items == [("collection:10", "missing", "The Godfather Collection")]

    def test_radarr_detect_collection_groups_movies_by_collection(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"search_type": "collection", "radarr_collection_search_mode": "detect", "stagger_interval_seconds": 0},
            {},
        )
        client._fetch_list = lambda endpoint, params=None: (
            [
                {"id": 1, "collection": {"id": 99, "title": "Die Hard Collection"}, "monitored": True},
                {"id": 2, "collection": {"id": 99, "title": "Die Hard Collection"}, "monitored": True},
                {"id": 3, "collection": None, "monitored": True},
            ]
            if endpoint == "/api/v3/movie"
            else []
        )
        client._fetch_unlimited = lambda endpoint: (
            [
                {"id": 101, "movieId": 1, "title": "Die Hard", "isAvailable": True},
                {"id": 102, "movieId": 2, "title": "Die Hard 2", "isAvailable": True},
                {"id": 103, "movieId": 3, "title": "Standalone Movie", "isAvailable": True},
            ]
            if "missing" in endpoint
            else []
        )

        items = client.get_media_to_search(10, 0)

        assert items == [
            ("collection:99", "missing", "Die Hard Collection"),
            (103, "missing", "Standalone Movie"),
        ]

    def test_radarr_detect_collection_falls_back_to_movies_search_for_uncollected(self) -> None:
        client = RadarrClient(
            "radarr",
            "http://radarr:7878",
            "abc123",
            {"search_type": "collection", "radarr_collection_search_mode": "detect", "stagger_interval_seconds": 0},
            {},
        )
        client._fetch_list = lambda endpoint, params=None: (
            [{"id": 5, "collection": None, "monitored": True}] if endpoint == "/api/v3/movie" else []
        )
        client._fetch_unlimited = lambda endpoint: (
            [{"id": 201, "movieId": 5, "title": "Solo Film", "isAvailable": True}] if "missing" in endpoint else []
        )
        posts = []

        class Response:
            def raise_for_status(self) -> None:
                return None

        client.session.post = lambda url, *, json, timeout: posts.append((url, json, timeout)) or Response()

        items = client.get_media_to_search(10, 0)
        client.trigger_search(items)

        assert items == [(201, "missing", "Solo Film")]
        assert posts == [("http://radarr:7878/api/v3/command", {"name": "MoviesSearch", "movieIds": [201]}, 120)]


class TestSettingsSchema:
    def test_search_schema_has_custom_fields(self) -> None:
        assert "circuit_breaker_threshold" in SEARCH_SETTINGS_SCHEMA
        assert "max_queue_size" in SEARCH_SETTINGS_SCHEMA
        assert "fetch_timeout_seconds" in SEARCH_SETTINGS_SCHEMA
        assert SEARCH_SETTINGS_SCHEMA["circuit_breaker_threshold"]["default"] == 0
        assert SEARCH_SETTINGS_SCHEMA["max_queue_size"]["default"] == 0
        assert SEARCH_SETTINGS_SCHEMA["fetch_timeout_seconds"]["default"] == 120

    def test_cleanup_schema_has_custom_fields(self) -> None:
        assert "circuit_breaker_threshold" in CLEANUP_SETTINGS_SCHEMA
        assert "fetch_timeout_seconds" in CLEANUP_SETTINGS_SCHEMA

    def test_hardening_search_schema_defaults(self) -> None:
        assert SEARCH_SETTINGS_SCHEMA["api_request_interval_seconds"]["default"] == 0
        assert SEARCH_SETTINGS_SCHEMA["search_after_cleanup"]["default"] is True
        assert SEARCH_SETTINGS_SCHEMA["search_after_cleanup_actions"]["default"] == ["retry", "blocklist"]
        assert SEARCH_SETTINGS_SCHEMA["search_jitter_seconds"]["default"] == 0
        assert SEARCH_SETTINGS_SCHEMA["radarr_collection_search_mode"]["default"] == "off"
        assert SEARCH_SETTINGS_SCHEMA["queue_check_timeout_seconds"]["default"] == 15

    def test_hardening_cleanup_schema_defaults(self) -> None:
        assert CLEANUP_SETTINGS_SCHEMA["cleanup_page_size"]["default"] == 100
        assert CLEANUP_SETTINGS_SCHEMA["max_cleanup_queue_records"]["default"] == 0
        assert CLEANUP_SETTINGS_SCHEMA["delete_timeout_seconds"]["default"] == 15
        assert CLEANUP_SETTINGS_SCHEMA["max_removals_per_instance"]["default"] == 0
        assert CLEANUP_SETTINGS_SCHEMA["search_after_cleanup"]["default"] is None

    def test_radarr_collection_search_type_is_valid(self) -> None:
        config = parse_config(
            {
                "global": {"radarr_collection_search_mode": "detect"},
                "instances": {
                    "MyRadarr": {
                        "type": "radarr",
                        "host": "http://radarr:7878",
                        "api_key": "abc123",
                        "enabled": True,
                        "search_type": "collection",
                    },
                },
            }
        )
        assert config["instances"]["radarr"][0]["search_type"] == "collection"

    def test_search_schema_defaults(self) -> None:
        assert SEARCH_SETTINGS_SCHEMA["run_interval_minutes"]["default"] == 60
        assert SEARCH_SETTINGS_SCHEMA["missing_batch_size"]["default"] == 20
        assert SEARCH_SETTINGS_SCHEMA["upgrade_batch_size"]["default"] == 10
        assert SEARCH_SETTINGS_SCHEMA["search_order"]["default"] == "last_searched_ascending"

    def test_cleanup_schema_defaults(self) -> None:
        assert CLEANUP_SETTINGS_SCHEMA["interval"]["default"] == 3600
        assert CLEANUP_SETTINGS_SCHEMA["batch_size"]["default"] == 10
        assert CLEANUP_SETTINGS_SCHEMA["removal_order"]["default"] == "api_order"
