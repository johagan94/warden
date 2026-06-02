"""Tests for Defence-loop safety: protect healthy downloads from age-reaping, and
degrade gracefully (fallback + cooldown) when a queue removal fails.

Style matches test_core.py: real clients with monkeypatched session, plain asserts.
"""

import requests

from warden.clients.arr import QueueItem, SonarrClient

OLD = "2000-01-01T00:00:00Z"  # always older than any queue_max_age_hours window


def _sonarr(**cleanup) -> SonarrClient:
    base = {"dry_run": False, "stagger_interval_seconds": 0, "queue_max_age_hours": 1}
    base.update(cleanup)
    return SonarrClient("sonarr", "http://sonarr:8989", "abc123", {"stagger_interval_seconds": 0}, base)


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Server Error")


# --------------------------------------------------------------------------- #
# Protect healthy in-progress downloads from age-based reaping
# --------------------------------------------------------------------------- #


class TestProtectHealthyDownloads:
    def test_age_reap_protects_actively_downloading(self) -> None:
        client = _sonarr()
        record = {
            "added": OLD,
            "status": "downloading",
            "trackedDownloadStatus": "ok",
            "trackedDownloadState": "downloading",
        }
        # Old enough to trip queue_max_age_hours, but healthy -> must NOT be stalled.
        assert client._is_stalled(record) is False

    def test_age_reap_protects_queued(self) -> None:
        client = _sonarr()
        assert client._is_stalled({"added": OLD, "status": "queued", "trackedDownloadStatus": "ok"}) is False

    def test_age_reap_catches_old_non_progressing(self) -> None:
        client = _sonarr()
        # Old, not completed, not warning, and not in a healthy in-flight state.
        record = {"added": OLD, "status": "", "trackedDownloadState": "importPending"}
        assert client._is_stalled(record) is True

    def test_warning_overrides_health(self) -> None:
        client = _sonarr()
        # Even if it looks like it's downloading, a warning flag means stalled.
        record = {
            "added": OLD,
            "status": "downloading",
            "trackedDownloadStatus": "warning",
            "trackedDownloadState": "downloading",
        }
        assert client._is_stalled(record) is True
        assert client._is_healthy_in_progress(record) is False

    def test_failed_and_client_unavailable_still_stalled(self) -> None:
        client = _sonarr()
        assert client._is_stalled({"status": "failed"}) is True
        assert client._is_stalled({"status": "downloadClientUnavailable"}) is True

    def test_recent_healthy_not_stalled_without_age_setting(self) -> None:
        client = _sonarr(queue_max_age_hours=0)  # age reaping disabled
        assert client._is_stalled({"added": OLD, "status": "downloading"}) is False

    def test_healthy_in_progress_definition(self) -> None:
        client = _sonarr()
        assert client._is_healthy_in_progress({"status": "downloading"}) is True
        assert client._is_healthy_in_progress({"trackedDownloadState": "downloading"}) is True
        assert client._is_healthy_in_progress({"status": "queued"}) is True
        assert client._is_healthy_in_progress({"status": "completed"}) is False
        assert client._is_healthy_in_progress({"status": "downloading", "trackedDownloadStatus": "warning"}) is False


class TestCleanupFetchCircuitBreaker:
    def test_queue_fetch_timeout_counts_toward_cleanup_circuit_breaker(self) -> None:
        client = _sonarr(circuit_breaker_threshold=2)
        client.circuit_breaker.threshold = 2
        calls = 0

        def get(url, *, params, timeout):
            nonlocal calls
            calls += 1
            raise requests.ReadTimeout("queue timed out")

        client.session.get = get

        first_items, first_stats = client.get_stalled_items()
        second_items, second_stats = client.get_stalled_items()
        third_items, third_stats = client.get_stalled_items()

        assert first_items == []
        assert second_items == []
        assert third_items == []
        assert first_stats["total_evaluated"] == 0
        assert second_stats["total_evaluated"] == 0
        assert third_stats["total_evaluated"] == 0
        assert calls == 2


# --------------------------------------------------------------------------- #
# Removal: fallback (blocklist -> plain remove) + cooldown on failure
# --------------------------------------------------------------------------- #


def _capture_delete(client, behavior):
    """Install a fake session.delete that records calls and returns per `behavior(params)`."""
    calls = []

    def delete(url, *, params, timeout):
        calls.append({"url": url, "params": dict(params), "timeout": timeout})
        return _Resp(behavior(params))

    client.session.delete = delete
    return calls


class TestRemovalFallback:
    def _item(self, action="blocklist"):
        return QueueItem(101, 55, "Example Show - S01E01", action, "missing_items", [], OLD)

    def test_blocklist_failure_falls_back_to_plain_remove(self) -> None:
        client = _sonarr(retry_interval_minutes=60)
        searches = []
        client._trigger_single = lambda *a, **k: searches.append(a)
        # 500 whenever blocklist is requested, 200 for the plain remove.
        calls = _capture_delete(client, lambda p: 500 if p.get("blocklist") == "true" else 200)

        client.execute_removal(self._item(), 1, 1)

        assert len(calls) == 2
        assert calls[0]["params"].get("blocklist") == "true"  # primary attempt
        assert "blocklist" not in calls[1]["params"]  # fallback: plain remove
        assert calls[1]["params"]["removeFromClient"] == "true"
        assert searches, "replacement search should fire after a successful removal"
        assert client._retry_state.get(55) is not None  # cooldown recorded on success

    def test_total_failure_records_cooldown_and_no_search(self) -> None:
        client = _sonarr(retry_interval_minutes=60)
        searches = []
        client._trigger_single = lambda *a, **k: searches.append(a)
        calls = _capture_delete(client, lambda p: 500)  # everything 500s

        client.execute_removal(self._item(), 1, 1)  # must not raise

        assert len(calls) == 2  # primary + fallback both tried
        assert not searches, "no replacement search when the item was never removed"
        assert client._retry_state.get(55) is not None  # cooled down so next cycle backs off

    def test_non_blocklist_action_has_no_fallback(self) -> None:
        client = _sonarr(retry_interval_minutes=60)
        calls = _capture_delete(client, lambda p: 500)

        client.execute_removal(self._item(action="remove"), 1, 1)

        assert len(calls) == 1  # plain remove only, no second attempt
        assert "blocklist" not in calls[0]["params"]
        assert client._retry_state.get(55) is not None

    def test_404_is_treated_as_success(self) -> None:
        client = _sonarr(retry_interval_minutes=60)
        searches = []
        client._trigger_single = lambda *a, **k: searches.append(a)
        calls = _capture_delete(client, lambda p: 404)

        client.execute_removal(self._item(), 1, 1)

        assert len(calls) == 1  # 404 on first attempt = already gone
        assert searches
        assert client._retry_state.get(55) is not None

    def test_successful_blocklist_no_fallback(self) -> None:
        client = _sonarr(retry_interval_minutes=60)
        calls = _capture_delete(client, lambda p: 200)

        client.execute_removal(self._item(), 1, 1)

        assert len(calls) == 1
        assert calls[0]["params"].get("blocklist") == "true"

    def test_failure_without_cooldown_setting_does_not_crash(self) -> None:
        client = _sonarr(retry_interval_minutes=0)  # cooldown disabled
        _capture_delete(client, lambda p: 500)

        client.execute_removal(self._item(), 1, 1)  # must not raise

        assert client._retry_state.get(55) is None  # nothing recorded when cooldown is off

    def test_dry_run_makes_no_delete_calls(self) -> None:
        client = _sonarr(dry_run=True, retry_interval_minutes=60)
        calls = _capture_delete(client, lambda p: 500)

        client.execute_removal(self._item(), 1, 1)

        assert calls == []
