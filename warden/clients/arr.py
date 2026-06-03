"""Unified *arr API clients: search, queue management, circuit breaker, and queue size checking."""

from __future__ import annotations

import datetime
import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, NamedTuple, cast

import requests

from warden.validators import classify_stall

logger = logging.getLogger(__name__)

MediaItem = tuple[int | str, str, str]
Record = dict[str, Any]
RequestParams = dict[str, str | int | list[int]]
SeasonPackThreshold = bool | int | float


class QueueItem(NamedTuple):
    queue_id: int
    media_id: int | str
    title: str
    action: str
    category: str
    messages: list[str]
    added: str = ""


class CircuitBreaker:
    def __init__(self, threshold: int) -> None:
        self.threshold = threshold
        self._failures: dict[str, int] = {}

    def record_failure(self, instance_name: str) -> None:
        if self.threshold <= 0:
            return
        self._failures[instance_name] = self._failures.get(instance_name, 0) + 1

    def record_success(self, instance_name: str) -> None:
        self._failures[instance_name] = 0

    def is_open(self, instance_name: str) -> bool:
        if self.threshold <= 0:
            return False
        return self._failures.get(instance_name, 0) >= self.threshold

    def reset(self, instance_name: str) -> None:
        self._failures.pop(instance_name, None)


class ArrClient(ABC):
    DEFAULT_FETCH_PAGE_SIZE = 2000
    ENDPOINT_COMMAND = "/api/v3/command"
    ENDPOINT_QUALITY_PROFILE = "/api/v3/qualityprofile"
    ENDPOINT_QUEUE = "/api/v3/queue"
    ENDPOINT_TAG = "/api/v3/tag"
    ENDPOINT_WANTED_CUTOFF = "/api/v3/wanted/cutoff"
    ENDPOINT_WANTED_MISSING = "/api/v3/wanted/missing"

    def __init__(
        self,
        name: str,
        url: str,
        api_key: str,
        search_settings: dict[str, Any],
        cleanup_settings: dict[str, Any],
        weight: float = 1.0,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self.name = name
        self.url = url.rstrip("/")
        self.search_settings = search_settings
        self.cleanup_settings = cleanup_settings
        self.weight = weight
        self.circuit_breaker = circuit_breaker or CircuitBreaker(0)

        self.dry_run = search_settings.get("dry_run", False) or cleanup_settings.get("dry_run", False)
        self.fetch_page_size = search_settings.get("fetch_page_size", self.DEFAULT_FETCH_PAGE_SIZE)
        self.fetch_record_limit = search_settings.get("fetch_record_limit", 0)
        self.fetch_timeout = search_settings.get("fetch_timeout_seconds", 120)
        self.queue_check_timeout = search_settings.get("queue_check_timeout_seconds", 15)
        self.stagger_seconds = search_settings.get("stagger_interval_seconds", 30)
        self.search_order = search_settings.get("search_order", "last_searched_ascending")
        self.retry_interval_days = search_settings.get("retry_interval_days", 30)
        self.retry_interval_days_missing = search_settings.get("retry_interval_days_missing")
        self.retry_interval_days_upgrade = search_settings.get("retry_interval_days_upgrade")
        self.api_request_interval_seconds = search_settings.get("api_request_interval_seconds", 0)
        self.search_jitter_seconds = search_settings.get("search_jitter_seconds", 0)

        self.max_queue_size = search_settings.get("max_queue_size", 0)
        self.cleanup_batch_size = cleanup_settings.get("batch_size", 10)
        self.cleanup_page_size = cleanup_settings.get("cleanup_page_size", 100)
        self.max_cleanup_queue_records = cleanup_settings.get("max_cleanup_queue_records", 0)
        self.cleanup_retry_minutes = cleanup_settings.get("retry_interval_minutes", 0)
        self.delete_timeout_seconds = cleanup_settings.get("delete_timeout_seconds", 15)
        self.queue_max_age_hours = cleanup_settings.get("queue_max_age_hours", 0)
        cleanup_override = cleanup_settings.get("search_after_cleanup")
        self.search_after_cleanup = (
            search_settings.get("search_after_cleanup", True) if cleanup_override is None else cleanup_override
        )
        self.search_after_cleanup_actions = set(
            search_settings.get("search_after_cleanup_actions", ["retry", "blocklist"])
        )
        self._retry_state: dict[int | str, datetime.datetime] = {}
        self._circuit_breaker_fetch = search_settings.get("circuit_breaker_threshold", 0)
        self._circuit_breaker_cleanup = cleanup_settings.get("circuit_breaker_threshold", 0)
        self._last_mutating_request = 0.0
        self._api_request_lock = threading.RLock()
        self._time_func = time.monotonic
        self._sleep_func = time.sleep

        if not self.url.lower().startswith("https://"):
            logger.info(f"Client '{name}' is using a non-HTTPS URL ({self.url}).")

        self.session = requests.Session()
        self.session.headers.update({"X-Api-Key": api_key, "Content-Type": "application/json"})

        self._include_tag_ids: set[int] = set()
        self._exclude_tag_ids: set[int] = set()
        self._tag_limits_raw: dict[str, int] = dict(search_settings.get("tag_limits", {}) or {})
        self._tag_limit_ids: dict[int, int] = {}
        self._resolve_tag_ids()

    # ----- abstract properties -----

    @property
    @abstractmethod
    def _command_name(self) -> str: ...

    @property
    @abstractmethod
    def _id_field(self) -> str: ...

    @abstractmethod
    def _get_record_title(self, record: Record) -> str: ...

    @abstractmethod
    def _get_record_tags(self, record: Record) -> list[int]: ...

    @abstractmethod
    def _get_release_date(self, record: Record) -> str: ...

    @abstractmethod
    def _is_available(self, record: Record) -> bool: ...

    @abstractmethod
    def _get_media_id(self, record: Record) -> int | str: ...

    # ----- common HTTP utilities -----

    def _api_get(self, url: str, **kwargs: Any) -> requests.Response:
        with self._api_request_lock:
            return self.session.get(url, **kwargs)

    def _api_post_command(self, url: str, payload: dict[str, Any]) -> requests.Response:
        with self._api_request_lock:
            self._prepare_search_request()
            return self.session.post(url, json=payload, timeout=self.fetch_timeout)

    def _api_delete_queue_item(self, url: str, params: dict[str, str]) -> requests.Response:
        with self._api_request_lock:
            self._throttle_mutating_request()
            return self.session.delete(url, params=params, timeout=self.delete_timeout_seconds)

    def _fetch_list(self, endpoint: str, params: RequestParams | None = None) -> list[Record]:
        url = f"{self.url}{endpoint}"
        try:
            response = self._api_get(url, params=params or {}, timeout=self.fetch_timeout)
            response.raise_for_status()
            return cast(list[Record], response.json())
        except requests.RequestException as error:
            logger.error(f"[{self.name}] Failed to fetch {endpoint}: {error}")
            return []

    def _fetch_unlimited(self, endpoint: str) -> list[Record]:
        url = f"{self.url}{endpoint}"
        result: list[Record] = []
        current_page = 1
        page_size = self.fetch_page_size
        record_limit = self.fetch_record_limit
        while True:
            params: RequestParams = {**self._extra_fetch_params(), "page": current_page, "pageSize": page_size}
            try:
                response = self._api_get(url, params=params, timeout=self.fetch_timeout)
                response.raise_for_status()
                records = cast(list[Record], response.json().get("records", []))
                if record_limit > 0:
                    remaining = record_limit - len(result)
                    if remaining <= 0:
                        break
                    records = records[:remaining]
                result.extend(records)
                if len(records) < page_size or (record_limit > 0 and len(result) >= record_limit):
                    break
                current_page += 1
            except requests.RequestException as error:
                logger.error(f"[{self.name}] Failed to fetch unlimited {endpoint}: {error}")
                break
        return result

    def _extra_fetch_params(self) -> dict[str, str]:
        return {"monitored": "true"}

    def check_connection(self) -> bool:
        url = f"{self.url}{self.ENDPOINT_TAG}"
        try:
            response = self._api_get(url, timeout=self.fetch_timeout)
            response.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def _throttle_mutating_request(self) -> None:
        if self.api_request_interval_seconds <= 0:
            return
        now = self._time_func()
        elapsed = now - self._last_mutating_request
        sleep_for = self.api_request_interval_seconds - elapsed
        if sleep_for > 0:
            logger.debug(f"[{self.name}] Throttling next mutating API call for {sleep_for:.2f}s.")
            self._sleep_func(sleep_for)
        self._last_mutating_request = self._time_func()

    def _prepare_search_request(self) -> None:
        if self.search_jitter_seconds > 0:
            delay = random.uniform(0, self.search_jitter_seconds)
            if delay > 0:
                logger.debug(f"[{self.name}] Applying search jitter for {delay:.2f}s.")
                self._sleep_func(delay)
        self._throttle_mutating_request()

    # ----- tag filtering -----

    def _resolve_tag_ids(self) -> None:
        include_names: list[str] = self.search_settings.get("include_tags", []) or self.cleanup_settings.get(
            "include_tags", []
        )
        exclude_names: list[str] = self.search_settings.get("exclude_tags", []) or self.cleanup_settings.get(
            "exclude_tags", []
        )
        if include_names or exclude_names or self._tag_limits_raw:
            url = f"{self.url}{self.ENDPOINT_TAG}"
            try:
                response = self._api_get(url, timeout=15)
                response.raise_for_status()
                tag_map = {tag["label"].lower(): tag["id"] for tag in response.json()}
                self._include_tag_ids = self._resolve_tag_names(tag_map, include_names)
                self._exclude_tag_ids = self._resolve_tag_names(tag_map, exclude_names)
                self._tag_limit_ids = self._resolve_tag_limit_ids(tag_map)
            except requests.RequestException as err:
                logger.error(f"[{self.name}] Failed to fetch tags, tag filtering disabled: {err}")

    def _resolve_tag_limit_ids(self, tag_map: dict[str, int]) -> dict[int, int]:
        result: dict[int, int] = {}
        for label, limit in self._tag_limits_raw.items():
            tag_id = tag_map.get(label.lower())
            if tag_id is None:
                logger.warning(f"[{self.name}] tag_limits tag not found, ignoring: {label}")
            else:
                result[tag_id] = limit
        return result

    @property
    def uses_tag_limits(self) -> bool:
        """Whether this client self-bounds its search via per-tag limits (bypasses global allocation)."""
        return False

    def _resolve_tag_names(self, tag_map: dict[str, int], names: list[str]) -> set[int]:
        result: set[int] = set()
        for name in names:
            tag_id = tag_map.get(name.lower())
            if tag_id is None:
                logger.warning(f"[{self.name}] Tag not found, ignoring: {name}")
            else:
                result.add(tag_id)
        return result

    def _is_tag_filtered_out(self, record: Record) -> bool:
        record_tag_ids = set(self._get_record_tags(record))
        return bool(
            (self._exclude_tag_ids and record_tag_ids & self._exclude_tag_ids)
            or (self._include_tag_ids and not record_tag_ids & self._include_tag_ids)
        )

    # ----- retry / date utilities -----

    def _is_date_past(self, date_str: str | None) -> bool:
        if date_str:
            now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            return date_str <= now
        return False

    def _is_within_retry_window(self, record: Record, reason: str) -> bool:
        last_search = record.get("lastSearchTime")
        interval = self.retry_interval_days
        if reason == "missing" and self.retry_interval_days_missing is not None:
            interval = self.retry_interval_days_missing
        elif reason == "upgrade" and self.retry_interval_days_upgrade is not None:
            interval = self.retry_interval_days_upgrade
        if interval > 0 and last_search:
            last_search_dt = datetime.datetime.fromisoformat(last_search)
            cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=interval)
            return last_search_dt > cutoff
        return False

    def _is_within_cleanup_retry(self, media_id: int | str) -> bool:
        recorded = self._retry_state.get(media_id)
        if self.cleanup_retry_minutes == 0 or recorded is None:
            return False
        cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=self.cleanup_retry_minutes)
        if recorded <= cutoff:
            del self._retry_state[media_id]
        return recorded > cutoff

    def _record_cleanup_retry(self, media_id: int | str, title: str) -> None:
        if self.cleanup_retry_minutes == 0:
            return
        self._retry_state[media_id] = datetime.datetime.now(datetime.UTC)
        logger.debug(f"[{self.name}] Cooldown recorded for media ID {media_id}: {title}")

    # ----- sorting -----

    def _sort_records_client_side(self, records: list[Record]) -> None:
        sort_keys = {
            "alphabetical": self._get_record_title,
            "last_added": lambda rec: rec.get("dateAdded") or "",
            "last_searched": lambda rec: rec.get("lastSearchTime") or "",
            "release_date": self._get_release_date,
        }
        if self.search_order != "random":
            base = self.search_order.rsplit("_", 1)[0]
            reverse = self.search_order.endswith("_descending")
            records.sort(key=sort_keys[base], reverse=reverse)

    # ----- search pipeline -----

    def _extract_item(self, record: Record, reason: str) -> MediaItem:
        return (record["id"], reason, self._get_record_title(record))

    def _process_record(
        self,
        record: Record,
        reason: str,
        seen: set[int],
        check_availability: bool = False,
    ) -> MediaItem | None:
        record_id = record.get("id")
        if record_id is None or record_id in seen:
            return None
        if self._is_tag_filtered_out(record):
            logger.debug(f"[{self.name}] Skipping {reason} item (tag filter): {self._get_record_title(record)}")
            return None
        if check_availability and not self._is_available(record):
            logger.debug(f"[{self.name}] Skipping {reason} item (not yet available): {self._get_record_title(record)}")
            return None
        if self._is_within_retry_window(record, reason):
            title = self._get_record_title(record)
            logger.debug(
                f"[{self.name}] Skipping {reason} item (within retry window, last searched: {record.get('lastSearchTime')}): {title}"
            )
            return None
        seen.add(record_id)
        return self._extract_item(record, reason)

    def _get_target_media(
        self,
        endpoint: str,
        target_batch_size: int,
        reason: str,
        seen: set[int],
        check_availability: bool = False,
    ) -> list[MediaItem]:
        items: list[MediaItem] = []
        if target_batch_size == 0:
            return items
        records = self._fetch_unlimited(endpoint)
        if not records:
            return items
        self._sort_records_client_side(records)
        for record in records:
            if 0 < target_batch_size <= len(items):
                break
            item = self._process_record(record, reason, seen, check_availability)
            if item:
                items.append(item)
        return items

    def _interleave_items(self, missing_items: list[MediaItem], upgrade_items: list[MediaItem]) -> list[MediaItem]:
        total_missing = len(missing_items)
        total_upgrade = len(upgrade_items)
        total = total_missing + total_upgrade
        result = []
        mi, ui = 0, 0
        for current in range(total):
            missing_ratio = total_missing / total if total > 0 else 0
            current_ratio = mi / (current + 1)
            if mi < total_missing and (ui >= total_upgrade or current_ratio < missing_ratio):
                result.append(missing_items[mi])
                mi += 1
            elif ui < total_upgrade:
                result.append(upgrade_items[ui])
                ui += 1
        return result

    def _fetch_quality_profile_cutoffs(self) -> dict[int, int]:
        profiles = self._fetch_list(self.ENDPOINT_QUALITY_PROFILE)
        return {p["id"]: p["cutoffFormatScore"] for p in profiles if p.get("cutoffFormatScore", 0) > 0}

    def _get_custom_format_upgrade_records(self, _profile_cutoffs: dict[int, int]) -> list[Record]:
        return []

    def _get_custom_format_score_unmet_records(self) -> list[Record]:
        profile_cutoffs = self._fetch_quality_profile_cutoffs()
        if not profile_cutoffs:
            return []
        return self._get_custom_format_upgrade_records(profile_cutoffs)

    def get_media_to_search(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        if self._circuit_breaker_fetch > 0 and self.circuit_breaker.is_open(self.name):
            logger.warning(f"[{self.name}] Circuit breaker open — skipping fetch cycle.")
            return []
        try:
            seen: set[int] = set()
            missing_items = self._get_target_media(
                self.ENDPOINT_WANTED_MISSING,
                missing_batch_size,
                "missing",
                seen,
                check_availability=True,
            )
            upgrade_items = self._get_target_media(
                self.ENDPOINT_WANTED_CUTOFF,
                upgrade_batch_size,
                "upgrade",
                seen,
            )
            if upgrade_batch_size != 0:
                supplemental = self._get_custom_format_score_unmet_records()
                self._sort_records_client_side(supplemental)
                for record in supplemental:
                    if 0 < upgrade_batch_size <= len(upgrade_items):
                        break
                    item = self._process_record(record, "upgrade", seen)
                    if item:
                        upgrade_items.append(item)
            merged = self._interleave_items(missing_items, upgrade_items)
            if self.search_order == "random":
                random.shuffle(merged)
            self.circuit_breaker.record_success(self.name)
            return merged
        except Exception:
            self.circuit_breaker.record_failure(self.name)
            raise

    def trigger_search(self, items: list[MediaItem], *, index: int | None = None, total: int | None = None) -> None:
        display_total = total if total is not None else len(items)
        for local_index, (item_id, reason, title) in enumerate(items, start=1):
            display_index = index if index is not None else local_index
            self._trigger_single(item_id, reason, title, display_index, display_total)
            if self.stagger_seconds > 0 and local_index < len(items):
                logger.debug(f"[{self.name}] Staggering next search by {self.stagger_seconds}s.")
                time.sleep(self.stagger_seconds)

    def _trigger_single(self, item_id: int | str, reason: str, title: str, index: int, total: int) -> None:
        if self.dry_run:
            logger.info(f"[{self.name}] [DRY RUN] Would search ({reason}): {title} ({index}/{total})")
        else:
            url = f"{self.url}{self.ENDPOINT_COMMAND}"
            payload = {"name": self._command_name, self._id_field: [item_id]}
            try:
                response = self._api_post_command(url, payload)
                response.raise_for_status()
                logger.info(f"[{self.name}] Searching ({reason}): {title} ({index}/{total})")
            except requests.RequestException as error:
                logger.error(
                    f"[{self.name}] Failed to trigger {self._command_name} for {title} (ID: {item_id}): {error}"
                )

    # ----- queue / cleanup pipeline -----

    def _check_queue_size(self) -> bool:
        if self.max_queue_size <= 0:
            return True
        try:
            url = f"{self.url}{self.ENDPOINT_QUEUE}"
            params: RequestParams = {"page": 1, "pageSize": 1, "includeUnknownSeriesItems": "false"}
            response = self._api_get(url, params=params, timeout=self.queue_check_timeout)
            response.raise_for_status()
            total = cast(int, response.json().get("totalRecords", 0))
            if total >= self.max_queue_size:
                logger.info(f"[{self.name}] Queue size ({total}) >= max ({self.max_queue_size}) — pausing searches.")
                return False
            return True
        except requests.Timeout as error:
            logger.warning(f"[{self.name}] Timed out checking queue size; continuing searches: {error}")
            return True
        except requests.RequestException as error:
            logger.error(f"[{self.name}] Failed to check queue size: {error}")
            return False

    def is_queue_too_large(self) -> bool:
        return not self._check_queue_size()

    def _fetch_all_queue(self) -> tuple[list[Record], bool]:
        result: list[Record] = []
        fetch_failed = False
        current_page = 1
        page_size = self.cleanup_page_size
        record_cap = self.max_cleanup_queue_records
        cleanup_timeout = self.cleanup_settings.get("fetch_timeout_seconds", 30)
        while True:
            url = f"{self.url}{self.ENDPOINT_QUEUE}"
            params = {"page": current_page, "pageSize": page_size}
            try:
                response = self._api_get(url, params=params, timeout=cleanup_timeout)
                response.raise_for_status()
                records = response.json().get("records", [])
                if record_cap > 0:
                    remaining = record_cap - len(result)
                    if remaining <= 0:
                        break
                    records = records[:remaining]
                result.extend(records)
                if len(records) < page_size or (record_cap > 0 and len(result) >= record_cap):
                    break
                current_page += 1
            except requests.RequestException as error:
                logger.error(f"[{self.name}] Failed to fetch queue: {error}")
                fetch_failed = True
                break
        return result, fetch_failed

    def _is_healthy_in_progress(self, record: Record) -> bool:
        """Whether the download client reports this item as healthy and still in flight.

        Such items are exempt from age-based (``queue_max_age_hours``) reaping so Warden
        never blocklists a download that is legitimately still progressing — slow is not
        the same as stalled. If it later genuinely stalls, the *arr flips
        ``trackedDownloadStatus`` to ``warning`` and the normal stall path reaps it
        immediately, regardless of age. (Explicit problem states — ``warning``,
        ``failed``, ``downloadClientUnavailable`` — are handled by ``_is_stalled`` before
        this is consulted.)
        """
        if record.get("trackedDownloadStatus", "") == "warning":
            return False
        status = record.get("status", "")
        tracked_state = record.get("trackedDownloadState", "")
        return status in ("downloading", "queued", "delay", "paused") or tracked_state in (
            "downloading",
            "queued",
        )

    def _is_stalled(self, record: Record) -> bool:
        status = record.get("status", "")
        tracked_status = record.get("trackedDownloadStatus", "")
        if tracked_status == "warning":
            return True
        if status == "downloadClientUnavailable":
            return True
        if status == "failed":
            return True
        if self.queue_max_age_hours > 0:
            added = record.get("added", "")
            # Age-reap only items that are NOT healthily downloading — protects slow but
            # healthy in-progress downloads from being blocklisted just for being old.
            if added and status != "completed" and not self._is_healthy_in_progress(record):
                try:
                    added_dt = datetime.datetime.fromisoformat(added.replace("Z", "+00:00"))
                    if datetime.datetime.now(datetime.UTC) - added_dt > datetime.timedelta(
                        hours=self.queue_max_age_hours
                    ):
                        return True
                except (ValueError, TypeError):
                    pass
        return False

    def _resolve_cleanup_action(self, category: str) -> str:
        action = self.cleanup_settings.get(category)
        if not action and category != "stalled":
            action = self.cleanup_settings.get("stalled")
        return action or "ignore"

    def _get_status_messages(self, record: Record) -> list[str]:
        messages: list[str] = []
        for msg_obj in record.get("statusMessages", []):
            title = msg_obj.get("title")
            if title:
                messages.append(title)
            messages.extend(msg_obj.get("messages", []))
        return list(dict.fromkeys(messages))

    def _get_skip_series_ids(self, all_records: list[Record]) -> set[int]:
        """Return series IDs whose stalled items should be protected this cycle.

        Default returns an empty set — subclasses override to implement series-aware protection.
        """
        return set()

    def get_stalled_items(self) -> tuple[list[QueueItem], dict[str, int]]:
        empty_stats = {
            "total_evaluated": 0,
            "ignored": 0,
            "tag_filtered": 0,
            "not_stalled": 0,
            "retry_interval": 0,
            "series_protected": 0,
        }
        if self._circuit_breaker_cleanup > 0 and self.circuit_breaker.is_open(f"{self.name}_cleanup"):
            logger.warning(f"[{self.name}] Circuit breaker open for cleanup — skipping.")
            return [], empty_stats

        if self.cleanup_batch_size == 0:
            return [], empty_stats

        try:
            all_records, fetch_failed = self._fetch_all_queue()
            if fetch_failed:
                self.circuit_breaker.record_failure(f"{self.name}_cleanup")
            skip_series_ids = self._get_skip_series_ids(all_records)
            items: list[QueueItem] = []
            skip_stats = {
                "total_evaluated": len(all_records),
                "ignored": 0,
                "tag_filtered": 0,
                "not_stalled": 0,
                "retry_interval": 0,
                "series_protected": 0,
            }

            for record in all_records:
                title = self._get_record_title(record)
                if not self._is_stalled(record):
                    skip_stats["not_stalled"] += 1
                    continue

                if skip_series_ids and record.get("seriesId") in skip_series_ids:
                    logger.debug(f"[{self.name}] Protecting stalled item (active series in progress): {title}")
                    skip_stats["series_protected"] += 1
                    continue

                status = record.get("status", "")
                messages: list[str]
                if status == "downloadClientUnavailable":
                    category = "download_unavailable"
                    messages = []
                    action = self._resolve_cleanup_action(category)
                else:
                    messages = self._get_status_messages(record)
                    category = classify_stall(messages)
                    action = self._resolve_cleanup_action(category)
                    if category == "unknown":
                        log_message = (
                            f'[{self.name}] Unrecognized status messages for "{title}" — please report: {messages}'
                        )
                        if action == "ignore":
                            logger.debug(log_message)
                        else:
                            logger.warning(log_message)

                if action == "ignore":
                    logger.debug(f"[{self.name}] Skipping stalled item (action: ignore, category: {category}): {title}")
                    skip_stats["ignored"] += 1
                    continue

                media_id = self._get_media_id(record)
                if self._is_within_cleanup_retry(media_id):
                    logger.debug(f"[{self.name}] Skipping stalled item (retry_interval, media ID {media_id}): {title}")
                    skip_stats["retry_interval"] += 1
                    continue

                if self._is_tag_filtered_out(record):
                    logger.debug(f"[{self.name}] Skipping stalled item (tag filter): {title}")
                    skip_stats["tag_filtered"] += 1
                    continue

                queue_id = record["id"]
                added = record.get("added", "")
                items.append(QueueItem(queue_id, media_id, title, action, category, messages, added))

                if self.cleanup_batch_size > 0 and len(items) >= self.cleanup_batch_size:
                    break

            if not fetch_failed:
                self.circuit_breaker.record_success(f"{self.name}_cleanup")
            return items, skip_stats
        except Exception:
            self.circuit_breaker.record_failure(f"{self.name}_cleanup")
            raise

    def execute_removal(self, item: QueueItem, index: int, total: int) -> None:
        logger.debug(f'[{self.name}] Stall details for "{item.title}": {item.messages}')
        if self.dry_run:
            logger.info(
                f"[{self.name}] [DRY RUN] Would {item.action} ({item.category}): {item.title} ({index}/{total})"
            )
            return

        removed = self._delete_queue_item(item, index, total)
        if not removed:
            # Terminal failure even after the fallback. Cool the item down so subsequent
            # cycles don't re-hammer the same failing removal (avoids log spam + wasted
            # API calls on an item the *arr currently refuses to drop). Takes effect only
            # when retry_interval_minutes > 0; otherwise it is retried next cycle as before.
            self._record_cleanup_retry(item.media_id, item.title)
            return

        if self.search_after_cleanup and item.action in self.search_after_cleanup_actions:
            try:
                self._trigger_single(item.media_id, item.action, item.title, index, total)
            except Exception:
                logger.exception(f"[{self.name}] Failed to trigger replacement search for {item.title}.")

    def _delete_queue_item(self, item: QueueItem, index: int, total: int) -> bool:
        """Delete a queue item, degrading gracefully if the primary attempt fails.

        Strategy:
          1. Primary attempt — ``blocklist``+remove for blocklist actions, else a plain
             remove (``removeFromClient=true``).
          2. If the primary attempt asked to blocklist and it errors, retry **once** as a
             plain remove without blocklist. Some download clients (e.g. decypharr) return
             HTTP 500 on the blocklist variant for items they cannot blocklist yet still
             honour a plain removal — so this clears the queue item instead of failing
             every cycle forever.

        A 404 is treated as success (the item was already gone / removed via cascade).
        Records the cleanup cooldown on success. Returns True if removed, else False.
        """
        base_params: dict[str, str] = {"removeFromClient": "true"}
        wants_blocklist = item.action == "blocklist"
        attempts: list[tuple[dict[str, str], str]] = []
        if wants_blocklist:
            attempts.append(({**base_params, "blocklist": "true"}, item.action))
            attempts.append((dict(base_params), "remove"))  # fallback: drop without blocklisting
        else:
            attempts.append((dict(base_params), item.action))

        url = f"{self.url}{self.ENDPOINT_QUEUE}/{item.queue_id}"
        last_error: Exception | None = None
        for attempt_idx, (params, effective_action) in enumerate(attempts):
            is_fallback = attempt_idx > 0
            try:
                response = self._api_delete_queue_item(url, params)
                if response.status_code == 404:
                    logger.info(
                        f"[{self.name}] Removed ({item.action}, {item.category}, cascade): "
                        f"{item.title} ({index}/{total})"
                    )
                    self._record_cleanup_retry(item.media_id, item.title)
                    return True
                response.raise_for_status()
                detail = f"{effective_action}, {item.category}" + (", fallback" if is_fallback else "")
                logger.info(f"[{self.name}] Removed ({detail}): {item.title} ({index}/{total})")
                self._record_cleanup_retry(item.media_id, item.title)
                return True
            except requests.RequestException as error:
                last_error = error
                if not is_fallback and wants_blocklist:
                    logger.warning(
                        f"[{self.name}] Blocklist-remove failed for {item.title} "
                        f"(ID: {item.queue_id}): {error}. Retrying as plain remove."
                    )

        logger.error(
            f"[{self.name}] Failed to remove {item.title} (ID: {item.queue_id}) "
            f"after {len(attempts)} attempt(s): {last_error}"
        )
        return False


class LidarrClient(ArrClient):
    ENDPOINT_COMMAND = "/api/v1/command"
    ENDPOINT_QUEUE = "/api/v1/queue"
    ENDPOINT_TAG = "/api/v1/tag"
    ENDPOINT_WANTED_CUTOFF = "/api/v1/wanted/cutoff"
    ENDPOINT_WANTED_MISSING = "/api/v1/wanted/missing"
    ARTIST_ID_PREFIX = "artist:"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.search_type = self.search_settings.get("search_type") or "album"

    @property
    def _command_name(self) -> str:
        return "ArtistSearch" if self.search_type == "artist" else "AlbumSearch"

    @property
    def _id_field(self) -> str:
        return "albumIds"

    def _get_record_title(self, record: Record) -> str:
        artist_name = record.get("artist", {}).get("artistName", "Unknown Artist")
        album_title = record.get("title", "Unknown Album")
        return f"{artist_name} - {album_title}"

    def _get_record_tags(self, record: Record) -> list[int]:
        return cast(list[int], record.get("artist", {}).get("tags", []) or record.get("tags", []))

    def _get_release_date(self, record: Record) -> str:
        return record.get("releaseDate") or ""

    def _is_available(self, record: Record) -> bool:
        return self._is_date_past(record.get("releaseDate"))

    def _get_media_id(self, record: Record) -> int:
        return cast(int, record.get("albumId") or record["id"])

    def _fetch_quality_profile_cutoffs(self) -> dict[int, int]:
        return {}

    def get_media_to_search(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        if self._circuit_breaker_fetch > 0 and self.circuit_breaker.is_open(self.name):
            logger.warning(f"[{self.name}] Circuit breaker open — skipping fetch cycle.")
            return []
        try:
            if self.search_type == "artist":
                result = self._get_media_to_search_by_artist(missing_batch_size, upgrade_batch_size)
                self.circuit_breaker.record_success(self.name)
                return result
            return super().get_media_to_search(missing_batch_size, upgrade_batch_size)
        except Exception:
            self.circuit_breaker.record_failure(self.name)
            raise

    def _get_media_to_search_by_artist(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        seen_artists: set[int] = set()
        missing_items: list[MediaItem] = []
        upgrade_items: list[MediaItem] = []

        if missing_batch_size != 0:
            records = self._fetch_unlimited(self.ENDPOINT_WANTED_MISSING)
            self._sort_records_client_side(records)
            for record in records:
                if 0 < missing_batch_size <= len(missing_items):
                    break
                if self._is_tag_filtered_out(record):
                    continue
                if not self._is_available(record):
                    continue
                if self._is_within_retry_window(record, "missing"):
                    continue
                artist_id = record.get("artist", {}).get("id")
                if artist_id is None or artist_id in seen_artists:
                    continue
                seen_artists.add(artist_id)
                artist_name = record.get("artist", {}).get("artistName", "Unknown Artist")
                missing_items.append((f"{self.ARTIST_ID_PREFIX}{artist_id}", "missing", artist_name))

        if upgrade_batch_size != 0:
            records = self._fetch_unlimited(self.ENDPOINT_WANTED_CUTOFF)
            self._sort_records_client_side(records)
            for record in records:
                if 0 < upgrade_batch_size <= len(upgrade_items):
                    break
                if self._is_tag_filtered_out(record):
                    continue
                if self._is_within_retry_window(record, "upgrade"):
                    continue
                artist_id = record.get("artist", {}).get("id")
                if artist_id is None or artist_id in seen_artists:
                    continue
                seen_artists.add(artist_id)
                artist_name = record.get("artist", {}).get("artistName", "Unknown Artist")
                upgrade_items.append((f"{self.ARTIST_ID_PREFIX}{artist_id}", "upgrade", artist_name))

        merged = self._interleave_items(missing_items, upgrade_items)
        if self.search_order == "random":
            random.shuffle(merged)
        return merged

    def _trigger_single(self, item_id: int | str, reason: str, title: str, index: int, total: int) -> None:
        if isinstance(item_id, str) and item_id.startswith(self.ARTIST_ID_PREFIX):
            artist_id = int(item_id.removeprefix(self.ARTIST_ID_PREFIX))
            if self.dry_run:
                logger.info(f"[{self.name}] [DRY RUN] Would search ({reason}): {title} ({index}/{total})")
            else:
                url = f"{self.url}{self.ENDPOINT_COMMAND}"
                payload = {"name": "ArtistSearch", "artistId": artist_id}
                try:
                    response = self._api_post_command(url, payload)
                    response.raise_for_status()
                    logger.info(f"[{self.name}] Searching ({reason}): {title} ({index}/{total})")
                except requests.RequestException as error:
                    logger.error(
                        f"[{self.name}] Failed to trigger ArtistSearch for {title} (Artist ID: {artist_id}): {error}"
                    )
        else:
            super()._trigger_single(item_id, reason, title, index, total)


class RadarrClient(ArrClient):
    ENDPOINT_COLLECTION = "/api/v3/collection"
    ENDPOINT_MOVIE = "/api/v3/movie"
    ENDPOINT_MOVIE_FILE = "/api/v3/moviefile"
    COLLECTION_ID_PREFIX = "collection:"
    MOVIE_FILE_BATCH_SIZE = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.search_type = self.search_settings.get("search_type") or "movie"
        self.collection_search_mode = self.search_settings.get("radarr_collection_search_mode", "off")

    @property
    def _command_name(self) -> str:
        return "MoviesSearch"

    @property
    def _id_field(self) -> str:
        return "movieIds"

    def _trigger_single(self, item_id: int | str, reason: str, title: str, index: int, total: int) -> None:
        if isinstance(item_id, str) and item_id.startswith(self.COLLECTION_ID_PREFIX):
            collection_id = int(item_id.removeprefix(self.COLLECTION_ID_PREFIX))
            if self.dry_run:
                logger.info(f"[{self.name}] [DRY RUN] Would search ({reason}): {title} ({index}/{total})")
                return
            url = f"{self.url}{self.ENDPOINT_COMMAND}"
            payload = {"name": "CollectionSearch", "collectionIds": [collection_id]}
            try:
                response = self._api_post_command(url, payload)
                response.raise_for_status()
                logger.info(f"[{self.name}] Searching ({reason}, collection): {title} ({index}/{total})")
            except requests.RequestException as error:
                logger.error(
                    f"[{self.name}] CollectionSearch failed for {title}; falling back to MoviesSearch: {error}"
                )
                super()._trigger_single(collection_id, reason, title, index, total)
            return
        super()._trigger_single(item_id, reason, title, index, total)

    def _get_record_title(self, record: Record) -> str:
        return cast(str, record.get("title", f"Movie {record.get('id', 'Unknown')}"))

    def _get_record_tags(self, record: Record) -> list[int]:
        return cast(list[int], record.get("tags", []))

    def _get_release_date(self, record: Record) -> str:
        return record.get("releaseDate") or ""

    def _is_available(self, record: Record) -> bool:
        return cast(bool, record.get("isAvailable", True))

    def _get_media_id(self, record: Record) -> int:
        return cast(int, record["movieId"])

    def _fetch_movie_collection_map(self) -> dict[int, tuple[int, str]]:
        """Return {movie_id: (collection_id, collection_title)} for all movies in a collection."""
        movies = self._fetch_list(self.ENDPOINT_MOVIE)
        result: dict[int, tuple[int, str]] = {}
        for movie in movies:
            movie_id = movie.get("id")
            collection = movie.get("collection")
            if movie_id is None or not collection:
                continue
            coll_id = collection.get("id")
            coll_title = collection.get("title") or f"Collection {coll_id}"
            if coll_id is not None:
                result[movie_id] = (coll_id, coll_title)
        return result

    def get_media_to_search(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        if self._circuit_breaker_fetch > 0 and self.circuit_breaker.is_open(self.name):
            logger.warning(f"[{self.name}] Circuit breaker open — skipping fetch cycle.")
            return []
        try:
            if self.search_type == "collection":
                result = self._get_media_to_search_by_collection(missing_batch_size, upgrade_batch_size)
                self.circuit_breaker.record_success(self.name)
                return result
            return super().get_media_to_search(missing_batch_size, upgrade_batch_size)
        except Exception:
            self.circuit_breaker.record_failure(self.name)
            raise

    def _get_media_to_search_by_collection(
        self,
        missing_batch_size: int,
        upgrade_batch_size: int,
    ) -> list[MediaItem]:
        if self.collection_search_mode == "force":
            return self._get_collections_force(missing_batch_size)
        return self._get_collections_detect(missing_batch_size, upgrade_batch_size)

    def _get_collections_force(self, missing_batch_size: int) -> list[MediaItem]:
        """Fetch monitored collections with missing movies from /api/v3/collection."""
        collections = self._fetch_list(self.ENDPOINT_COLLECTION)
        items: list[MediaItem] = []
        for collection in collections:
            if 0 < missing_batch_size <= len(items):
                break
            if not collection.get("monitored", False):
                continue
            has_missing = any(
                m.get("monitored", False) and not m.get("hasFile", False) and m.get("isAvailable", True)
                for m in collection.get("movies", [])
            )
            if not has_missing:
                continue
            collection_id = collection.get("id")
            if collection_id is None:
                continue
            title = collection.get("title") or f"Collection {collection_id}"
            items.append((f"{self.COLLECTION_ID_PREFIX}{collection_id}", "missing", title))
        if self.search_order == "random":
            random.shuffle(items)
        return items

    def _get_collections_detect(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        """Process wanted endpoints, grouping movies into CollectionSearch where possible."""
        movie_collection_map = self._fetch_movie_collection_map()
        seen_collections: set[int] = set()
        seen_movies: set[int] = set()
        missing_items: list[MediaItem] = []
        upgrade_items: list[MediaItem] = []

        if missing_batch_size != 0:
            records = self._fetch_unlimited(self.ENDPOINT_WANTED_MISSING)
            self._sort_records_client_side(records)
            for record in records:
                if 0 < missing_batch_size <= len(missing_items):
                    break
                if self._is_tag_filtered_out(record):
                    continue
                if not self._is_available(record):
                    continue
                if self._is_within_retry_window(record, "missing"):
                    continue
                movie_id = record.get("movieId") or record.get("id")
                record_id = record.get("id")
                if movie_id is None or record_id is None:
                    continue
                coll = movie_collection_map.get(movie_id)
                if coll:
                    coll_id, coll_title = coll
                    if coll_id in seen_collections:
                        continue
                    seen_collections.add(coll_id)
                    seen_movies.add(movie_id)
                    missing_items.append((f"{self.COLLECTION_ID_PREFIX}{coll_id}", "missing", coll_title))
                else:
                    if movie_id in seen_movies:
                        continue
                    seen_movies.add(movie_id)
                    missing_items.append((record_id, "missing", self._get_record_title(record)))

        if upgrade_batch_size != 0:
            records = self._fetch_unlimited(self.ENDPOINT_WANTED_CUTOFF)
            self._sort_records_client_side(records)
            for record in records:
                if 0 < upgrade_batch_size <= len(upgrade_items):
                    break
                if self._is_tag_filtered_out(record):
                    continue
                if self._is_within_retry_window(record, "upgrade"):
                    continue
                movie_id = record.get("movieId") or record.get("id")
                record_id = record.get("id")
                if movie_id is None or record_id is None:
                    continue
                coll = movie_collection_map.get(movie_id)
                if coll:
                    coll_id, coll_title = coll
                    if coll_id in seen_collections:
                        continue
                    seen_collections.add(coll_id)
                    seen_movies.add(movie_id)
                    upgrade_items.append((f"{self.COLLECTION_ID_PREFIX}{coll_id}", "upgrade", coll_title))
                else:
                    if movie_id in seen_movies:
                        continue
                    seen_movies.add(movie_id)
                    upgrade_items.append((record_id, "upgrade", self._get_record_title(record)))

            upgrades_so_far = len(upgrade_items)
            remaining = max(0, upgrade_batch_size - upgrades_so_far) if upgrade_batch_size > 0 else upgrade_batch_size
            if remaining != 0:
                supplemental = self._get_custom_format_score_unmet_records()
                self._sort_records_client_side(supplemental)
                for record in supplemental:
                    if 0 < upgrade_batch_size <= len(upgrade_items):
                        break
                    if self._is_tag_filtered_out(record):
                        continue
                    if self._is_within_retry_window(record, "upgrade"):
                        continue
                    movie_id = record.get("id")
                    if movie_id is None or movie_id in seen_movies:
                        continue
                    coll = movie_collection_map.get(movie_id)
                    if coll:
                        coll_id, coll_title = coll
                        if coll_id in seen_collections:
                            continue
                        seen_collections.add(coll_id)
                        seen_movies.add(movie_id)
                        upgrade_items.append((f"{self.COLLECTION_ID_PREFIX}{coll_id}", "upgrade", coll_title))
                    else:
                        seen_movies.add(movie_id)
                        upgrade_items.append((movie_id, "upgrade", self._get_record_title(record)))

        merged = self._interleave_items(missing_items, upgrade_items)
        if self.search_order == "random":
            random.shuffle(merged)
        return merged

    def _fetch_movie_file_scores(self, file_ids: list[int]) -> dict[int, int]:
        scores: dict[int, int] = {}
        for batch_start in range(0, len(file_ids), self.MOVIE_FILE_BATCH_SIZE):
            batch = file_ids[batch_start : batch_start + self.MOVIE_FILE_BATCH_SIZE]
            movie_files = self._fetch_list(self.ENDPOINT_MOVIE_FILE, {"movieFileIds": batch})
            for mfile in movie_files:
                score = mfile.get("customFormatScore")
                scores[mfile["id"]] = score if score is not None else 0
        return scores

    def _get_custom_format_upgrade_records(self, profile_cutoffs: dict[int, int]) -> list[Record]:
        movies = self._fetch_list(self.ENDPOINT_MOVIE)
        candidates: dict[int, tuple[Record, int]] = {}
        for movie in movies:
            if not movie.get("monitored", False):
                continue
            cutoff_score = profile_cutoffs.get(movie.get("qualityProfileId", -1), 0)
            file_id = movie.get("movieFileId")
            if cutoff_score > 0 and file_id:
                candidates[file_id] = (movie, cutoff_score)
        result: list[Record] = []
        if candidates:
            scores = self._fetch_movie_file_scores(list(candidates.keys()))
            result = [
                movie for file_id, (movie, cutoff_score) in candidates.items() if scores.get(file_id, 0) < cutoff_score
            ]
        return result


class SonarrClient(ArrClient):
    ENDPOINT_EPISODE = "/api/v3/episode"
    ENDPOINT_EPISODE_FILE = "/api/v3/episodefile"
    ENDPOINT_SERIES = "/api/v3/series"
    SEASON_ID_PREFIX = "season:"
    SERIES_ID_PREFIX = "series:"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.search_type = self.search_settings.get("search_type") or "episode"
        self.season_packs: SeasonPackThreshold = self.search_settings.get("season_packs", False)
        self.protect_downloading_series: bool = self.cleanup_settings.get("protect_downloading_series", False)
        self.cleanup_search_scope: str = self.cleanup_settings.get("cleanup_search_scope", "episode")
        # Per-tag rotating cursor (in-memory) so successive tag_limits cycles walk
        # through a tag's series backlog instead of re-searching the same top-N every
        # cycle. Resets on restart — intentionally stateless, no disk persistence.
        self._tag_search_cursor: dict[int, int] = {}

    @property
    def uses_tag_limits(self) -> bool:
        return bool(self._tag_limit_ids)

    def _get_skip_series_ids(self, all_records: list[Record]) -> set[int]:
        """Return series IDs that have at least one non-stalled download still in progress.

        When `protect_downloading_series` is enabled, stalled items from these series are held
        back for the cycle so active multi-season downloads are not disrupted.
        """
        if not self.protect_downloading_series:
            return set()
        protected: set[int] = set()
        for record in all_records:
            if not self._is_stalled(record):
                series_id = record.get("seriesId")
                if series_id is not None:
                    protected.add(series_id)
        if protected:
            logger.debug(f"[{self.name}] Series with active downloads (protected this cycle): {sorted(protected)}")
        return protected

    @property
    def _command_name(self) -> str:
        return "EpisodeSearch"

    @property
    def _id_field(self) -> str:
        return "episodeIds"

    def _get_record_title(self, record: Record) -> str:
        series_title = record.get("series", {}).get("title", "Unknown Series")
        season = record.get("seasonNumber", 0)
        episode = record.get("episodeNumber", 0)
        episode_title = record.get("title", "Unknown Episode")
        return f"{series_title} - S{season:02d}E{episode:02d} - {episode_title}"

    def _get_record_tags(self, record: Record) -> list[int]:
        return cast(list[int], record.get("series", {}).get("tags", []))

    def _get_release_date(self, record: Record) -> str:
        return record.get("airDateUtc") or ""

    def _is_available(self, record: Record) -> bool:
        return self._is_date_past(record.get("airDateUtc"))

    def _extract_item(self, record: Record, reason: str) -> MediaItem:
        return (record.get("episodeId") or record["id"], reason, self._get_record_title(record))

    def _get_media_id(self, record: Record) -> int | str:
        series_id = record.get("seriesId") or record.get("series", {}).get("id")
        if self.cleanup_search_scope == "series" and series_id is not None:
            return f"{self.SERIES_ID_PREFIX}{series_id}"
        if self.cleanup_search_scope == "season" and series_id is not None:
            season_number = record.get("seasonNumber")
            if season_number is not None:
                return f"{self.SEASON_ID_PREFIX}{series_id}:{season_number}"
        return cast(int | str, record["episodeId"])

    def _extra_fetch_params(self) -> dict[str, str]:
        return {"includeSeries": "true", "monitored": "true"}

    def _get_series_id(self, record: Record) -> int | None:
        return cast(int | None, record.get("series", {}).get("id"))

    def _get_season_number(self, record: Record) -> int | None:
        return record.get("seasonNumber")

    def _fetch_season_metadata(self) -> dict[tuple[int, int], Record]:
        series_list = self._fetch_list(self.ENDPOINT_SERIES)
        result: dict[tuple[int, int], Record] = {}
        for series in series_list:
            series_id = series.get("id")
            for season in series.get("seasons", []):
                season_number = season.get("seasonNumber")
                if series_id is None or season_number is None:
                    continue
                stats = season.get("statistics", {})
                result[(series_id, season_number)] = {
                    "next_airing": stats.get("nextAiring"),
                    "monitored_count": stats.get("episodeCount", 0),
                }
        return result

    def _is_season_still_airing(
        self,
        series_id: int,
        season_number: int,
        season_metadata: dict[tuple[int, int], Record],
    ) -> bool:
        next_airing = season_metadata.get((series_id, season_number), {}).get("next_airing")
        return bool(next_airing and not self._is_date_past(next_airing))

    def _meets_season_pack_threshold(
        self,
        series_id: int,
        season_number: int,
        season_record_counts: dict[tuple[int, int], int],
        season_metadata: dict[tuple[int, int], Record],
    ) -> bool:
        key = (series_id, season_number)
        rec_count = season_record_counts.get(key, 0)
        if isinstance(self.season_packs, bool):
            return self.season_packs
        if isinstance(self.season_packs, int):
            return rec_count >= self.season_packs
        monitored = cast(int, season_metadata.get(key, {}).get("monitored_count", 0))
        return monitored > 0 and (rec_count / monitored) >= self.season_packs

    def _tally_season_records(self, records: list[Record]) -> dict[tuple[int, int], int]:
        counts: dict[tuple[int, int], int] = {}
        for record in records:
            series_id = self._get_series_id(record)
            season_number = self._get_season_number(record)
            if series_id is None or season_number is None:
                continue
            key = (series_id, season_number)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _get_season_title(self, record: Record, season_number: int) -> str:
        series_title = record.get("series", {}).get("title", "Unknown Series")
        return f"{series_title} - Season {season_number:02d}"

    def _collect_season_pack_records(
        self,
        records: list[Record],
        batch_size: int,
        reason: str,
        seen_seasons: set[tuple[int, int]],
        check_availability: bool,
        season_metadata: dict[tuple[int, int], Record],
        season_record_counts: dict[tuple[int, int], int],
    ) -> list[MediaItem]:
        items: list[MediaItem] = []
        for record in records:
            if 0 < batch_size <= len(items):
                break
            if self._is_tag_filtered_out(record):
                continue
            if check_availability and not self._is_available(record):
                continue
            if self._is_within_retry_window(record, reason):
                continue
            series_id = self._get_series_id(record)
            season_number = self._get_season_number(record)
            if series_id is None or season_number is None:
                continue
            key = (series_id, season_number)
            if key in seen_seasons:
                continue
            if self._is_season_still_airing(series_id, season_number, season_metadata):
                items.append(self._extract_item(record, reason))
                continue
            if not self._meets_season_pack_threshold(series_id, season_number, season_record_counts, season_metadata):
                items.append(self._extract_item(record, reason))
                continue
            seen_seasons.add(key)
            title = self._get_season_title(record, season_number)
            items.append((f"{self.SEASON_ID_PREFIX}{series_id}:{season_number}", reason, title))
        return items

    def _trigger_single(self, item_id: int | str, reason: str, title: str, index: int, total: int) -> None:
        if isinstance(item_id, str) and item_id.startswith(self.SERIES_ID_PREFIX):
            series_id = int(item_id.removeprefix(self.SERIES_ID_PREFIX))
            if self.dry_run:
                logger.info(f"[{self.name}] [DRY RUN] Would search ({reason}): {title} ({index}/{total})")
            else:
                url = f"{self.url}{self.ENDPOINT_COMMAND}"
                payload = {"name": "SeriesSearch", "seriesId": series_id}
                try:
                    response = self._api_post_command(url, payload)
                    response.raise_for_status()
                    logger.info(f"[{self.name}] Searching ({reason}): {title} ({index}/{total})")
                except requests.RequestException as error:
                    logger.error(
                        f"[{self.name}] Failed to trigger SeriesSearch for {title} (Series ID: {series_id}): {error}"
                    )
            return
        if not (isinstance(item_id, str) and item_id.startswith(self.SEASON_ID_PREFIX)):
            super()._trigger_single(item_id, reason, title, index, total)
            return
        _, series_id_str, season_str = item_id.split(":")
        series_id = int(series_id_str)
        season_number = int(season_str)
        if self.dry_run:
            logger.info(f"[{self.name}] [DRY RUN] Would search ({reason}): {title} ({index}/{total})")
        else:
            url = f"{self.url}{self.ENDPOINT_COMMAND}"
            payload = {"name": "SeasonSearch", "seriesId": series_id, "seasonNumber": season_number}
            try:
                response = self._api_post_command(url, payload)
                response.raise_for_status()
                logger.info(f"[{self.name}] Searching ({reason}): {title} ({index}/{total})")
            except requests.RequestException as error:
                logger.error(
                    f"[{self.name}] Failed to trigger SeasonSearch for {title} "
                    f"(Series: {series_id}, Season: {season_number}): {error}"
                )

    def _series_has_missing(self, series: Record) -> bool:
        stats = series.get("statistics", {}) or {}
        return cast(int, stats.get("episodeCount", 0)) > cast(int, stats.get("episodeFileCount", 0))

    def _get_media_to_search_by_tag_limits(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        """Series-centric, per-tag-capped, rotating selection.

        Fetches the series list once (instead of paging the entire wanted/missing set),
        buckets monitored-with-missing series by configured tag, and selects up to N
        per tag — one SeriesSearch each, so Sonarr resolves season packs with
        per-episode fallback (series > season > episode) and a whole multi-season
        series counts as a single unit against the cap.

        A per-tag in-memory cursor advances each cycle so successive cycles walk
        through the tag's backlog rather than re-searching the same top-N. Series are
        de-duplicated within a cycle, so a series carrying two capped tags is searched
        once and the other tag fills its remaining cap with distinct series.
        """
        upgrade_items = self._get_media_to_search_by_series(0, upgrade_batch_size)
        if missing_batch_size == 0:
            return upgrade_items
        series_list = self._fetch_list(self.ENDPOINT_SERIES)
        buckets: dict[int, list[Record]] = {tag_id: [] for tag_id in self._tag_limit_ids}
        for series in series_list:
            if not series.get("monitored", False):
                continue
            if not self._series_has_missing(series):
                continue
            tags = set(series.get("tags") or [])
            if self._exclude_tag_ids and tags & self._exclude_tag_ids:
                continue
            for tag_id in tags & buckets.keys():
                buckets[tag_id].append(series)

        items: list[MediaItem] = []
        seen: set[int] = set()
        for tag_id, cap in self._tag_limit_ids.items():
            bucket = buckets.get(tag_id, [])
            if not bucket:
                self._tag_search_cursor[tag_id] = 0
                continue
            bucket.sort(key=lambda s: s.get("id", 0))
            start = self._tag_search_cursor.get(tag_id, 0) % len(bucket)
            picked = 0
            scanned = 0
            while picked < cap and scanned < len(bucket):
                series = bucket[(start + scanned) % len(bucket)]
                scanned += 1
                series_id = series.get("id")
                if series_id is None or series_id in seen:
                    continue
                seen.add(series_id)
                items.append(
                    (f"{self.SERIES_ID_PREFIX}{series_id}", "missing", series.get("title", f"Series {series_id}"))
                )
                picked += 1
            # Advance past everything scanned so the next cycle resumes after it.
            self._tag_search_cursor[tag_id] = (start + scanned) % len(bucket)
        if self.search_order == "random":
            random.shuffle(items)
        return self._interleave_items(items, upgrade_items)

    def _get_media_to_search_by_series(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        seen_series: set[int] = set()
        missing_items: list[MediaItem] = []
        upgrade_items: list[MediaItem] = []

        if missing_batch_size != 0:
            records = self._fetch_unlimited(self.ENDPOINT_WANTED_MISSING)
            self._sort_records_client_side(records)
            for record in records:
                if 0 < missing_batch_size <= len(missing_items):
                    break
                if self._is_tag_filtered_out(record):
                    continue
                if not self._is_available(record):
                    continue
                if self._is_within_retry_window(record, "missing"):
                    continue
                series_id = self._get_series_id(record)
                if series_id is None or series_id in seen_series:
                    continue
                seen_series.add(series_id)
                series_title = record.get("series", {}).get("title", "Unknown Series")
                missing_items.append((f"{self.SERIES_ID_PREFIX}{series_id}", "missing", series_title))

        if upgrade_batch_size != 0:
            records = self._fetch_unlimited(self.ENDPOINT_WANTED_CUTOFF)
            self._sort_records_client_side(records)
            for record in records:
                if 0 < upgrade_batch_size <= len(upgrade_items):
                    break
                if self._is_tag_filtered_out(record):
                    continue
                if self._is_within_retry_window(record, "upgrade"):
                    continue
                series_id = self._get_series_id(record)
                if series_id is None or series_id in seen_series:
                    continue
                seen_series.add(series_id)
                series_title = record.get("series", {}).get("title", "Unknown Series")
                upgrade_items.append((f"{self.SERIES_ID_PREFIX}{series_id}", "upgrade", series_title))

            upgrades_so_far = len(upgrade_items)
            remaining = max(0, upgrade_batch_size - upgrades_so_far) if upgrade_batch_size > 0 else upgrade_batch_size
            if remaining != 0:
                supplemental = self._get_custom_format_score_unmet_records()
                self._sort_records_client_side(supplemental)
                for record in supplemental:
                    if 0 < upgrade_batch_size <= len(upgrade_items):
                        break
                    if self._is_tag_filtered_out(record):
                        continue
                    if self._is_within_retry_window(record, "upgrade"):
                        continue
                    series_id = self._get_series_id(record)
                    if series_id is None or series_id in seen_series:
                        continue
                    seen_series.add(series_id)
                    series_title = record.get("series", {}).get("title", "Unknown Series")
                    upgrade_items.append((f"{self.SERIES_ID_PREFIX}{series_id}", "upgrade", series_title))

        merged = self._interleave_items(missing_items, upgrade_items)
        if self.search_order == "random":
            random.shuffle(merged)
        return merged

    def get_media_to_search(self, missing_batch_size: int, upgrade_batch_size: int) -> list[MediaItem]:
        if self._circuit_breaker_fetch > 0 and self.circuit_breaker.is_open(self.name):
            logger.warning(f"[{self.name}] Circuit breaker open — skipping fetch cycle.")
            return []
        try:
            if self._tag_limit_ids:
                result = self._get_media_to_search_by_tag_limits(missing_batch_size, upgrade_batch_size)
                self.circuit_breaker.record_success(self.name)
                return result
            if self.search_type == "series":
                result = self._get_media_to_search_by_series(missing_batch_size, upgrade_batch_size)
                self.circuit_breaker.record_success(self.name)
                return result
            if not self.season_packs:
                result = super().get_media_to_search(missing_batch_size, upgrade_batch_size)
                self.circuit_breaker.record_success(self.name)
                return result

            seen_seasons: set[tuple[int, int]] = set()
            season_metadata = self._fetch_season_metadata()
            missing_items: list[MediaItem] = []
            upgrade_items: list[MediaItem] = []

            if missing_batch_size != 0:
                missing_records = self._fetch_unlimited(self.ENDPOINT_WANTED_MISSING)
                self._sort_records_client_side(missing_records)
                missing_counts = self._tally_season_records(missing_records)
                missing_items = self._collect_season_pack_records(
                    missing_records,
                    missing_batch_size,
                    "missing",
                    seen_seasons,
                    True,
                    season_metadata,
                    missing_counts,
                )

            if upgrade_batch_size != 0:
                upgrade_records = self._fetch_unlimited(self.ENDPOINT_WANTED_CUTOFF)
                self._sort_records_client_side(upgrade_records)
                upgrade_counts = self._tally_season_records(upgrade_records)
                upgrade_items = self._collect_season_pack_records(
                    upgrade_records,
                    upgrade_batch_size,
                    "upgrade",
                    seen_seasons,
                    False,
                    season_metadata,
                    upgrade_counts,
                )
                upgrades_so_far = len(upgrade_items)
                remaining = (
                    max(0, upgrade_batch_size - upgrades_so_far) if upgrade_batch_size > 0 else upgrade_batch_size
                )
                if remaining != 0:
                    supplemental = self._get_custom_format_score_unmet_records()
                    self._sort_records_client_side(supplemental)
                    supplemental_counts = self._tally_season_records(supplemental)
                    upgrade_items += self._collect_season_pack_records(
                        supplemental,
                        remaining,
                        "upgrade",
                        seen_seasons,
                        False,
                        season_metadata,
                        supplemental_counts,
                    )

            merged = self._interleave_items(missing_items, upgrade_items)
            if self.search_order == "random":
                random.shuffle(merged)
            self.circuit_breaker.record_success(self.name)
            return merged
        except Exception:
            self.circuit_breaker.record_failure(self.name)
            raise

    def _fetch_episode_file_scores(self, series_id: int, cutoff_score: int) -> set[int]:
        episode_files = self._fetch_list(self.ENDPOINT_EPISODE_FILE, {"seriesId": series_id})
        return {ef["id"] for ef in episode_files if ef.get("customFormatScore", 0) < cutoff_score}

    def _get_custom_format_upgrade_records(self, profile_cutoffs: dict[int, int]) -> list[Record]:
        series_list = self._fetch_list(self.ENDPOINT_SERIES)
        result = []
        for series in series_list:
            if not series.get("monitored", False):
                continue
            cutoff_score = profile_cutoffs.get(series.get("qualityProfileId", -1), 0)
            if cutoff_score > 0:
                low_score_file_ids = self._fetch_episode_file_scores(series["id"], cutoff_score)
                if low_score_file_ids:
                    episodes = self._fetch_list(self.ENDPOINT_EPISODE, {"seriesId": series["id"], "hasFile": "true"})
                    for episode in episodes:
                        if not episode.get("monitored", False):
                            continue
                        if episode.get("episodeFileId", -1) in low_score_file_ids:
                            episode["series"] = series
                            result.append(episode)
        return result


_CLIENT_MAP: dict[str, type[ArrClient]] = {
    "lidarr": LidarrClient,
    "radarr": RadarrClient,
    "sonarr": SonarrClient,
}
