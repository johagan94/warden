from __future__ import annotations

import datetime
import logging
import math
import time
from typing import Any, cast

from warden.clients.arr import ArrClient
from warden.config import get_setting_default, parse_active_hours

logger = logging.getLogger(__name__)

MediaItem = tuple[int | str, str, str]

_SEARCH_ORDER_LABELS: dict[str, str] = {
    "alphabetical_ascending": "Alphabetical (Ascending)",
    "alphabetical_descending": "Alphabetical (Descending)",
    "last_added_ascending": "Last Added (Ascending)",
    "last_added_descending": "Last Added (Descending)",
    "last_searched_ascending": "Last Searched (Ascending)",
    "last_searched_descending": "Last Searched (Descending)",
    "random": "Random",
    "release_date_ascending": "Release Date (Ascending)",
    "release_date_descending": "Release Date (Descending)",
}

_MIN_SLEEP_SECONDS: float = 1.0


def _get_setting(settings: dict[str, Any], key: str) -> Any:
    return settings.get(key, get_setting_default(key))


def _batch_display_str(batch: int) -> str:
    return {0: "Disabled", -1: "Unlimited"}.get(batch, str(batch))


def _format_retry_interval_str(retry_days: int, retry_missing: int | None, retry_upgrade: int | None) -> str:
    global_retry_str = "Disabled" if retry_days == 0 else f"{retry_days} Days"
    if retry_missing is not None or retry_upgrade is not None:
        missing_retry_str = (
            ("Disabled" if retry_missing == 0 else f"{retry_missing} Days")
            if retry_missing is not None
            else global_retry_str
        )
        upgrade_retry_str = (
            ("Disabled" if retry_upgrade == 0 else f"{retry_upgrade} Days")
            if retry_upgrade is not None
            else global_retry_str
        )
        return f"Global: {global_retry_str}, Missing: {missing_retry_str}, Upgrade: {upgrade_retry_str}"
    return global_retry_str


def _format_run_interval_str(
    run_interval_m: int,
    run_interval_missing_m: int | None,
    run_interval_upgrade_m: int | None,
) -> str:
    if run_interval_missing_m is not None or run_interval_upgrade_m is not None:
        eff_missing_m = run_interval_missing_m if run_interval_missing_m is not None else run_interval_m
        eff_upgrade_m = run_interval_upgrade_m if run_interval_upgrade_m is not None else run_interval_m
        return f"{run_interval_m}m (Missing: {eff_missing_m}m, Upgrade: {eff_upgrade_m}m)"
    return f"{run_interval_m}m"


def _calculate_eta(item_count: int, stagger_seconds: int) -> str:
    if stagger_seconds <= 0 or item_count <= 1:
        return ""
    eta = datetime.timedelta(seconds=(item_count - 1) * stagger_seconds)
    return f" (1 every {stagger_seconds} seconds, ETA: {eta})"


def _is_within_active_hours(start: datetime.time, end: datetime.time, now: datetime.time) -> bool:
    if start <= end:
        return start <= now < end
    return now >= start or now < end


def _seconds_until_window_open(start: datetime.time, now: datetime.time, today: datetime.date | None = None) -> int:
    date = today if today is not None else datetime.date.today()
    start_dt = datetime.datetime.combine(date, start)
    now_dt = datetime.datetime.combine(date, now)
    if start_dt <= now_dt:
        start_dt += datetime.timedelta(days=1)
    return math.ceil((start_dt - now_dt).total_seconds())


def _allocate_slots(
    limit: int,
    client_backlogs: dict[Any, list[MediaItem]],
) -> list[tuple[Any, MediaItem]]:
    winners: list[tuple[Any, MediaItem]] = []
    pools = {client: list(items) for client, items in client_backlogs.items() if items}
    if limit == 0 or not pools:
        return []
    sorted_clients = sorted(
        pools.keys(),
        key=lambda clt: (-getattr(clt, "weight", 1.0), getattr(clt, "name", "")),
    )
    while pools and (limit == -1 or len(winners) < limit):
        for client in list(sorted_clients):
            if client not in pools:
                continue
            weight = getattr(client, "weight", 1.0)
            turns = max(1, int(weight))
            for _ in range(turns):
                if limit != -1 and len(winners) >= limit:
                    break
                if pools[client]:
                    winners.append((client, pools[client].pop(0)))
                if not pools[client]:
                    del pools[client]
                    break
            if limit != -1 and len(winners) >= limit:
                break
    return winners


def _clients_in_allocation_order(
    allocated_missing: list[tuple[Any, MediaItem]],
    allocated_upgrade: list[tuple[Any, MediaItem]],
) -> list[Any]:
    seen: set[Any] = set()
    clients: list[Any] = []
    for client, _ in allocated_missing + allocated_upgrade:
        if client not in seen:
            clients.append(client)
            seen.add(client)
    return clients


def _build_final_queue(
    allocated_missing: list[tuple[Any, MediaItem]],
    allocated_upgrade: list[tuple[Any, MediaItem]],
    interleave_instances: bool,
    interleave_types: bool,
) -> list[tuple[Any, MediaItem]]:
    final_queue: list[tuple[Any, MediaItem]] = []
    if interleave_types and interleave_instances:
        for idx in range(max(len(allocated_missing), len(allocated_upgrade))):
            if idx < len(allocated_missing):
                final_queue.append(allocated_missing[idx])
            if idx < len(allocated_upgrade):
                final_queue.append(allocated_upgrade[idx])
    elif interleave_types:
        for client in _clients_in_allocation_order(allocated_missing, allocated_upgrade):
            cli_missing = [(clt, item) for clt, item in allocated_missing if clt is client]
            cli_upgrade = [(clt, item) for clt, item in allocated_upgrade if clt is client]
            for idx in range(max(len(cli_missing), len(cli_upgrade))):
                if idx < len(cli_missing):
                    final_queue.append(cli_missing[idx])
                if idx < len(cli_upgrade):
                    final_queue.append(cli_upgrade[idx])
    elif interleave_instances:
        final_queue = allocated_missing + allocated_upgrade
    else:
        for client in _clients_in_allocation_order(allocated_missing, allocated_upgrade):
            cli_missing = [(clt, item) for clt, item in allocated_missing if clt is client]
            cli_upgrade = [(clt, item) for clt, item in allocated_upgrade if clt is client]
            final_queue.extend(cli_missing)
            final_queue.extend(cli_upgrade)
    return final_queue


def _format_cycle_complete_log(
    ran_missing: bool,
    ran_upgrade: bool,
    next_missing_secs: float,
    next_upgrade_secs: float,
) -> str:
    types = []
    if ran_missing:
        types.append("missing")
    if ran_upgrade:
        types.append("upgrade")
    ran_str = ", ".join(types)
    next_missing_m = max(0, math.ceil(next_missing_secs / 60))
    next_upgrade_m = max(0, math.ceil(next_upgrade_secs / 60))
    return (
        f"--- Search cycle complete ({ran_str}). Next: missing in {next_missing_m}m, upgrade in {next_upgrade_m}m. ---"
    )


def _resolve_interval_secs(settings: dict[str, Any], specific_key: str) -> float:
    override = _get_setting(settings, specific_key)
    resolved_minutes = override if override is not None else _get_setting(settings, "run_interval_minutes")
    return cast(int, resolved_minutes) * 60


def _log_searcher_start(active_clients: list[ArrClient], settings: dict[str, Any]) -> None:
    global_missing = _get_setting(settings, "missing_batch_size")
    global_upgrade = _get_setting(settings, "upgrade_batch_size")
    stagger_seconds = _get_setting(settings, "stagger_interval_seconds")
    dry_run = _get_setting(settings, "dry_run")
    active_hours = _get_setting(settings, "active_hours")
    interleave_instances = _get_setting(settings, "interleave_instances")
    interleave_types = _get_setting(settings, "interleave_types")

    retry_str = _format_retry_interval_str(
        _get_setting(settings, "retry_interval_days"),
        _get_setting(settings, "retry_interval_days_missing"),
        _get_setting(settings, "retry_interval_days_upgrade"),
    )
    raw_order = _get_setting(settings, "search_order")
    search_order_str = _SEARCH_ORDER_LABELS.get(raw_order, raw_order.capitalize())
    dry_run_str = " (DRY RUN ENABLED)" if dry_run else ""
    active_hours_str = active_hours if active_hours else "All hours"
    interleave_instances_str = "Yes" if interleave_instances else "No"
    interleave_types_str = "Yes" if interleave_types else "No"
    interval_str = _format_run_interval_str(
        _get_setting(settings, "run_interval_minutes"),
        _get_setting(settings, "run_interval_minutes_missing"),
        _get_setting(settings, "run_interval_minutes_upgrade"),
    )

    logger.info(
        f"Warden-Search started{dry_run_str} | "
        f"Instances: {len(active_clients)} active | "
        f"Run Interval: {interval_str} | "
        f"Missing Batch: {_batch_display_str(global_missing)} | "
        f"Upgrade Batch: {_batch_display_str(global_upgrade)} | "
        f"Search Stagger: {stagger_seconds} Seconds | "
        f"Search Order: {search_order_str} | "
        f"Retry Interval: {retry_str} | "
        f"Active Hours: {active_hours_str} | "
        f"Interleave Instances: {interleave_instances_str} | "
        f"Interleave Types: {interleave_types_str}"
    )


def run_search_cycle(
    active_clients: list[ArrClient],
    settings: dict[str, Any],
    *,
    run_missing: bool = True,
    run_upgrade: bool = True,
) -> None:
    logger.info("--- Starting search cycle ---")
    global_missing = _get_setting(settings, "missing_batch_size") if run_missing else 0
    global_upgrade = _get_setting(settings, "upgrade_batch_size") if run_upgrade else 0
    stagger_seconds = _get_setting(settings, "stagger_interval_seconds")
    interleave_instances = _get_setting(settings, "interleave_instances")
    interleave_types = _get_setting(settings, "interleave_types")

    missing_pools: dict[ArrClient, list[MediaItem]] = {}
    upgrade_pools: dict[ArrClient, list[MediaItem]] = {}
    tag_limited_queue: list[tuple[ArrClient, MediaItem]] = []
    failed_clients = 0

    for client in active_clients:
        try:
            if client.is_queue_too_large():
                continue
            client_settings = getattr(client, "search_settings", {})
            client_missing = client_settings.get("missing_batch_size", global_missing) if run_missing else 0
            client_upgrade = client_settings.get("upgrade_batch_size", global_upgrade) if run_upgrade else 0
            candidates = client.get_media_to_search(client_missing, client_upgrade)
        except Exception:
            failed_clients += 1
            logger.exception(f"[{client.name}] Search candidate fetch failed; continuing with remaining instances.")
            continue
        if getattr(client, "uses_tag_limits", False):
            # Client self-bounds via per-tag limits; take all its items, bypassing the
            # shared cross-instance allocation so the per-tag caps stay authoritative.
            tag_limited_queue.extend((client, item) for item in candidates)
            continue
        m_items = [item for item in candidates if item[1] == "missing"]
        u_items = [item for item in candidates if item[1] == "upgrade"]
        if m_items:
            missing_pools[client] = m_items
        if u_items:
            upgrade_pools[client] = u_items

    allocated_missing = _allocate_slots(global_missing, missing_pools)
    allocated_upgrade = _allocate_slots(global_upgrade, upgrade_pools)
    final_queue = _build_final_queue(allocated_missing, allocated_upgrade, interleave_instances, interleave_types)
    final_queue = tag_limited_queue + final_queue

    if not final_queue:
        logger.info("No media to search this cycle across all instances.")
        return

    logger.info(f"Total search batch: {len(final_queue)} item(s){_calculate_eta(len(final_queue), stagger_seconds)}")

    cycle_start = time.monotonic()
    searched = 0

    for index, (client, item) in enumerate(final_queue, start=1):
        try:
            client.trigger_search([item], index=index, total=len(final_queue))
            searched += 1
        except Exception:
            logger.exception(f"[{client.name}] Search trigger failed for {item[2]} ({index}/{len(final_queue)}).")
        if stagger_seconds > 0 and index < len(final_queue):
            logger.debug(f"Staggering next search by {stagger_seconds}s.")
            time.sleep(stagger_seconds)

    duration = time.monotonic() - cycle_start
    logger.info(
        f"Search cycle summary: selected_missing={len(allocated_missing)}, selected_upgrade={len(allocated_upgrade)}, "
        f"searched={searched}, failed_clients={failed_clients}, duration={duration:.2f}s."
    )


def run_searcher_loop(active_clients: list[ArrClient], settings: dict[str, Any], shutdown_event: Any = None) -> None:
    _log_searcher_start(active_clients, settings)

    missing_interval_secs = _resolve_interval_secs(settings, "run_interval_minutes_missing")
    upgrade_interval_secs = _resolve_interval_secs(settings, "run_interval_minutes_upgrade")
    active_hours = _get_setting(settings, "active_hours")
    parsed_window = parse_active_hours(active_hours) if active_hours else None

    last_missing_run = -math.inf
    last_upgrade_run = -math.inf

    while True:
        if shutdown_event and shutdown_event.is_set():
            logger.info("Shutdown requested, exiting searcher loop.")
            break
        if parsed_window:
            start_time, end_time = parsed_window
            current_time = datetime.datetime.now().time()
            if not _is_within_active_hours(start_time, end_time, current_time):
                secs = _seconds_until_window_open(start_time, current_time)
                logger.info(f"Outside active hours ({active_hours}). Sleeping {secs}s until window opens.")
                if shutdown_event:
                    shutdown_event.wait(timeout=secs)
                else:
                    time.sleep(secs)
                continue

        monotonic_now = time.monotonic()
        run_missing = (monotonic_now - last_missing_run) >= missing_interval_secs
        run_upgrade = (monotonic_now - last_upgrade_run) >= upgrade_interval_secs

        if run_missing:
            last_missing_run = monotonic_now
        if run_upgrade:
            last_upgrade_run = monotonic_now

        try:
            run_search_cycle(active_clients, settings, run_missing=run_missing, run_upgrade=run_upgrade)
        except Exception:
            logger.exception("Search cycle failed unexpectedly; continuing after sleep.")

        monotonic_now = time.monotonic()
        logger.info(
            _format_cycle_complete_log(
                run_missing,
                run_upgrade,
                missing_interval_secs - (monotonic_now - last_missing_run),
                upgrade_interval_secs - (monotonic_now - last_upgrade_run),
            )
        )
        sleep_secs = max(
            _MIN_SLEEP_SECONDS,
            min(
                missing_interval_secs - (monotonic_now - last_missing_run),
                upgrade_interval_secs - (monotonic_now - last_upgrade_run),
            ),
        )
        if shutdown_event:
            if shutdown_event.wait(timeout=max(1, int(sleep_secs))):
                logger.info("Shutdown requested, exiting searcher loop.")
                break
        else:
            time.sleep(sleep_secs)
