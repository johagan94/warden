"""Defence cleanup orchestration: stalled download detection and removal across *arr instances."""

import datetime
import logging
import time
from typing import Any

from warden.config import get_setting_default, parse_active_hours
from warden.validators import STALL_CATEGORIES

logger = logging.getLogger(__name__)

CleanupBacklogs = dict[Any, list[Any]]
CleanupQueue = list[tuple[Any, Any]]


def _get_setting(settings: dict[str, Any], key: str) -> Any:
    return settings.get(key, get_setting_default(key))


def _calculate_eta(item_count: int, stagger_seconds: int) -> str:
    if stagger_seconds > 0 and item_count > 0:
        eta = datetime.timedelta(seconds=item_count * stagger_seconds)
        return f", 1 every {stagger_seconds}s, ETA: {eta}"
    return ""


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
    return int((start_dt - now_dt).total_seconds())


def _format_cycle_info(client_name: str, item_count: int, skip_stats: dict[str, int]) -> str:
    total_eval = skip_stats["total_evaluated"]
    skipped = skip_stats["ignored"] + skip_stats["tag_filtered"] + skip_stats.get("retry_interval", 0)
    protected = skip_stats.get("series_protected", 0)
    protected_str = f", SeriesProtected: {protected}" if protected else ""
    return f"[{client_name}] Found {item_count} items to remove (Evaluated: {total_eval}, Skipped: {skipped}{protected_str})."


def _allocate_slots(limit: int, client_backlogs: CleanupBacklogs) -> CleanupQueue:
    if limit == 0 or not client_backlogs:
        return []
    winners: CleanupQueue = []
    pools = {client: list(items) for client, items in client_backlogs.items() if items}
    sorted_clients = sorted(
        pools.keys(),
        key=lambda clt: (-getattr(clt, "weight", 1.0), getattr(clt, "name", "")),
    )
    while pools and (limit == -1 or len(winners) < limit):
        for client in list(sorted_clients):
            if client not in pools:
                continue
            turns = max(1, int(getattr(client, "weight", 1.0)))
            for _turn in range(turns):
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


def _apply_removal_order(client_backlogs: CleanupBacklogs, removal_order: str) -> None:
    if removal_order != "api_order":
        reverse = removal_order == "age_descending"
        for items in client_backlogs.values():
            items.sort(key=lambda item: item.added or "9999", reverse=reverse)


def _log_cleaner_start(active_clients: list[Any], settings: dict[str, Any]) -> None:
    batch = _get_setting(settings, "batch_size")
    batch_str = {0: "Disabled", -1: "Unlimited"}.get(batch, str(batch))
    stagger = _get_setting(settings, "stagger_interval_seconds")
    dry_run = _get_setting(settings, "dry_run")
    dry_run_str = " (DRY RUN ENABLED)" if dry_run else ""
    active_hours = _get_setting(settings, "active_hours")
    active_hours_str = active_hours if active_hours else "All hours"
    retry_minutes = _get_setting(settings, "retry_interval_minutes")
    retry_str = f"{retry_minutes}m" if retry_minutes > 0 else "off"
    interleave = _get_setting(settings, "interleave_instances")
    interleave_str = "Yes" if interleave else "No"
    stall_actions = {
        category: settings.get(category, settings.get("stalled", "ignore")) for category in STALL_CATEGORIES
    }
    action_str = ", ".join(f"{category}={action}" for category, action in stall_actions.items())

    logger.info(
        f"Warden-Defence started{dry_run_str} | "
        f"Instances: {len(active_clients)} active | "
        f"Run Interval: {_get_setting(settings, 'interval')}s | "
        f"Batch: {batch_str} | "
        f"Stagger: {stagger}s | "
        f"Active Hours: {active_hours_str} | "
        f"Retry Interval: {retry_str} | "
        f"Interleave: {interleave_str} | "
        f"Handling: {action_str}"
    )


def run_removal_cycle(active_clients: list[Any], settings: dict[str, Any]) -> None:
    logger.info("--- Starting removal cycle ---")
    batch_size: int = _get_setting(settings, "batch_size")
    max_per_instance: int = _get_setting(settings, "max_removals_per_instance")
    interleave: bool = _get_setting(settings, "interleave_instances")
    removal_order: str = _get_setting(settings, "removal_order")
    stagger: int = _get_setting(settings, "stagger_interval_seconds")

    backlogs: CleanupBacklogs = {}
    for client in active_clients:
        items, skip_stats = client.get_stalled_items()
        if not items:
            logger.info(
                f"[{client.name}] No stalled items found this cycle (Evaluated: {skip_stats['total_evaluated']})."
            )
        else:
            logger.info(_format_cycle_info(client.name, len(items), skip_stats))
            client_cap = getattr(client, "cleanup_settings", {}).get("max_removals_per_instance", max_per_instance)
            backlogs[client] = items[:client_cap] if client_cap > 0 else items

    if not backlogs or batch_size == 0:
        return

    _apply_removal_order(backlogs, removal_order)
    queue = _allocate_slots(batch_size, backlogs)

    if not interleave:
        per_client: CleanupBacklogs = {}
        for client, item in queue:
            per_client.setdefault(client, []).append(item)
        queue = [(c, item) for c in active_clients for item in per_client.get(c, [])]

    total = len(queue)
    logger.info(f"Removing {total} item(s){_calculate_eta(total, stagger)}.")

    cycle_start = time.monotonic()
    removed_attempts = 0
    failed = 0

    for index, (client, item) in enumerate(queue, start=1):
        try:
            client.execute_removal(item, index, total)
            removed_attempts += 1
        except Exception:
            failed += 1
            logger.exception(f"[{client.name}] Cleanup failed for {item.title} ({index}/{total}).")
        if stagger > 0 and index < total:
            time.sleep(stagger)

    duration = time.monotonic() - cycle_start
    logger.info(f"Cleanup cycle summary: attempted={removed_attempts}, failed={failed}, duration={duration:.2f}s.")


def run_cleaner_loop(active_clients: list[Any], settings: dict[str, Any], shutdown_event: Any = None) -> None:
    _log_cleaner_start(active_clients, settings)

    run_interval_seconds = _get_setting(settings, "interval")
    active_hours = _get_setting(settings, "active_hours")
    parsed_window = parse_active_hours(active_hours) if active_hours else None

    while True:
        if shutdown_event and shutdown_event.is_set():
            logger.info("Shutdown requested, exiting cleaner loop.")
            break
        if parsed_window:
            start_time, end_time = parsed_window
            now = datetime.datetime.now().time()
            if not _is_within_active_hours(start_time, end_time, now):
                secs = _seconds_until_window_open(start_time, now)
                logger.info(f"Outside active hours ({active_hours}). Sleeping {secs}s until window opens.")
                if shutdown_event:
                    shutdown_event.wait(timeout=secs)
                else:
                    time.sleep(secs)
                continue
        try:
            run_removal_cycle(active_clients, settings)
        except Exception:
            logger.exception("Cleanup cycle failed unexpectedly; continuing after sleep.")
        if shutdown_event:
            if shutdown_event.wait(timeout=run_interval_seconds):
                logger.info("Shutdown requested, exiting cleaner loop.")
                break
        else:
            time.sleep(run_interval_seconds)
