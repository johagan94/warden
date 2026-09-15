"""Validation logic for Warden configuration."""

import datetime
import re
from collections.abc import Callable, Sequence
from typing import Any, cast

VALID_ACTIONS = ("ignore", "remove", "retry", "blocklist")
VALID_ARR_TYPES = ("radarr", "sonarr", "lidarr")
VALID_SEARCH_ORDERS = (
    "alphabetical_ascending",
    "alphabetical_descending",
    "last_added_ascending",
    "last_added_descending",
    "last_searched_ascending",
    "last_searched_descending",
    "random",
    "release_date_ascending",
    "release_date_descending",
)
VALID_REMOVAL_ORDERS = ("age_ascending", "age_descending", "api_order")
VALID_SEARCH_TYPES = ("episode", "series", "album", "artist", "collection")

STALL_CATEGORIES = (
    "dangerous_file",
    "manual_import",
    "no_files",
    "no_upgrade",
    "stalled",
    "missing_items",
    "tba_title",
    "no_messages",
    "download_unavailable",
    "unknown",
)

_CATEGORY_MAP: dict[str, list[str]] = {
    "dangerous_file": ["potentially dangerous file extension"],
    "manual_import": [
        "import failed, path does not exist",
        "non-sample file detected",
        "not enough space",
        "sample file detected",
        "sample",
        "unable to parse file",
        "found matching movie via grab history",
        "release was matched to movie by id",
        "matched to movie by id",
        "unable to determine if file is a sample",
        "automatic import is not possible",
        "release title doesn't match series title",
        "release was matched to series by id",
        "matched to series by id",
        "single episode file contains all episodes",
        "single episode file contains",
        "matched to album by id",
        "track does not belong to album",
        "manual import required",
        "album match is not close enough",
        "couldn't find similar album",
        "failed to import track",
        "permissions error",
        "worst track match",
    ],
    "no_files": [
        "no audio files found",
        "no files found are eligible for import",
        "no video files found",
        "invalid season or episode",
    ],
    "no_upgrade": [
        "already meets cutoff",
        "custom format upgrade",
        "do not improve on existing",
        "not a custom format upgrade",
        "not an upgrade for existing",
    ],
    "stalled": [
        "is locked by another process",
        "qbittorrent is downloading metadata",
        "the download is stalled with no",
    ],
    "missing_items": [
        "not imported or missing from the release",
        "not found in the grabbed release",
        "has unmatched tracks",
        "has fewer tracks than existing release",
        "has missing tracks",
    ],
    "tba_title": ["tba title"],
}


def _validate_active_hours(value: str) -> None:
    """Validate the active_hours setting format and component ranges."""
    if not value:
        return
    if not re.match(r"^\d{2}:\d{2}-\d{2}:\d{2}$", value):
        raise ValueError(f"'active_hours' must be in HH:MM-HH:MM format (e.g. '22:00-06:00'), got '{value}'.")
    start_str, end_str = value.split("-")
    for part, label in ((start_str, "start"), (end_str, "end")):
        try:
            _parse_hhmm(part)
        except ValueError as exc:
            raise ValueError(f"'active_hours' {label} time '{part}' is not a valid 24-hour time.") from exc
    if start_str == end_str:
        raise ValueError("'active_hours' start and end times must differ.")


def _parse_hhmm(token: str) -> datetime.time:
    """Parse an HH:MM token into a datetime.time object."""
    return datetime.time.fromisoformat(token)


def _validate_season_packs(setting: str, value: Any) -> None:
    """Validate the season_packs setting."""
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (value <= 0 or value >= 1):
            raise ValueError(f"'{setting}' as a float ratio must be between 0 and 1 (exclusive), got {value}.")
        if isinstance(value, int) and value < 1:
            raise ValueError(f"'{setting}' as an integer threshold must be >= 1, got {value}.")
        return
    raise ValueError(
        f"'{setting}' must be a boolean, a positive integer, or a float ratio (0-1), got {type(value).__name__}."
    )


def _validate_tag_limits(setting: str, value: Any) -> None:
    """Validate the tag_limits setting: a mapping of tag-name -> positive integer cap."""
    if not isinstance(value, dict):
        raise ValueError(f"'{setting}' must be a mapping of tag-name to an integer limit.")
    for label, limit in value.items():
        if not isinstance(label, str) or not label:
            raise ValueError(f"'{setting}' keys must be non-empty tag-name strings.")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError(f"'{setting}.{label}' must be an integer >= 1, got {limit!r}.")


def _validate_setting(
    setting: str,
    value: Any,
    expected_type: type,
    choices: Sequence[Any] | None = None,
    allow_special_values: bool = False,
    min_value: int | None = None,
    prefix: str = "global",
    element_type: type | None = None,
    validator: Callable[[Any], None] | None = None,
) -> None:
    """Validate a single setting value against its schema definition."""
    if not isinstance(value, expected_type):
        raise ValueError(f"'{prefix}.{setting}' must be of type {expected_type.__name__}.")

    if expected_type is int:
        int_value = cast(int, value)
        if min_value is not None and int_value < min_value:
            raise ValueError(f"'{prefix}.{setting}' must be at least {min_value}.")
        if min_value is None:
            limit = -1 if allow_special_values else 0
            if int_value < limit:
                msg = (
                    f"'{prefix}.{setting}' must be 0 (disabled), -1 (unlimited), or a positive integer."
                    if allow_special_values
                    else f"'{prefix}.{setting}' must be a non-negative integer."
                )
                raise ValueError(msg)

    if expected_type is list and element_type is not None:
        list_value = cast(list[Any], value)
        for element in list_value:
            if not isinstance(element, element_type):
                raise ValueError(f"'{prefix}.{setting}' must be a list of {element_type.__name__} values.")
            if element_type is str and not element:
                raise ValueError(f"'{prefix}.{setting}' entries must not be empty strings.")

    if choices is not None and value not in choices:
        valid_choices = ", ".join(repr(choice) for choice in choices)
        raise ValueError(f"'{prefix}.{setting}' must be one of: {valid_choices}.")

    if validator is not None:
        validator(value)


def classify_stall(messages: list[str]) -> str:
    """Classify *arr status messages into a stall category."""
    if not messages:
        return "no_messages"

    combined = " ".join(messages).lower()
    for category in ("dangerous_file", "missing_items"):
        if any(pattern in combined for pattern in _CATEGORY_MAP[category]):
            return category
    return next(
        (cat for cat, patterns in _CATEGORY_MAP.items() if any(pattern in combined for pattern in patterns)),
        "unknown",
    )
