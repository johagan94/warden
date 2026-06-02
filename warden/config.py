"""Unified configuration loader and validator for Warden."""

import datetime
import logging
import os
import re
from collections.abc import Callable, Sequence
from typing import Any, NotRequired, TypedDict, cast

import yaml

from warden.validators import (
    STALL_CATEGORIES,
    VALID_ACTIONS,
    VALID_ARR_TYPES,
    VALID_REMOVAL_ORDERS,
    VALID_SEARCH_ORDERS,
    VALID_SEARCH_TYPES,
    _parse_hhmm,
    _validate_active_hours,
    _validate_season_packs,
    _validate_setting,
    _validate_tag_limits,
)

logger = logging.getLogger(__name__)

REQUIRED_TOP_LEVEL = ("instances",)


class SettingSchema(TypedDict):
    default: Any
    type: NotRequired[type]
    choices: NotRequired[Sequence[Any]]
    allow_special_values: NotRequired[bool]
    min_value: NotRequired[int]
    element_type: NotRequired[type]
    validator: NotRequired[Callable[[Any], None]]
    custom_validator: NotRequired[Callable[[str, Any], None]]


ConfigMap = dict[str, Any]
SchemaMap = dict[str, SettingSchema]


SEARCH_SETTINGS_SCHEMA: SchemaMap = {
    "active_hours": {"default": "", "type": str, "validator": _validate_active_hours},
    "api_request_interval_seconds": {"default": 0, "type": int, "min_value": 0},
    "circuit_breaker_threshold": {"default": 0, "type": int, "min_value": 0},
    "dry_run": {"default": False, "type": bool},
    "exclude_tags": {"default": [], "type": list, "element_type": str},
    "fetch_page_size": {"default": 2000, "type": int, "min_value": 1},
    "fetch_record_limit": {"default": 0, "type": int, "min_value": 0},
    "fetch_timeout_seconds": {"default": 120, "type": int, "min_value": 5},
    "include_tags": {"default": [], "type": list, "element_type": str},
    "interleave_instances": {"default": False, "type": bool},
    "interleave_types": {"default": True, "type": bool},
    "max_queue_size": {"default": 0, "type": int, "min_value": 0},
    "missing_batch_size": {"default": 20, "type": int, "allow_special_values": True},
    "queue_check_timeout_seconds": {"default": 15, "type": int, "min_value": 1},
    "radarr_collection_search_mode": {"default": "off", "type": str, "choices": ("off", "detect", "force")},
    "retry_interval_days": {"default": 30, "type": int, "min_value": 0},
    "retry_interval_days_missing": {"default": None, "type": int, "min_value": 0},
    "retry_interval_days_upgrade": {"default": None, "type": int, "min_value": 0},
    "run_interval_minutes": {"default": 60, "type": int, "min_value": 1},
    "run_interval_minutes_missing": {"default": None, "type": int, "min_value": 1},
    "run_interval_minutes_upgrade": {"default": None, "type": int, "min_value": 1},
    "search_after_cleanup": {"default": True, "type": bool},
    "search_after_cleanup_actions": {"default": ["retry", "blocklist"], "type": list, "element_type": str},
    "search_order": {"default": "last_searched_ascending", "type": str, "choices": VALID_SEARCH_ORDERS},
    "search_jitter_seconds": {"default": 0, "type": int, "min_value": 0},
    "search_type": {"default": None, "type": str, "choices": VALID_SEARCH_TYPES},
    "season_packs": {"default": False, "custom_validator": _validate_season_packs},
    "stagger_interval_seconds": {"default": 30, "type": int, "min_value": 1},
    "tag_limits": {"default": {}, "custom_validator": _validate_tag_limits},
    "upgrade_batch_size": {"default": 10, "type": int, "allow_special_values": True},
}

CLEANUP_SETTINGS_SCHEMA: SchemaMap = {
    "active_hours": {"default": "", "type": str, "validator": _validate_active_hours},
    "batch_size": {"default": 10, "type": int, "allow_special_values": True},
    "circuit_breaker_threshold": {"default": 0, "type": int, "min_value": 0},
    "cleanup_page_size": {"default": 100, "type": int, "min_value": 1},
    "delete_timeout_seconds": {"default": 15, "type": int, "min_value": 5},
    "dry_run": {"default": False, "type": bool},
    "exclude_tags": {"default": [], "type": list, "element_type": str},
    "fetch_timeout_seconds": {"default": 30, "type": int, "min_value": 5},
    "include_tags": {"default": [], "type": list, "element_type": str},
    "interleave_instances": {"default": False, "type": bool},
    "interval": {"default": 3600, "type": int, "min_value": 1},
    "max_cleanup_queue_records": {"default": 0, "type": int, "min_value": 0},
    "max_removals_per_instance": {"default": 0, "type": int, "min_value": 0},
    "cleanup_search_scope": {"default": "episode", "type": str, "choices": ("episode", "season", "series")},
    "protect_downloading_series": {"default": False, "type": bool},
    "queue_max_age_hours": {"default": 0, "type": int, "min_value": 0},
    "removal_order": {"default": "api_order", "type": str, "choices": VALID_REMOVAL_ORDERS},
    "retry_interval_minutes": {"default": 0, "type": int, "min_value": 0},
    "search_after_cleanup": {"default": None, "type": bool},
    "stagger_interval_seconds": {"default": 5, "type": int, "min_value": 0},
}


def _apply_interval_conversions(settings: ConfigMap) -> None:
    if "interval" in settings and "run_interval_minutes" not in settings:
        if not isinstance(settings["interval"], int):
            raise ValueError("'global.interval' must be an integer.")
        if settings["interval"] < 60:
            raise ValueError("'global.interval' must be at least 60 seconds.")
        settings["run_interval_minutes"] = settings["interval"] // 60
    if "interval_missing" in settings:
        if not isinstance(settings["interval_missing"], int):
            raise ValueError("'global.interval_missing' must be an integer.")
        if settings["interval_missing"] < 60:
            raise ValueError("'global.interval_missing' must be at least 60 seconds.")
        settings["run_interval_minutes_missing"] = settings["interval_missing"] // 60
    if "interval_upgrade" in settings:
        if not isinstance(settings["interval_upgrade"], int):
            raise ValueError("'global.interval_upgrade' must be an integer.")
        if settings["interval_upgrade"] < 60:
            raise ValueError("'global.interval_upgrade' must be at least 60 seconds.")
        settings["run_interval_minutes_upgrade"] = settings["interval_upgrade"] // 60


def _expand_env_var(match: re.Match[str]) -> str:
    name = match.group(1)
    val = os.environ.get(name)
    if val is None:
        raise ValueError(f"Environment variable '{name}' referenced in config is not set.")
    return val


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {key: _expand_env_vars(val) for key, val in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    if isinstance(obj, str):
        expanded = re.sub(r"\$\{([^}]+)\}", _expand_env_var, obj)
        return _parse_env_value(expanded) if expanded != obj else expanded
    return obj


def _parse_env_value(value: str) -> Any:
    val_lower = value.lower()
    if val_lower == "true":
        return True
    if val_lower == "false":
        return False
    if re.match(r"^-?\d+$", value):
        return int(value)
    if re.match(r"^-?\d+\.\d+$", value):
        return float(value)
    return value


def _parse_instance(name: str, config: ConfigMap) -> tuple[str, ConfigMap] | None:
    instance = config.copy()
    if "host" in instance:
        instance["url"] = instance.pop("host")
    inst_type = str(instance.pop("type", None) or "").lower()
    if not inst_type:
        raise ValueError(f"Missing 'type' field for instance '{name}'. Must be one of: {', '.join(VALID_ARR_TYPES)}.")
    if inst_type not in VALID_ARR_TYPES:
        raise ValueError(
            f"Invalid type '{inst_type}' for instance '{name}'. Must be one of: {', '.join(VALID_ARR_TYPES)}."
        )
    instance["name"] = name
    for field in ("url", "api_key"):
        if not instance.get(field):
            raise ValueError(f"Missing or empty '{field}' for instance '{name}'.")
    instance.setdefault("weight", 1)
    if not isinstance(instance["weight"], (int, float)) or instance["weight"] <= 0:
        raise ValueError(f"'weight' for instance '{name}' must be a positive number.")

    if isinstance(instance.get("search_type"), str):
        instance["search_type"] = instance["search_type"].lower()
    search_type = instance.get("search_type", "")
    if search_type and search_type not in VALID_SEARCH_TYPES:
        raise ValueError(f"Invalid search_type '{search_type}' for instance '{name}'.")

    if not instance.get("enabled", False):
        return None
    return (inst_type, instance)


def _validate_action_list(setting: str, value: list[str], prefix: str) -> None:
    valid_actions = ", ".join(repr(action) for action in VALID_ACTIONS)
    for action in value:
        if action not in VALID_ACTIONS:
            raise ValueError(f"'{prefix}.{setting}' entries must be one of: {valid_actions}.")


def _validate_schema_setting(setting: str, value: Any, definition: SettingSchema, prefix: str) -> None:
    if definition["default"] is None and value is None:
        return
    if "custom_validator" in definition:
        definition["custom_validator"](setting, value)
        return
    _validate_setting(
        setting,
        value,
        definition["type"],
        definition.get("choices"),
        allow_special_values=definition.get("allow_special_values", False),
        min_value=definition.get("min_value"),
        element_type=definition.get("element_type"),
        validator=definition.get("validator"),
        prefix=prefix,
    )
    if setting == "search_after_cleanup_actions":
        _validate_action_list(setting, value, prefix)


def _copy_default(default: Any) -> Any:
    if isinstance(default, list):
        return list(default)
    if isinstance(default, dict):
        return dict(default)
    return default


def _validate_search_settings(settings: ConfigMap, schema: SchemaMap) -> None:
    for setting, definition in schema.items():
        settings.setdefault(setting, _copy_default(definition["default"]))
        _validate_schema_setting(setting, settings[setting], definition, "global")


def _validate_cleanup_settings(settings: ConfigMap, schema: SchemaMap) -> None:
    for setting, definition in schema.items():
        settings.setdefault(setting, _copy_default(definition["default"]))
        _validate_schema_setting(setting, settings[setting], definition, "cleanup")


def _validate_stall_actions(settings: ConfigMap) -> None:
    for category in STALL_CATEGORIES:
        if category in settings:
            _validate_setting(category, settings[category], str, choices=VALID_ACTIONS)


def _validate_instance_overrides(name: str, instance: ConfigMap) -> None:
    seen: set[str] = set()
    for schema in (SEARCH_SETTINGS_SCHEMA, CLEANUP_SETTINGS_SCHEMA):
        for setting, definition in schema.items():
            if setting in instance and setting not in seen:
                _validate_schema_setting(setting, instance[setting], definition, f"instances.{name}")
                seen.add(setting)
    for category in STALL_CATEGORIES:
        if category in instance:
            _validate_setting(category, instance[category], str, choices=VALID_ACTIONS, prefix=f"instances.{name}")


def get_setting_default(setting: str) -> Any:
    for schema in (SEARCH_SETTINGS_SCHEMA, CLEANUP_SETTINGS_SCHEMA):
        if setting in schema:
            return schema[setting]["default"]
    raise KeyError(f"Unknown setting: {setting}")


def load_config(path: str) -> ConfigMap:
    with open(path, encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if config is None:
        config = {}
    expanded = _expand_env_vars(config)
    return parse_config(expanded)


def parse_active_hours(value: str) -> tuple[datetime.time, datetime.time]:
    start_str, end_str = value.split("-")
    return _parse_hhmm(start_str), _parse_hhmm(end_str)


def parse_config(config: ConfigMap) -> ConfigMap:
    if not isinstance(config, dict):
        raise ValueError("Configuration file must be a YAML mapping at the top level.")

    for key in REQUIRED_TOP_LEVEL:
        if key not in config:
            raise ValueError(f"Missing required top-level key: '{key}'")

    # Section name: "vigilance" matches the WARDEN_MODE name for the search
    # loop; "global" is kept as a backward-compat alias.
    raw_search_settings = config.get("vigilance", config.get("global", {}))
    if not isinstance(raw_search_settings, dict):
        raise ValueError("'vigilance' (or legacy 'global') must be a YAML mapping.")
    search_settings = cast(ConfigMap, dict(raw_search_settings))
    _apply_interval_conversions(search_settings)
    _validate_search_settings(search_settings, SEARCH_SETTINGS_SCHEMA)

    # Section name: "defence" matches the WARDEN_MODE name for the cleanup
    # loop; "cleanup" is kept as a backward-compat alias.
    raw_cleanup_settings = config.get("defence", config.get("cleanup", config.get("killarr", {})))
    if not isinstance(raw_cleanup_settings, dict):
        raise ValueError("'defence' (or legacy 'cleanup') must be a YAML mapping.")
    cleanup_settings = cast(ConfigMap, dict(raw_cleanup_settings))
    _validate_cleanup_settings(cleanup_settings, CLEANUP_SETTINGS_SCHEMA)
    _validate_stall_actions(cleanup_settings)

    raw_instances = config.get("instances", {})
    if not isinstance(raw_instances, dict):
        raise ValueError("'instances' must be a YAML mapping.")

    final_instances: dict[str, list[ConfigMap]] = {"radarr": [], "sonarr": [], "lidarr": []}
    all_empty = True

    for instance_name, instance_config in raw_instances.items():
        if not isinstance(instance_config, dict):
            raise ValueError(f"Instance '{instance_name}' must be a YAML mapping.")
        parsed = _parse_instance(instance_name, instance_config)
        if parsed is not None:
            inst_type, inst = parsed
            _validate_instance_overrides(instance_name, inst)
            all_empty = False
            final_instances[inst_type].append(inst)

    if all_empty:
        raise ValueError("No instances defined under 'instances'. Add at least one Radarr, Sonarr, or Lidarr instance.")

    return {
        "search_settings": search_settings,
        "cleanup_settings": cleanup_settings,
        "instances": final_instances,
    }
