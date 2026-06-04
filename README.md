# Warden

Automated media library management for *Arr ecosystems. Warden operates in two complementary modes:

- **Vigilance** — hunts for missing and upgrade-eligible media across your library
- **Defence** — clears stalled, broken, and problematic downloads from your queue

Run either mode independently, or combine both for full library automation.

## Quick Start

```bash
docker compose up -d
```

## Modes

| Mode | Env Value | Description |
|------|-----------|-------------|
| Warden | `WARDEN_MODE=warden` | Run both Vigilance and Defence (default) |
| Vigilance | `WARDEN_MODE=vigilance` | Search only — find and trigger missing/upgrades |
| Defence | `WARDEN_MODE=defence` | Cleanup only — remove stalled and broken downloads |

## Configuration

See `config.example.yaml` for a complete reference. Copy it to `config.yaml` and customize.

### Instances

Define your *Arr instances under `instances:`. Each instance shares the same configuration structure:

```yaml
instances:
  sonarr-instance:
    type: sonarr
    host: "http://<sonarr-host>:<port>"
    api_key: "${SONARR_API_KEY}"
    enabled: true
```

Supported types: `sonarr`, `radarr`, `lidarr`.

### Search Type

Control the granularity of search commands per instance:

| Type | `search_type` values | Default | API Command |
|------|---------------------|---------|-------------|
| Sonarr | `series`, `episode` | `episode` | `SeriesSearch` / `EpisodeSearch` |
| Lidarr | `artist`, `album` | `album` | `ArtistSearch` / `AlbumSearch` |
| Radarr | `movie`, `collection` | `movie` | `MoviesSearch` / `CollectionSearch` |

```yaml
instances:
  sonarr-instance:
    type: sonarr
    search_type: episode       # Keep SeriesSearch off; use season_packs for season-level searches
  lidarr-instance:
    type: lidarr
    search_type: artist        # Search entire artist instead of individual albums
```

**Note:** `series` search uses `SeriesSearch` with `seriesId`, which is more efficient than triggering individual episode searches.

### Collection Search (Radarr)

Radarr supports grouping movies by collection:

```yaml
global:
  radarr_collection_search_mode: "off"  # off | detect | force
```

- `off` — Standard movie-level search (default)
- `detect` — Group movies by collection when found in wanted endpoints
- `force` — Fetch from `/api/v3/collection` and search all monitored collections with missing movies

### Vigilance — Search Scheduling

Configure how Warden hunts for missing and upgrade media:

```yaml
global:
  dry_run: false                    # Log intended searches without sending commands
  active_hours: ""                  # Optional local-time window, e.g. "22:00-06:00"
  run_interval_minutes: 30          # How often to check for new items
  run_interval_minutes_missing:     # Optional missing-only interval in minutes
  run_interval_minutes_upgrade:     # Optional upgrade-only interval in minutes
  missing_batch_size: 25            # Items searched per cycle (0 = disabled, -1 = unlimited)
  upgrade_batch_size: 0             # Upgrade searches per cycle (0 = disabled)
  search_order: release_date_ascending
  stagger_interval_seconds: 10      # Delay between individual search commands
  retry_interval_days: 5            # Skip items searched within this window
  retry_interval_days_missing: 3    # Override for missing items only
  retry_interval_days_upgrade: 7    # Override for upgrade items only
  fetch_page_size: 2000             # Records per API request
  fetch_record_limit: 0             # Max wanted records read per endpoint per cycle (0 = unlimited)
  fetch_timeout_seconds: 120        # HTTP request timeout
  max_queue_size: 500               # Pause if queue >= this (0 = disabled)
  circuit_breaker_threshold: 3      # Skip instance after N consecutive failures
  interleave_instances: false       # Alternate between instances in search queue
  interleave_types: true            # Alternate between missing and upgrade searches
  search_after_cleanup: true        # Search for replacements after Defence removals
  search_after_cleanup_actions: [retry, blocklist]
  api_request_interval_seconds: 2   # Min delay between mutating API calls
  search_jitter_seconds: 3          # Random extra delay to avoid burst patterns
  include_tags: []                  # Optional *Arr tag labels to include
  exclude_tags: []                  # Optional *Arr tag labels to exclude
```

#### Season Packs (Sonarr)

Control when to search for entire seasons instead of individual episodes. For Sonarr, prefer `search_type: episode` with `season_packs` enabled; this lets Warden use `SeasonSearch` where it makes sense and fall back to `EpisodeSearch` for still-airing or partial seasons.

```yaml
instances:
  sonarr-instance:
    type: sonarr
    season_packs: true              # Always use season search
    # season_packs: 0.75           # Or: threshold ratio (75% of episodes missing)
    # season_packs: 5              # Or: minimum episode count
```

### Defence — Queue Cleanup

Configure how Warden defends your library from problematic downloads:

```yaml
killarr:
  dry_run: false                    # Log intended removals without deleting queue items
  active_hours: ""                  # Optional local-time window, e.g. "22:00-06:00"
  interval: 600                     # Run every 10 minutes
  batch_size: -1                    # Process all stalled items (-1 = unlimited, 0 = disabled)
  stagger_interval_seconds: 5       # Delay between removals
  circuit_breaker_threshold: 3      # Skip after N consecutive failures
  cleanup_page_size: 100            # Queue records per API request
  max_cleanup_queue_records: 0      # Cap total records fetched (0 = unlimited)
  max_removals_per_instance: 25     # Per-instance removal cap per cycle (0 = no cap)
  delete_timeout_seconds: 15        # Timeout for queue deletion calls
  retry_interval_minutes: 0         # Cooldown before re-evaluating removed items (0 = off)
  removal_order: api_order          # api_order | age_ascending | age_descending
  cleanup_search_scope: episode     # episode | season | series (what ID to search after removal)
  protect_downloading_series: false # Hold back stalled items from series with active downloads
  interleave_instances: false       # Alternate removals between instances
  search_after_cleanup:             # Optional override for global search_after_cleanup
  include_tags: []                  # Optional *Arr tag labels to include
  exclude_tags: []                  # Optional *Arr tag labels to exclude

  # Action per stall reason: ignore | remove | retry | blocklist
  dangerous_file: blocklist
  manual_import: blocklist
  no_files: blocklist
  no_upgrade: blocklist
  stalled: blocklist
  missing_items: blocklist
  tba_title: blocklist
  no_messages: blocklist
  unknown: blocklist
```

#### Stall Categories

| Category | Triggered By |
|----------|-------------|
| `dangerous_file` | Potentially dangerous file extension detected |
| `manual_import` | Import failures, sample conflicts, matching issues |
| `no_files` | No eligible video/audio files found |
| `no_upgrade` | Existing file already meets cutoff or is better |
| `stalled` | Download stuck (metadata, no peers, locked files) |
| `missing_items` | Files not found in grabbed release |
| `tba_title` | Title still "TBA" (unreleased) |
| `no_messages` | No status messages provided by *Arr |
| `unknown` | Unrecognized messages (please report) |

### Shared Settings

#### Active Hours

Restrict both modes to a specific local-time window. Set `TZ` in the container environment to control the timezone used by these checks:

```yaml
global:
  active_hours: "22:00-06:00"     # Only run between 10 PM and 6 AM local time
```

Leave empty or omit for all hours (default).

#### Circuit Breaker

Skip instances after consecutive failures:

```yaml
global:
  circuit_breaker_threshold: 3      # Vigilance fetch failures
killarr:
  circuit_breaker_threshold: 3      # Defence cleanup failures
```

#### Per-Instance Overrides

Any global or killarr setting can be overridden per instance:

```yaml
instances:
  lidarr-instance:
    type: lidarr
    max_removals_per_instance: 5     # Override Defence cap for this instance only
    manual_import: remove            # Override one cleanup stall category for this instance
    search_type: artist
```

## Docker Compose

```yaml
services:
  warden:
    image: ghcr.io/jackohagan94-afk/warden:latest
    container_name: warden
    restart: unless-stopped
    env_file:
      - .env
    environment:
      TZ: Australia/Brisbane
      LOG_LEVEL: INFO
      WARDEN_MODE: warden
    volumes:
      - ./config.yaml:/app/config.yaml:ro
    networks:
      - homelab
```

**Registry:** Images are published to GitHub Container Registry (`ghcr.io/jackohagan94-afk/warden`). For private deployments, use your own registry URL.

## Building

```bash
docker build -t warden:latest .
```

## License

MIT
