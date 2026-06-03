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

For Sonarr on large libraries, see [Per-Tag Search Limits](#per-tag-search-limits-sonarr) to cap searches per tag and rotate through the backlog.

```yaml
instances:
  sonarr-instance:
    type: sonarr
    search_type: episode       # Safest Sonarr default; pair with season_packs for broader searches
  lidarr-instance:
    type: lidarr
    search_type: artist        # Search entire artist instead of individual albums
```

**Note:** `series` search uses `SeriesSearch` with `seriesId`. It is efficient in API call count, but it asks Sonarr to search the whole series and can create broad grab patterns. Prefer `season_packs` when you want pack-oriented searches without expanding every cleanup replacement into a whole-series search.

### Collection Search (Radarr)

Radarr supports grouping movies by collection:

```yaml
vigilance:
  radarr_collection_search_mode: "off"  # off | detect | force
```

- `off` — Standard movie-level search (default)
- `detect` — Group movies by collection when found in wanted endpoints
- `force` — Fetch from `/api/v3/collection` and search all monitored collections with missing movies

### Vigilance — Search Scheduling

Configure how Warden hunts for missing and upgrade media:

```yaml
vigilance:                          # legacy alias: global
  dry_run: false                    # Log intended searches without sending commands
  active_hours: ""                  # Optional local-time window, e.g. "22:00-06:00"
  run_interval_minutes: 30          # How often to check for new items
  run_interval_minutes_missing:     # Optional missing-only interval in minutes
  run_interval_minutes_upgrade:     # Optional upgrade-only interval in minutes
  missing_batch_size: 25            # Items searched per cycle (0 = disabled, -1 = unlimited)
  upgrade_batch_size: 0             # Upgrade searches per cycle (0 = disabled)
  search_order: release_date_ascending  # alphabetical_* | last_added_* | last_searched_* | release_date_* | random
  stagger_interval_seconds: 10      # Delay between individual search commands
  retry_interval_days: 5            # Skip items searched within this window
  retry_interval_days_missing: 3    # Override for missing items only
  retry_interval_days_upgrade: 7    # Override for upgrade items only
  fetch_page_size: 2000             # Records per API request
  fetch_record_limit: 0             # Cap records pulled per wanted fetch (0 = unlimited)
  fetch_timeout_seconds: 120        # HTTP request timeout
  queue_check_timeout_seconds: 15   # Short timeout for max_queue_size checks
  max_queue_size: 500               # Vigilance only: pause searches if queue >= this (0 = disabled)
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

Control when to search for entire seasons instead of individual episodes:

```yaml
instances:
  sonarr-instance:
    type: sonarr
    season_packs: true              # Always use season search
    # season_packs: 0.75           # Or: threshold ratio (75% of episodes missing)
    # season_packs: 5              # Or: minimum episode count
```

#### Per-Tag Search Limits (Sonarr)

For very large libraries, paging the entire wanted/missing set every cycle is slow
and can flood your download client. `tag_limits` instead caps how many **series**
are searched per *Arr tag each cycle, replacing the whole-library scan:

```yaml
instances:
  sonarr-instance:
    type: sonarr
    search_type: series
    tag_limits:
      anime: 10                     # up to 10 anime series per cycle
      live-action: 10               # up to 10 live-action series per cycle
```

- Each entry triggers one **`SeriesSearch`**, so Sonarr resolves season packs with
  per-episode fallback (**series → season → episode**); a whole multi-season series
  counts as a single unit against the cap.
- A per-tag cursor **rotates through the backlog** across cycles (first N → next N →
  … → wrap) rather than re-searching the same top-N every time. The cursor is
  in-memory and resets to the top of each tag on restart.
- Series whose tags are not listed — and untagged series — are **not** searched by
  this path; add their tag to `tag_limits` to include them.
- When set, `tag_limits` takes precedence over `search_type` and `season_packs` for
  that instance, and the instance is exempt from cross-instance batch allocation (its
  per-tag caps are authoritative). It is **missing-only** — upgrades are unaffected.
- Raise the numbers to move through a backlog faster. For huge libraries this is
  preferable to `fetch_record_limit`, which only truncates the scan rather than
  fairly distributing searches across tags.

### Defence — Queue Cleanup

Configure how Warden defends your library from problematic downloads:

```yaml
defence:                            # legacy alias: cleanup
  dry_run: false                    # Log intended removals without deleting queue items
  active_hours: ""                  # Optional local-time window, e.g. "22:00-06:00"
  interval: 600                     # Run every 10 minutes
  batch_size: -1                    # Process all stalled items (-1 = unlimited, 0 = disabled)
  stagger_interval_seconds: 5       # Delay between removals
  circuit_breaker_threshold: 3      # Skip after N consecutive failures
  cleanup_page_size: 100            # Queue records per API request
  max_cleanup_queue_records: 0      # Defence scan safety cap only; does not pause cleanup based on queue size
  max_removals_per_instance: 25     # Per-instance removal cap per cycle (0 = no cap)
  delete_timeout_seconds: 15        # Timeout for queue deletion calls
  fetch_timeout_seconds: 30         # HTTP timeout for queue fetches
  retry_interval_minutes: 0         # Cooldown before re-evaluating removed items (0 = off)
  removal_order: api_order          # api_order | age_ascending | age_descending
  cleanup_search_scope: episode     # episode | season | series (what ID to search after removal)
  protect_downloading_series: false # Hold back stalled items from series with active downloads
  queue_max_age_hours: 0            # Clean non-completed items stuck in queue > N hours (0 = off)
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
  download_unavailable: blocklist   # Items orphaned by an unavailable download client
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
| `download_unavailable` | Download client reported unavailable (orphaned queue item) |
| `unknown` | Unrecognized messages (please report) |

### Shared Settings

#### Active Hours

Restrict both modes to a specific local-time window. Set `TZ` in the container environment to control the timezone used by these checks:

```yaml
vigilance:
  active_hours: "22:00-06:00"     # Only run between 10 PM and 6 AM local time
```

Leave empty or omit for all hours (default).

#### Circuit Breaker

Skip instances after consecutive failures:

```yaml
vigilance:
  circuit_breaker_threshold: 3      # Vigilance fetch failures
defence:
  circuit_breaker_threshold: 3      # Defence cleanup failures
```

#### Per-Instance Overrides

Any vigilance or defence setting can be overridden per instance (legacy `global`/`cleanup` names still accepted):

```yaml
instances:
  lidarr-instance:
    type: lidarr
    max_removals_per_instance: 5     # Override Defence cap for this instance only
    manual_import: remove            # Override one cleanup stall category for this instance
    search_type: artist
  sonarr-instance:
    type: sonarr
    max_queue_size: 200              # Override Vigilance queue-pause threshold; cleanup ignores this
    tag_limits: { anime: 10 }        # Per-tag search caps apply per instance
```

## Docker Compose

```yaml
services:
  warden:
    image: ghcr.io/johagan94/warden:latest
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

**Registry:** Images are published to GitHub Container Registry (`ghcr.io/johagan94/warden`). For private deployments, use your own registry URL.

## Decypharr (optional)

Running Warden alongside [decypharr](https://github.com/sirrobot01/decypharr)?
The [`extras/decypharr/`](extras/decypharr/) folder has optional host-side
helper scripts for decypharr's FUSE mount lifecycle — a stale-mount watchdog
(`heartbeat.sh`) and a clean array-stop orchestrator (`janitor.sh`). They are
not required by Warden and are ignored by anyone not using decypharr. See
[`extras/decypharr/README.md`](extras/decypharr/README.md).

## Building

```bash
docker build -t warden:latest .
```

## License

MIT
