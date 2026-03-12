# subv

CLI for downloading datasets and historical versions from Socrata-based open data portals. Defaults to the Catalan government's transparency portal (`analisi.transparenciacatalunya.cat`).

## Setup

```
uv sync
```

## Commands

### info

Show metadata for a dataset.

```
uv run python main.py info gn9e-3qhr
```

### versions

List archived versions (newest first).

```
uv run python main.py versions gn9e-3qhr
uv run python main.py versions gn9e-3qhr --limit 10
```

### download

Download a specific archived version as CSV. Triggers server-side materialization if needed, then streams the result.

```
uv run python main.py download gn9e-3qhr 1348
uv run python main.py download gn9e-3qhr 1348 -o output.csv
```

Note: archival materialization can be slow or may not work for very large datasets (19M+ rows). Use `download-current` as a fallback.

### download-current

Download the latest snapshot directly (no archival, always works).

```
uv run python main.py download-current s9xt-n979
```

## Socrata API details

This tool uses several undocumented Socrata endpoints discovered via reverse engineering:

- `/api/publishing/v1/revision/{id}/changes` -- list archived versions (cursor-paginated)
- `/api/archival?id={id}&method=createArchive&version={v}` -- trigger materialization
- `/api/archival?id={id}&version={v}&method=status` -- poll materialization status
- `/api/archival.csv?id={id}&version={v}&method=export` -- download materialized archive
- `/api/views/{id}/rows.csv?accessType=DOWNLOAD` -- download current snapshot
