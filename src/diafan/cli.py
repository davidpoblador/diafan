# ABOUTME: CLI for interacting with Socrata-based open data portals.
# ABOUTME: Supports listing dataset versions and downloading archived snapshots.

import time
from datetime import datetime
from pathlib import Path

import httpx
import tqdm
import typer

app = typer.Typer()

DEFAULT_DOMAIN = "analisi.transparenciacatalunya.cat"
CHANGES_PAGE_SIZE = 100
ARCHIVE_POLL_INTERVAL_SECONDS = 3
ARCHIVE_POLL_TIMEOUT_SECONDS = 600


def _base_url(domain: str) -> str:
    return f"https://{domain}"


def _make_client() -> httpx.Client:
    return httpx.Client(http2=True)


def _format_timestamp(ts: str) -> str:
    """Parse an ISO timestamp and return a human-readable date string."""
    dt = datetime.fromisoformat(ts)
    return dt.strftime("%Y-%m-%d %H:%M")


def _build_archive(client: httpx.Client, domain: str, dataset_id: str, version: int) -> None:
    """Trigger archive materialization and poll until done."""
    base = _base_url(domain)

    resp = client.put(
        f"{base}/api/archival",
        params={"id": dataset_id, "method": "createArchive", "version": version},
        timeout=30,
    )
    resp.raise_for_status()

    deadline = time.monotonic() + ARCHIVE_POLL_TIMEOUT_SECONDS
    with tqdm.tqdm(
        bar_format="  Building archive... [{elapsed}] {desc}",
        desc="waiting",
    ) as progress:
        while time.monotonic() < deadline:
            status_resp = client.get(
                f"{base}/api/archival",
                params={"id": dataset_id, "version": version, "method": "status"},
                timeout=30,
            )
            status_resp.raise_for_status()
            status = status_resp.json()

            if status.get("type") == "done":
                progress.desc = "done"
                progress.close()
                return
            if status.get("type") == "error":
                progress.close()
                raise typer.Exit(f"Archive build failed: {status}")

            progress.desc = status.get("type", "?")
            progress.update()
            time.sleep(ARCHIVE_POLL_INTERVAL_SECONDS)

    typer.echo("Archive build timed out.", err=True)
    raise typer.Exit(code=1)


@app.command()
def info(
    dataset_id: str = typer.Argument(help="Socrata dataset identifier (e.g. gn9e-3qhr)"),
    domain: str = typer.Option(DEFAULT_DOMAIN, help="Socrata domain"),
) -> None:
    """Show basic metadata about a dataset."""
    url = f"{_base_url(domain)}/api/views/{dataset_id}.json"
    with _make_client() as client:
        resp = client.get(url, timeout=30)
    resp.raise_for_status()
    meta = resp.json()

    typer.echo(f"Name:        {meta['name']}")
    typer.echo(f"ID:          {meta['id']}")
    typer.echo(f"Category:    {meta.get('category', '—')}")
    typer.echo(f"Attribution: {meta.get('attribution', '—')}")
    typer.echo(f"Views:       {meta.get('viewCount', '—')}")
    typer.echo(f"Downloads:   {meta.get('downloadCount', '—')}")

    if meta.get("rowsUpdatedAt"):
        ts = datetime.fromtimestamp(meta["rowsUpdatedAt"])
        typer.echo(f"Data updated: {ts.strftime('%Y-%m-%d %H:%M')}")

    if meta.get("createdAt"):
        ts = datetime.fromtimestamp(meta["createdAt"])
        typer.echo(f"Created:     {ts.strftime('%Y-%m-%d %H:%M')}")

    if meta.get("description"):
        typer.echo(f"\n{meta['description'][:300]}")


@app.command()
def versions(
    dataset_id: str = typer.Argument(help="Socrata dataset identifier (e.g. gn9e-3qhr)"),
    domain: str = typer.Option(DEFAULT_DOMAIN, help="Socrata domain"),
    limit: int = typer.Option(0, help="Max versions to show (0 = all)"),
) -> None:
    """List archived versions of a Socrata dataset, newest first."""
    url = f"{_base_url(domain)}/api/publishing/v1/revision/{dataset_id}/changes"
    all_versions: list[dict] = []
    cursor: str = ""

    with _make_client() as client:
        while True:
            params: dict[str, str | int] = {"limit": CHANGES_PAGE_SIZE, "cursor": cursor}
            resp = client.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("resource", [])
            all_versions.extend(entries)

            next_cursor = data.get("meta", {}).get("next")
            if not next_cursor or not entries:
                break
            cursor = next_cursor

            if limit > 0 and len(all_versions) >= limit:
                all_versions = all_versions[:limit]
                break

    if not all_versions:
        typer.echo("No versions found.")
        raise typer.Exit()

    if limit > 0:
        all_versions = all_versions[:limit]

    typer.echo(f"{'VERSION':>8}  {'CREATED':>18}")
    typer.echo("-" * 30)

    for entry in all_versions:
        v = entry["value"]
        version = v["version"]
        created = _format_timestamp(v["created_at"])
        typer.echo(f"{version:>8}  {created:>18}")

    typer.echo(f"\nTotal: {len(all_versions)} version(s)")


def _resolve_output_path(
    client: httpx.Client, domain: str, dataset_id: str, suffix: str,
) -> Path:
    """Build a default output filename from the dataset's human name."""
    base = _base_url(domain)
    meta_resp = client.get(f"{base}/api/views/{dataset_id}.json", timeout=30)
    meta_resp.raise_for_status()
    name = meta_resp.json()["name"]
    safe_name = name.replace("/", "_").replace(" ", "_")
    return Path(f"{safe_name}_{suffix}.csv")


def _stream_to_file(resp: httpx.Response, output: Path) -> None:
    """Stream an HTTP response to a file with a tqdm progress bar."""
    content_length = resp.headers.get("content-length")
    total_expected = int(content_length) if content_length else None

    with (
        open(output, "wb") as f,
        tqdm.tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            total=total_expected,
            desc="Downloading",
        ) as progress,
    ):
        for chunk in resp.iter_bytes(chunk_size=8192):
            f.write(chunk)
            progress.update(len(chunk))


@app.command()
def download(
    dataset_id: str = typer.Argument(help="Socrata dataset identifier (e.g. gn9e-3qhr)"),
    version: int = typer.Argument(help="Version number to download"),
    output: Path = typer.Option(None, "--output", "-o", help="Output file path"),
    domain: str = typer.Option(DEFAULT_DOMAIN, help="Socrata domain"),
) -> None:
    """Download a specific archived version of a dataset as CSV."""
    base = _base_url(domain)

    with _make_client() as client:
        if output is None:
            output = _resolve_output_path(client, domain, dataset_id, f"v{version}")

        typer.echo(f"Requesting archive for {dataset_id} v{version}...")
        _build_archive(client, domain, dataset_id, version)

        with client.stream(
            "GET",
            f"{base}/api/archival.csv",
            params={"id": dataset_id, "version": version, "method": "export"},
            timeout=600,
            follow_redirects=True,
        ) as resp:
            resp.raise_for_status()
            _stream_to_file(resp, output)

    size_mb = output.stat().st_size / (1024 * 1024)
    typer.echo(f"Saved to {output} ({size_mb:.1f} MB)")


@app.command()
def download_current(
    dataset_id: str = typer.Argument(help="Socrata dataset identifier (e.g. gn9e-3qhr)"),
    output: Path = typer.Option(None, "--output", "-o", help="Output file path"),
    domain: str = typer.Option(DEFAULT_DOMAIN, help="Socrata domain"),
) -> None:
    """Download the current (latest) version of a dataset as CSV."""
    base = _base_url(domain)

    with _make_client() as client:
        if output is None:
            output = _resolve_output_path(client, domain, dataset_id, "current")

        typer.echo(f"Downloading current snapshot of {dataset_id}...")
        with client.stream(
            "GET",
            f"{base}/api/views/{dataset_id}/rows.csv",
            params={"accessType": "DOWNLOAD"},
            timeout=600,
            follow_redirects=True,
        ) as resp:
            resp.raise_for_status()
            _stream_to_file(resp, output)

    size_mb = output.stat().st_size / (1024 * 1024)
    typer.echo(f"Saved to {output} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    app()
