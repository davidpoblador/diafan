# ABOUTME: CLI for downloading datasets from the Catalan transparency portal.
# ABOUTME: Supports listing versions, downloading snapshots, and inspecting schema.

import asyncio
import enum
import json
import math
import tempfile
import time
from datetime import datetime
from pathlib import Path

import httpx
import tqdm
import typer
from slugify import slugify
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

app = typer.Typer(no_args_is_help=True)
console = Console()

DEFAULT_DOMAIN = "analisi.transparenciacatalunya.cat"
CHANGES_PAGE_SIZE = 100
RESOURCE_PAGE_SIZE = 50_000
DOWNLOAD_CONCURRENCY = 8
ARCHIVE_POLL_INTERVAL_SECONDS = 3
ARCHIVE_POLL_TIMEOUT_SECONDS = 600


class Format(str, enum.Enum):
    csv = "csv"
    json = "json"


def _base_url(domain: str) -> str:
    return f"https://{domain}"


def _make_client() -> httpx.Client:
    return httpx.Client(http2=True)


def _format_timestamp(ts: str) -> str:
    """Parse an ISO timestamp and return a human-readable date string."""
    dt = datetime.fromisoformat(ts)
    return dt.strftime("%Y-%m-%d %H:%M")


def _build_archive(
    client: httpx.Client, domain: str, dataset_id: str, version: int
) -> None:
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
        bar_format="  Construint arxiu... [{elapsed}] {desc}",
        desc="esperant",
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
                progress.desc = "fet"
                progress.close()
                return
            if status.get("type") == "error":
                progress.close()
                console.print(
                    f"[red]Error construint l'arxiu: {status}[/red]", stderr=True
                )
                raise typer.Exit(code=1)

            progress.desc = status.get("type", "?")
            progress.update()
            time.sleep(ARCHIVE_POLL_INTERVAL_SECONDS)

    console.print("[red]Temps d'espera excedit.[/red]", stderr=True)
    raise typer.Exit(code=1)


def _fetch_metadata(client: httpx.Client, domain: str, dataset_id: str) -> dict:
    """Fetch dataset metadata from the API."""
    base = _base_url(domain)
    resp = client.get(f"{base}/api/views/{dataset_id}.json", timeout=30)
    resp.raise_for_status()
    return resp.json()


def _resolve_output_path(
    client: httpx.Client,
    domain: str,
    dataset_id: str,
    suffix: str,
    fmt: Format,
) -> Path:
    """Build a default output filename from the dataset's human name."""
    meta = _fetch_metadata(client, domain, dataset_id)
    safe_name = slugify(meta["name"])
    return Path(f"{safe_name}-{dataset_id}-{suffix}.{fmt.value}")


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
            desc="Descarregant",
        ) as progress,
    ):
        for chunk in resp.iter_bytes(chunk_size=8192):
            f.write(chunk)
            progress.update(len(chunk))


def _relative_time(dt: datetime) -> str:
    """Return a Catalan relative time string like 'fa 3 dies' or 'fa 2 hores'."""
    now = datetime.now()
    delta = now - dt
    seconds = int(delta.total_seconds())

    if seconds < 0:
        return ""
    if seconds < 60:
        return f"fa {seconds} s"
    minutes = seconds // 60
    if minutes < 60:
        return f"fa {minutes} min"
    hours = minutes // 60
    if hours < 24:
        return f"fa {hours} {'hora' if hours == 1 else 'hores'}"
    days = hours // 24
    if days < 30:
        return f"fa {days} {'dia' if days == 1 else 'dies'}"
    months = days // 30
    if months < 12:
        return f"fa {months} {'mes' if months == 1 else 'mesos'}"
    years = days // 365
    return f"fa {years} {'any' if years == 1 else 'anys'}"


def _format_unix_timestamp(ts: int | float) -> Text:
    """Format a Unix timestamp with a relative time hint."""
    dt = datetime.fromtimestamp(ts)
    result = Text(dt.strftime("%Y-%m-%d %H:%M"))
    relative = _relative_time(dt)
    if relative:
        result.append(f"  ({relative})", style="dim")
    return result


@app.command()
def info(
    dataset_id: str = typer.Argument(
        help="Identificador del conjunt de dades (p. ex. gn9e-3qhr)"
    ),
    domain: str = typer.Option(
        DEFAULT_DOMAIN, help="Domini del portal de dades obertes"
    ),
) -> None:
    """Mostra les metadades bàsiques d'un conjunt de dades."""
    with _make_client() as client:
        meta = _fetch_metadata(client, domain, dataset_id)

    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()

    table.add_row("Nom", meta["name"])
    table.add_row("ID", meta["id"])
    table.add_row("Categoria", meta.get("category", "—"))
    table.add_row("Atribució", meta.get("attribution", "—"))

    if meta.get("attributionLink"):
        table.add_row("Enllaç", meta["attributionLink"])

    owner_name = meta.get("owner", {}).get("displayName")
    if owner_name:
        table.add_row("Publicat per", owner_name)

    if meta.get("provenance"):
        table.add_row("Procedència", meta["provenance"])

    license_name = meta.get("license", {}).get("name")
    if license_name:
        table.add_row("Llicència", license_name)

    table.add_row("Visualitzacions", f"{meta.get('viewCount', 0):,}".replace(",", "."))
    table.add_row("Descàrregues", f"{meta.get('downloadCount', 0):,}".replace(",", "."))

    columns = meta.get("columns", [])
    if columns:
        table.add_row("Columnes", str(len(columns)))

    if meta.get("createdAt"):
        table.add_row("Creat", _format_unix_timestamp(meta["createdAt"]))

    if meta.get("publicationDate"):
        table.add_row("Publicat", _format_unix_timestamp(meta["publicationDate"]))

    if meta.get("rowsUpdatedAt"):
        table.add_row(
            "Dades actualitzades", _format_unix_timestamp(meta["rowsUpdatedAt"])
        )

    if meta.get("viewLastModified"):
        table.add_row(
            "Última modificació", _format_unix_timestamp(meta["viewLastModified"])
        )

    parts: list = [table]

    if meta.get("description"):
        parts.append(Text())
        parts.append(Padding(Text("Descripció", style="bold"), (0, 1)))
        parts.append(Padding(Text(meta["description"]), (0, 1)))

    tags = meta.get("tags", [])
    if tags:
        parts.append(Text())
        parts.append(Padding(Text("Etiquetes", style="bold"), (0, 1)))
        tag_text = Text()
        for i, tag in enumerate(tags):
            if i > 0:
                tag_text.append(" ")
            tag_text.append(f"#{tag}", style="dim cyan")
        parts.append(Padding(tag_text, (0, 1)))

    console.print(Panel(Group(*parts), title=meta["name"], border_style="blue"))


@app.command()
def schema(
    dataset_id: str = typer.Argument(
        help="Identificador del conjunt de dades (p. ex. gn9e-3qhr)"
    ),
    domain: str = typer.Option(
        DEFAULT_DOMAIN, help="Domini del portal de dades obertes"
    ),
) -> None:
    """Mostra l'estructura (columnes) d'un conjunt de dades."""
    with _make_client() as client:
        meta = _fetch_metadata(client, domain, dataset_id)

    columns = meta.get("columns", [])
    if not columns:
        console.print("[yellow]No s'ha trobat informació de columnes.[/yellow]")
        raise typer.Exit()

    table = Table(title=meta["name"])
    table.add_column("Nom", style="bold")
    table.add_column("Camp")
    table.add_column("Tipus")

    for col in columns:
        table.add_row(
            col.get("name", "—"),
            col.get("fieldName", "—"),
            col.get("dataTypeName", "—"),
        )

    console.print(table)


@app.command()
def versions(
    dataset_id: str = typer.Argument(
        help="Identificador del conjunt de dades (p. ex. gn9e-3qhr)"
    ),
    domain: str = typer.Option(
        DEFAULT_DOMAIN, help="Domini del portal de dades obertes"
    ),
    limit: int = typer.Option(15, help="Nombre màxim de versions a mostrar"),
    all_versions_flag: bool = typer.Option(
        False, "--all", help="Mostra totes les versions"
    ),
) -> None:
    """Llista les versions arxivades d'un conjunt de dades, de la més recent a la més antiga."""
    if all_versions_flag:
        limit = 0
    url = f"{_base_url(domain)}/api/publishing/v1/revision/{dataset_id}/changes"
    all_versions: list[dict] = []
    cursor: str = ""

    with _make_client() as client:
        meta = _fetch_metadata(client, domain, dataset_id)
        while True:
            params: dict[str, str | int] = {
                "limit": CHANGES_PAGE_SIZE,
                "cursor": cursor,
            }
            resp = client.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            entries = [
                e for e in data.get("resource", []) if e.get("type") == "archive"
            ]
            all_versions.extend(entries)

            next_cursor = data.get("meta", {}).get("next")
            if not next_cursor or not entries:
                break
            cursor = next_cursor

            if limit > 0 and len(all_versions) > limit:
                break

    truncated = limit > 0 and len(all_versions) > limit
    if truncated:
        all_versions = all_versions[:limit]

    if not all_versions:
        console.print("[yellow]No s'han trobat versions.[/yellow]")
        raise typer.Exit()

    table = Table(title=meta["name"])
    table.add_column("Versió", justify="right")
    table.add_column("Creat", justify="right", no_wrap=True)
    table.add_column(header="", justify="left")

    for entry in all_versions:
        v = entry["value"]
        dt = datetime.fromisoformat(v["created_at"])
        relative = _relative_time(dt)
        table.add_row(
            str(v["version"]),
            dt.strftime("%Y-%m-%d %H:%M"),
            Text(relative, style="dim") if relative else "",
        )

    console.print(table)
    count = len(all_versions)
    if truncated:
        console.print(
            f"\nMostrant les {count} versions més recents. Feu servir [bold]--all[/bold] per veure-les totes."
        )
    else:
        console.print(f"\nTotal: {count} {'versió' if count == 1 else 'versions'}")


@app.command()
def download(
    dataset_id: str = typer.Argument(
        help="Identificador del conjunt de dades (p. ex. gn9e-3qhr)"
    ),
    version: int = typer.Argument(help="Número de versió a descarregar"),
    output: Path = typer.Option(
        None, "--output", "-o", help="Ruta del fitxer de sortida"
    ),
    fmt: Format = typer.Option(Format.csv, "--format", "-f", help="Format de sortida"),
    domain: str = typer.Option(
        DEFAULT_DOMAIN, help="Domini del portal de dades obertes"
    ),
) -> None:
    """Descarrega una versió arxivada específica d'un conjunt de dades."""
    base = _base_url(domain)

    with _make_client() as client:
        if output is None:
            output = _resolve_output_path(
                client, domain, dataset_id, f"v{version}", fmt
            )

        console.print(f"Sol·licitant arxiu per {dataset_id} v{version}...")
        _build_archive(client, domain, dataset_id, version)

        ext = "json" if fmt == Format.json else "csv"
        with client.stream(
            "GET",
            f"{base}/api/archival.{ext}",
            params={"id": dataset_id, "version": version, "method": "export"},
            timeout=600,
            follow_redirects=True,
        ) as resp:
            resp.raise_for_status()
            _stream_to_file(resp, output)

    size_mb = output.stat().st_size / (1024 * 1024)
    console.print(f"Desat a {output} ({size_mb:.1f} MB)")


async def _download_current_paginated(
    client: httpx.AsyncClient,
    base_url: str,
    dataset_id: str,
    output: Path,
    fmt_ext: str,
) -> None:
    """Download a dataset via the SODA resource API with concurrent pagination."""
    count_resp = await client.get(
        f"{base_url}/resource/{dataset_id}.json",
        params={"$select": "count(*)"},
        timeout=30,
    )
    count_resp.raise_for_status()
    total_rows = int(count_resp.json()[0]["count"])

    if total_rows == 0:
        output.write_text("")
        return

    total_pages = math.ceil(total_rows / RESOURCE_PAGE_SIZE)
    resource_url = f"{base_url}/resource/{dataset_id}.{fmt_ext}"
    semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    with tempfile.TemporaryDirectory(prefix="diafan-") as tmp_dir:
        tmp_path = Path(tmp_dir)

        async def fetch_page(page: int) -> None:
            async with semaphore:
                offset = page * RESOURCE_PAGE_SIZE
                resp = await client.get(
                    resource_url,
                    params={
                        "$limit": RESOURCE_PAGE_SIZE,
                        "$offset": offset,
                        "$order": ":id",
                    },
                    timeout=600,
                )
                resp.raise_for_status()
                (tmp_path / f"page_{page:06d}").write_text(resp.text)
                progress.update(1)

        with tqdm.tqdm(total=total_pages, unit="pàg", desc="Descarregant") as progress:
            await asyncio.gather(*[fetch_page(p) for p in range(total_pages)])

        with open(output, "w") as f:
            if fmt_ext == "json":
                all_items: list = []
                for page in range(total_pages):
                    page_text = (tmp_path / f"page_{page:06d}").read_text()
                    all_items.extend(json.loads(page_text))
                f.write(json.dumps(all_items))
            else:
                for page in range(total_pages):
                    page_text = (tmp_path / f"page_{page:06d}").read_text()
                    if page > 0:
                        page_text = page_text[page_text.index("\n") + 1 :]
                    f.write(page_text)


@app.command()
def download_current(
    dataset_id: str = typer.Argument(
        help="Identificador del conjunt de dades (p. ex. gn9e-3qhr)"
    ),
    output: Path = typer.Option(
        None, "--output", "-o", help="Ruta del fitxer de sortida"
    ),
    fmt: Format = typer.Option(Format.csv, "--format", "-f", help="Format de sortida"),
    domain: str = typer.Option(
        DEFAULT_DOMAIN, help="Domini del portal de dades obertes"
    ),
) -> None:
    """Descarrega la versió actual (més recent) d'un conjunt de dades."""
    base = _base_url(domain)
    ext = "json" if fmt == Format.json else "csv"

    with _make_client() as sync_client:
        if output is None:
            output = _resolve_output_path(
                sync_client, domain, dataset_id, "actual", fmt
            )

    console.print(f"Descarregant snapshot actual de {dataset_id}...")

    async def _run() -> None:
        async with httpx.AsyncClient(http2=True) as client:
            await _download_current_paginated(client, base, dataset_id, output, ext)

    asyncio.run(_run())

    size_mb = output.stat().st_size / (1024 * 1024)
    console.print(f"Desat a {output} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    app()
