# ABOUTME: Tests for paginated download of current dataset snapshots.
# ABOUTME: Verifies correct pagination, header deduplication, and progress tracking.

from pathlib import Path
from unittest.mock import MagicMock

import httpx

from diafan.cli import _download_current_paginated, RESOURCE_PAGE_SIZE


def _fake_response(text: str, status_code: int = 200) -> httpx.Response:
    """Build a real httpx.Response with the given text body."""
    return httpx.Response(
        status_code=status_code,
        text=text,
        request=httpx.Request("GET", "https://example.com"),
    )


class TestDownloadCurrentPaginated:
    def test_single_page_dataset(self, tmp_path: Path) -> None:
        """A dataset smaller than one page should produce a single request."""
        output = tmp_path / "out.csv"
        csv_content = "col_a,col_b\n1,2\n3,4\n"

        client = MagicMock(spec=httpx.Client)
        # Count request
        client.get.side_effect = [
            _fake_response('[{"count": "2"}]'),  # row count
            _fake_response(csv_content),  # single page
        ]

        _download_current_paginated(
            client=client,
            base_url="https://example.com",
            dataset_id="test-1234",
            output=output,
            fmt_ext="csv",
        )

        result = output.read_text()
        assert result == csv_content
        assert client.get.call_count == 2

    def test_multi_page_csv_strips_duplicate_headers(self, tmp_path: Path) -> None:
        """When paginating CSV, headers should appear only once."""
        output = tmp_path / "out.csv"
        page1 = "col_a,col_b\n1,2\n3,4\n"
        page2 = "col_a,col_b\n5,6\n7,8\n"

        client = MagicMock(spec=httpx.Client)
        page_size = RESOURCE_PAGE_SIZE
        client.get.side_effect = [
            _fake_response(f'[{{"count": "{page_size + 2}"}}]'),
            _fake_response(page1),
            _fake_response(page2),
        ]

        _download_current_paginated(
            client=client,
            base_url="https://example.com",
            dataset_id="test-1234",
            output=output,
            fmt_ext="csv",
        )

        result = output.read_text()
        assert result == "col_a,col_b\n1,2\n3,4\n5,6\n7,8\n"

    def test_multi_page_json_concatenates_arrays(self, tmp_path: Path) -> None:
        """When paginating JSON, arrays from each page should be concatenated."""
        output = tmp_path / "out.json"
        page1 = '[{"a": 1}, {"a": 2}]'
        page2 = '[{"a": 3}]'

        client = MagicMock(spec=httpx.Client)
        page_size = RESOURCE_PAGE_SIZE
        client.get.side_effect = [
            _fake_response(f'[{{"count": "{page_size + 1}"}}]'),
            _fake_response(page1),
            _fake_response(page2),
        ]

        _download_current_paginated(
            client=client,
            base_url="https://example.com",
            dataset_id="test-1234",
            output=output,
            fmt_ext="json",
        )

        import json

        result = json.loads(output.read_text())
        assert result == [{"a": 1}, {"a": 2}, {"a": 3}]

    def test_empty_dataset(self, tmp_path: Path) -> None:
        """A dataset with zero rows should produce an empty file."""
        output = tmp_path / "out.csv"

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _fake_response('[{"count": "0"}]'),
        ]

        _download_current_paginated(
            client=client,
            base_url="https://example.com",
            dataset_id="test-1234",
            output=output,
            fmt_ext="csv",
        )

        assert output.read_text() == ""
        assert client.get.call_count == 1  # only the count request

    def test_exact_page_boundary(self, tmp_path: Path) -> None:
        """When row count is exactly one page, only one data request is made."""
        output = tmp_path / "out.csv"
        page_size = RESOURCE_PAGE_SIZE
        rows = "".join(f"{i},{i + 1}\n" for i in range(page_size))
        csv_content = f"col_a,col_b\n{rows}"

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _fake_response(f'[{{"count": "{page_size}"}}]'),
            _fake_response(csv_content),
        ]

        _download_current_paginated(
            client=client,
            base_url="https://example.com",
            dataset_id="test-1234",
            output=output,
            fmt_ext="csv",
        )

        assert client.get.call_count == 2  # count + 1 page

    def test_requests_use_correct_params(self, tmp_path: Path) -> None:
        """Verify that pagination requests use correct $limit and $offset."""
        output = tmp_path / "out.csv"
        page_size = RESOURCE_PAGE_SIZE

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _fake_response(f'[{{"count": "{page_size + 1}"}}]'),
            _fake_response("a,b\n1,2\n"),
            _fake_response("a,b\n3,4\n"),
        ]

        _download_current_paginated(
            client=client,
            base_url="https://example.com",
            dataset_id="test-1234",
            output=output,
            fmt_ext="csv",
        )

        calls = client.get.call_args_list
        # First call: count
        assert "$select" in str(calls[0])
        # Second call: first page
        assert calls[1].kwargs.get("params", {}).get("$offset") == 0
        assert calls[1].kwargs.get("params", {}).get("$limit") == page_size
        # Third call: second page
        assert calls[2].kwargs.get("params", {}).get("$offset") == page_size
        # All data requests should use :id ordering for consistent pagination
        for call in calls[1:]:
            assert call.kwargs.get("params", {}).get("$order") == ":id"
