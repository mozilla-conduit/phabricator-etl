# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Unit tests for the small utility helpers exposed by `phabricator_etl.stats`."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from phabricator_etl.stats import (
    revision_year_month,
    sql_table_id,
    staging_table_id,
    truncate_staging_table,
)


def revision_with_modified(modified_at: datetime) -> SimpleNamespace:
    """Build a revision stand-in whose `dateModified` is the given UTC datetime."""
    return SimpleNamespace(dateModified=modified_at.timestamp())


def test_staging_table_id_appends_suffix():
    assert staging_table_id("mozdata.phabricator.revisions") == (
        "mozdata.phabricator.revisions_staging"
    ), (
        "`staging_table_id` should append `_staging` to a fully-qualified "
        "BigQuery table ID without modifying the rest of the identifier."
    )


def test_staging_table_id_appends_suffix_to_empty_string():
    assert staging_table_id("") == "_staging", (
        "`staging_table_id` should append `_staging` even when given an "
        "empty input, since it is a pure string operation with no "
        "validation."
    )


def test_sql_table_id_uses_dot_separators():
    table = SimpleNamespace(
        project="mozdata",
        dataset_id="phabricator",
        table_id="revisions",
    )
    assert sql_table_id(table) == "mozdata.phabricator.revisions", (
        "`sql_table_id` should produce a standard-SQL-style identifier "
        "`project.dataset.table`, not the BigQuery default form "
        "`project:dataset.table`."
    )


def test_sql_table_id_round_trips_with_staging_table_id():
    table = SimpleNamespace(
        project="mozdata",
        dataset_id="phabricator",
        table_id="diffs",
    )
    assert staging_table_id(sql_table_id(table)) == (
        "mozdata.phabricator.diffs_staging"
    ), (
        "`staging_table_id(sql_table_id(table))` is the canonical way the "
        "ETL builds staging-table IDs; the composed result should equal "
        "`<project>.<dataset>.<table>_staging`."
    )


def test_revision_year_month_extracts_utc_year_and_month():
    revision = revision_with_modified(
        datetime(2025, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
    )
    assert revision_year_month(revision) == (2025, 7), (
        "`revision_year_month` should return `(year, month)` based on the "
        "UTC interpretation of `dateModified`."
    )


def test_revision_year_month_handles_year_boundary():
    last_second_of_2024 = revision_with_modified(
        datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    )
    first_second_of_2025 = revision_with_modified(
        datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert revision_year_month(last_second_of_2024) == (2024, 12), (
        "A timestamp at `2024-12-31T23:59:59Z` should bucket into "
        "December 2024, not January 2025."
    )
    assert revision_year_month(first_second_of_2025) == (2025, 1), (
        "A timestamp at `2025-01-01T00:00:00Z` should bucket into January 2025."
    )


def test_revision_year_month_handles_month_boundary():
    end_of_april = revision_with_modified(
        datetime(2025, 4, 30, 23, 59, 59, tzinfo=timezone.utc)
    )
    start_of_may = revision_with_modified(
        datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert revision_year_month(end_of_april) == (2025, 4), (
        "A timestamp at the last second of April should bucket into April."
    )
    assert revision_year_month(start_of_may) == (2025, 5), (
        "A timestamp at the first second of May should bucket into May."
    )


def test_truncate_staging_table_rejects_non_staging_id():
    bq_client = MagicMock(name="bigquery.Client")
    bad_table_id = "mozdata.phabricator.revisions"

    with pytest.raises(ValueError) as excinfo:
        truncate_staging_table(bq_client, bad_table_id)

    assert bad_table_id in str(excinfo.value), (
        "The `ValueError` raised when refusing to truncate a non-staging "
        "table should name the offending table ID so the operator can "
        "tell what was being truncated."
    )
    assert not bq_client.query.called, (
        "`truncate_staging_table` must not issue a query when the guard "
        "fails; otherwise a production table could still be truncated."
    )


def test_truncate_staging_table_accepts_staging_id():
    bq_client = MagicMock(name="bigquery.Client")
    staging_id = "mozdata.phabricator.revisions_staging"

    truncate_staging_table(bq_client, staging_id)

    bq_client.query.assert_called_once_with(f"TRUNCATE TABLE `{staging_id}`")
    bq_client.query.return_value.result.assert_called_once_with()
