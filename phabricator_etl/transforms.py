# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Pure transformation helpers that turn Phabricator ORM rows into ETL output.

Everything in this module is side-effect free and importable without a
database connection or environment variables, which makes it directly
unit-testable. Functions that actually query the database live in
`phabricator_etl.stats`.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional


def convert_value_to_string(value: Any) -> str:
    """Coerce transaction values to string.

    If the passed value is a boolean, then we convert it to the string
    `"1"` or `"0"`. Otherwise we return it as a string.
    """
    if isinstance(value, bool):
        # `"1"` for `True`, `"0"` for `False`.
        return str(int(value))

    return str(value)


def transform_changeset_dict(
    changeset: Any,
    revision_id: int,
    diff_id: int,
) -> dict:
    """Build the output dict for a single changeset row."""
    return {
        "revision_id": revision_id,
        "diff_id": diff_id,
        "changeset_id": changeset.id,
        "lines_added": changeset.addLines,
        "lines_removed": changeset.delLines,
        "filename": changeset.filename.decode("utf-8"),
    }


def transform_comment_dict(
    comment: Any,
    revision_id: int,
    diff_id: Optional[int],
    author_email: Optional[str],
    author_username: Optional[str],
) -> dict:
    """Build the output dict for a single comment row.

    Parses the JSON-encoded `attributes` field to detect whether the
    comment is an inline suggestion. A malformed `attributes` field
    raises `json.JSONDecodeError` (preserving prior behavior).
    """
    attributes = json.loads(comment.attributes)
    is_suggestion = (
        "inline.state.initial" in attributes
        and attributes["inline.state.initial"].get("hassuggestion") == "true"
    )

    return {
        "revision_id": revision_id,
        "diff_id": diff_id,
        "changeset_id": comment.changesetID,
        "comment_id": comment.id,
        "author_email": author_email,
        "author_username": author_username,
        "date_created": comment.dateCreated,
        "character_count": len(comment.content),
        "is_suggestion": is_suggestion,
    }


def transform_transaction_dict(
    transaction: Any,
    revision_id: int,
    author_email: Optional[str],
    author_username: Optional[str],
) -> dict:
    """Build the output dict for a single transaction row."""
    return {
        "revision_id": revision_id,
        "transaction_id": transaction.id,
        "transaction_type": transaction.transactionType,
        "author_email": author_email,
        "author_username": author_username,
        "date_created": transaction.dateCreated,
        "old_value": convert_value_to_string(transaction.oldValue),
        "new_value": convert_value_to_string(transaction.newValue),
    }


def should_include_diff(diff: Any) -> bool:
    """Return `True` if a diff should be emitted as part of the diffs table.

    Two kinds of diffs are skipped:
    - diffs created via the `"commit"` method, which represent landings
      rather than user-uploaded revisions (their `dateCreated` feeds
      `latest_landed_date` instead),
    - diffs whose author PHID is a `PHID-RIDT-*` repository identity,
      which are bookkeeping artifacts and not real reviewer activity.
    """
    if diff.creationMethod == "commit":
        return False
    if diff.authorPHID.startswith(b"PHID-RIDT-"):
        return False
    return True


def latest_landed_date(diffs: Iterable[Any]) -> Optional[int]:
    """Return the latest `dateCreated` among diffs created via `"commit"`.

    Returns `None` when no commit diffs are present.
    """
    commit_dates = [
        diff.dateCreated for diff in diffs if diff.creationMethod == "commit"
    ]
    return max(commit_dates) if commit_dates else None


def transform_review_dict(
    review: Any,
    revision_id: int,
    reviewer_username: Optional[str],
    reviewer_email: Optional[str],
    is_group: bool,
    last_action_diff_id: Optional[int],
    last_comment_diff_id: Optional[int],
) -> dict:
    """Build the output dict for a single review row."""
    return {
        "revision_id": revision_id,
        "review_id": review.id,
        "reviewer_username": reviewer_username,
        "reviewer_email": reviewer_email,
        "is_group": is_group,
        "date_created": review.dateCreated,
        "date_modified": review.dateModified,
        "status": review.reviewerStatus,
        "last_action_diff_id": last_action_diff_id,
        "last_comment_diff_id": last_comment_diff_id,
    }


def latest_approved_date(reviews: Iterable[Any]) -> Optional[int]:
    """Return the latest `dateModified` among reviews with status `"accepted"`.

    Returns `None` when no accepted reviews are present.
    """
    accepted_dates = [
        review.dateModified for review in reviews if review.reviewerStatus == "accepted"
    ]
    return max(accepted_dates) if accepted_dates else None


def parse_repository_details(target_repo: Optional[Any]) -> dict:
    """Return the parsed `details` JSON for a repository, or `{}`.

    Returns an empty dict when the repository is missing or has no
    `details` payload.
    """
    if not target_repo or not target_repo.details:
        return {}
    return json.loads(target_repo.details)
