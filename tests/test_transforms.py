# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Unit tests for `phabricator_etl.transforms`."""

import json
from types import SimpleNamespace

import pytest

from phabricator_etl.transforms import (
    PhabricatorEdgeConstant,
    decode_name_transaction_value,
    is_membership_edge_transaction,
    parse_edge_member_phids,
    transform_project_transaction_dict,
    convert_value_to_string_list,
    latest_approved_date,
    latest_landed_date,
    parse_repository_details,
    should_include_diff,
    transform_changeset_dict,
    transform_comment_dict,
    transform_review_dict,
    transform_transaction_dict,
)


def test_is_membership_edge_transaction_true_for_member_edge():
    metadata = json.dumps(
        {"edge:type": PhabricatorEdgeConstant.PROJECT_HAS_MEMBER.value}
    )
    assert is_membership_edge_transaction(metadata) is True


def test_is_membership_edge_transaction_false_for_other_edge():
    # Some non-membership edge type (e.g. watchers).
    metadata = json.dumps({"edge:type": 42})
    assert is_membership_edge_transaction(metadata) is False


def test_is_membership_edge_transaction_false_for_missing_metadata():
    assert is_membership_edge_transaction(None) is False
    assert is_membership_edge_transaction("") is False
    assert is_membership_edge_transaction(json.dumps({})) is False


def test_parse_edge_member_phids_from_dict_snapshot():
    value = json.dumps(
        {
            "PHID-USER-alice": {"dst": "PHID-USER-alice"},
            "PHID-USER-bob": {"dst": "PHID-USER-bob"},
        }
    )
    assert parse_edge_member_phids(value) == {"PHID-USER-alice", "PHID-USER-bob"}


def test_parse_edge_member_phids_from_list_snapshot():
    value = json.dumps(["PHID-USER-alice", "PHID-USER-bob"])
    assert parse_edge_member_phids(value) == {"PHID-USER-alice", "PHID-USER-bob"}


def test_parse_edge_member_phids_empty_and_null():
    assert parse_edge_member_phids(None) == set()
    assert parse_edge_member_phids("") == set()
    assert parse_edge_member_phids("null") == set()


def test_membership_diff_added_and_removed():
    old = parse_edge_member_phids(
        json.dumps({"PHID-USER-alice": {}, "PHID-USER-bob": {}})
    )
    new = parse_edge_member_phids(
        json.dumps({"PHID-USER-bob": {}, "PHID-USER-carol": {}})
    )
    assert old - new == {"PHID-USER-alice"}  # removed
    assert new - old == {"PHID-USER-carol"}  # added


def test_decode_name_transaction_value():
    assert decode_name_transaction_value(json.dumps("My Project")) == ["My Project"]
    assert decode_name_transaction_value(None) == []
    assert decode_name_transaction_value("") == []
    assert decode_name_transaction_value("null") == []


def test_transform_project_transaction_dict_shape():
    transaction = SimpleNamespace(
        id=99,
        dateCreated=1700000000,
        transactionType="core:edge",
    )
    row = transform_project_transaction_dict(
        transaction=transaction,
        project_id=7,
        project_name="bmo-reviewers",
        author_email="alice@example.com",
        author_username="alice",
        old_value=["bob"],
        new_value=["carol"],
    )
    assert row == {
        "project_id": 7,
        "project_name": "bmo-reviewers",
        "transaction_id": 99,
        "author_email": "alice@example.com",
        "author_username": "alice",
        "date_created": 1700000000,
        "transaction_type": "core:edge",
        "old_value": ["bob"],
        "new_value": ["carol"],
    }


def test_transform_project_transaction_dict_create_has_new_name_only():
    transaction = SimpleNamespace(
        id=1,
        dateCreated=1700000000,
        transactionType="core:create",
    )
    row = transform_project_transaction_dict(
        transaction=transaction,
        project_id=7,
        project_name="bmo-reviewers",
        author_email=None,
        author_username=None,
        # `core:create` has no prior value; the new value is the project name.
        old_value=[],
        new_value=["bmo-reviewers"],
    )
    assert row["old_value"] == []
    assert row["new_value"] == ["bmo-reviewers"]
    assert row["transaction_type"] == "core:create"


def test_convert_value_to_string_list_true_becomes_one():
    assert convert_value_to_string_list(True) == ["1"], (
        '`convert_value_to_string_list(True)` should return `["1"]` so that '
        "boolean transaction values round-trip into BigQuery's REPEATED column."
    )


def test_convert_value_to_string_list_false_becomes_zero():
    assert convert_value_to_string_list(False) == ["0"], (
        '`convert_value_to_string_list(False)` should return `["0"]` '
        "(the string, not the integer)."
    )


def test_convert_value_to_string_list_str_passes_through():
    assert convert_value_to_string_list("already a string") == ["already a string"], (
        "A string input should be wrapped in a single-element list unchanged."
    )


def test_convert_value_to_string_list_int_stringifies():
    assert convert_value_to_string_list(42) == ["42"], (
        "Integer inputs should be coerced to their decimal string form."
    )


def test_convert_value_to_string_list_none_stringifies():
    assert convert_value_to_string_list(None) == ["None"], (
        "`None` should be coerced via `str(None)` rather than special-cased; "
        "the ETL relies on a non-null value for `oldValue`/`newValue`."
    )


def test_convert_value_to_string_list_empty_string_passes_through():
    assert convert_value_to_string_list("") == [""], (
        "Empty strings should remain empty rather than being coerced to another value."
    )


# ---------------------------------------------------------------------------
# `transform_changeset_dict`
# ---------------------------------------------------------------------------


def test_transform_changeset_dict_decodes_filename_from_bytes():
    changeset = SimpleNamespace(
        id=4242,
        addLines=12,
        delLines=3,
        filename=b"path/to/file.py",
    )

    result = transform_changeset_dict(changeset, revision_id=10, diff_id=99)

    assert result == {
        "revision_id": 10,
        "diff_id": 99,
        "changeset_id": 4242,
        "lines_added": 12,
        "lines_removed": 3,
        "filename": "path/to/file.py",
    }, (
        "`transform_changeset_dict` should map the changeset row's columns "
        "into the BigQuery row dict and UTF-8 decode the byte-string "
        "filename."
    )


def test_transform_changeset_dict_decodes_non_ascii_filename():
    changeset = SimpleNamespace(
        id=1,
        addLines=0,
        delLines=0,
        filename="café.py".encode("utf-8"),
    )

    result = transform_changeset_dict(changeset, revision_id=1, diff_id=1)

    assert result["filename"] == "café.py", (
        "Non-ASCII filenames stored as UTF-8 bytes should round-trip back "
        "to their unicode form."
    )


# ---------------------------------------------------------------------------
# `transform_comment_dict`
# ---------------------------------------------------------------------------


def test_transform_comment_dict_detects_inline_suggestion():
    comment = SimpleNamespace(
        id=7,
        changesetID=33,
        dateCreated=1_700_000_000,
        content="Consider renaming this variable.",
        attributes=json.dumps({"inline.state.initial": {"hassuggestion": "true"}}),
    )

    result = transform_comment_dict(
        comment,
        revision_id=42,
        diff_id=8,
        author_email="alice@example.com",
        author_username="alice",
    )

    assert result == {
        "revision_id": 42,
        "diff_id": 8,
        "changeset_id": 33,
        "comment_id": 7,
        "author_email": "alice@example.com",
        "author_username": "alice",
        "date_created": 1_700_000_000,
        "character_count": len("Consider renaming this variable."),
        "is_suggestion": True,
    }, (
        "A comment whose `attributes.inline.state.initial.hassuggestion` "
        'is the string `"true"` should produce `is_suggestion=True` and '
        "map every other field into the output row."
    )


def test_transform_comment_dict_marks_non_suggestion_as_false():
    comment = SimpleNamespace(
        id=2,
        changesetID=None,
        dateCreated=1,
        content="lgtm",
        attributes=json.dumps({}),
    )

    result = transform_comment_dict(
        comment,
        revision_id=1,
        diff_id=None,
        author_email=None,
        author_username=None,
    )

    assert result["is_suggestion"] is False, (
        "A comment with no `inline.state.initial` block should be "
        "classified as a regular comment (`is_suggestion=False`)."
    )
    assert result["character_count"] == len("lgtm"), (
        "`character_count` should be the length of `comment.content` "
        "regardless of whether the comment is a suggestion."
    )


def test_transform_comment_dict_suggestion_flag_only_true_for_literal_string():
    # Phab stores the suggestion flag as a string, not a bool.
    comment = SimpleNamespace(
        id=3,
        changesetID=1,
        dateCreated=0,
        content="",
        attributes=json.dumps({"inline.state.initial": {"hassuggestion": True}}),
    )

    result = transform_comment_dict(
        comment,
        revision_id=1,
        diff_id=1,
        author_email=None,
        author_username=None,
    )

    assert result["is_suggestion"] is False, (
        "Boolean `True` for `hassuggestion` should not be treated as a "
        "suggestion. The Phabricator schema stores the flag as the literal "
        'string `"true"`, and matching that exactly is what the ETL '
        "promises."
    )


def test_transform_comment_dict_malformed_attributes_raises():
    comment = SimpleNamespace(
        id=1,
        changesetID=None,
        dateCreated=0,
        content="",
        attributes="not-json",
    )

    with pytest.raises(json.JSONDecodeError):
        transform_comment_dict(
            comment,
            revision_id=1,
            diff_id=None,
            author_email=None,
            author_username=None,
        )


# ---------------------------------------------------------------------------
# `transform_transaction_dict`
# ---------------------------------------------------------------------------


def test_transform_transaction_dict_maps_fields_and_stringifies_values():
    transaction = SimpleNamespace(
        id=11,
        transactionType="differential.revision.accept",
        oldValue="0",
        newValue="2",
        dateCreated=1_700_000_000,
    )

    result = transform_transaction_dict(
        transaction,
        revision_id=99,
        author_email="bob@example.com",
        author_username="bob",
    )

    assert result == {
        "revision_id": 99,
        "transaction_id": 11,
        "transaction_type": "differential.revision.accept",
        "author_email": "bob@example.com",
        "author_username": "bob",
        "date_created": 1_700_000_000,
        "old_value": ["0"],
        "new_value": ["2"],
    }, (
        "`transform_transaction_dict` should map every column straight "
        "through, using `convert_value_to_string_list` on the old/new values."
    )


def test_transform_transaction_dict_coerces_boolean_values():
    transaction = SimpleNamespace(
        id=1,
        transactionType="differential.revision.close",
        oldValue=False,
        newValue=True,
        dateCreated=0,
    )

    result = transform_transaction_dict(
        transaction,
        revision_id=1,
        author_email=None,
        author_username=None,
    )

    assert (result["old_value"], result["new_value"]) == (["0"], ["1"]), (
        "Boolean `oldValue`/`newValue` should round-trip through "
        '`convert_value_to_string_list`, becoming `["0"]`/`["1"]`.'
    )


# ---------------------------------------------------------------------------
# `should_include_diff`
# ---------------------------------------------------------------------------


def test_should_include_diff_excludes_commit_method():
    diff = SimpleNamespace(creationMethod="commit", authorPHID=b"PHID-USER-x")
    assert should_include_diff(diff) is False, (
        'Diffs whose `creationMethod` is `"commit"` represent landings '
        "and must be filtered out of the diffs table (they only contribute "
        "to `date_landed`)."
    )


def test_should_include_diff_excludes_repository_identity_author():
    diff = SimpleNamespace(creationMethod="web", authorPHID=b"PHID-RIDT-abc")
    assert should_include_diff(diff) is False, (
        "Diffs authored by a repository identity (`PHID-RIDT-*`) are "
        "bookkeeping artifacts and must be filtered out."
    )


def test_should_include_diff_keeps_normal_diffs():
    diff = SimpleNamespace(creationMethod="web", authorPHID=b"PHID-USER-abc")
    assert should_include_diff(diff) is True, (
        "A web-uploaded diff authored by a regular user should be included."
    )


# ---------------------------------------------------------------------------
# `latest_landed_date`
# ---------------------------------------------------------------------------


def test_latest_landed_date_empty_input_is_none():
    assert latest_landed_date([]) is None, (
        "With no diffs there is no landing date to report."
    )


def test_latest_landed_date_ignores_non_commit_diffs():
    diffs = [
        SimpleNamespace(creationMethod="web", dateCreated=999),
        SimpleNamespace(creationMethod="raw", dateCreated=888),
    ]
    assert latest_landed_date(diffs) is None, (
        'Only `creationMethod == "commit"` diffs feed `date_landed`; '
        "non-commit diffs must not contribute even if they have a later "
        "`dateCreated`."
    )


def test_latest_landed_date_returns_max_of_commit_diffs():
    diffs = [
        SimpleNamespace(creationMethod="commit", dateCreated=100),
        SimpleNamespace(creationMethod="commit", dateCreated=300),
        SimpleNamespace(creationMethod="commit", dateCreated=200),
        SimpleNamespace(creationMethod="web", dateCreated=999),
    ]
    assert latest_landed_date(diffs) == 300, (
        "`latest_landed_date` should return the maximum `dateCreated` "
        "among the commit-method diffs, ignoring the unrelated web diff "
        "even though its date is larger."
    )


# ---------------------------------------------------------------------------
# `transform_review_dict`
# ---------------------------------------------------------------------------


def test_transform_review_dict_maps_user_reviewer_fields():
    review = SimpleNamespace(
        id=55,
        reviewerStatus="accepted",
        dateCreated=1_700_000_000,
        dateModified=1_700_000_500,
    )

    result = transform_review_dict(
        review,
        revision_id=99,
        reviewer_username="alice",
        reviewer_email="alice@example.com",
        is_group=False,
        last_action_diff_id=12,
        last_comment_diff_id=11,
    )

    assert result == {
        "revision_id": 99,
        "review_id": 55,
        "reviewer_username": "alice",
        "reviewer_email": "alice@example.com",
        "is_group": False,
        "date_created": 1_700_000_000,
        "date_modified": 1_700_000_500,
        "status": "accepted",
        "last_action_diff_id": 12,
        "last_comment_diff_id": 11,
    }, (
        "User reviews should map review and reviewer fields straight "
        "into the output row."
    )


def test_transform_review_dict_handles_group_reviewer():
    review = SimpleNamespace(
        id=1,
        reviewerStatus="added",
        dateCreated=0,
        dateModified=0,
    )

    result = transform_review_dict(
        review,
        revision_id=1,
        reviewer_username="conduit-reviewers",
        reviewer_email=None,
        is_group=True,
        last_action_diff_id=None,
        last_comment_diff_id=None,
    )

    assert result["is_group"] is True, (
        "Group reviewers should be flagged with `is_group=True`."
    )
    assert result["reviewer_email"] is None, (
        "Group reviewers have no email; the field should be `None` rather "
        "than missing from the row."
    )


# ---------------------------------------------------------------------------
# `latest_approved_date`
# ---------------------------------------------------------------------------


def test_latest_approved_date_empty_input_is_none():
    assert latest_approved_date([]) is None, (
        "With no reviews there is no approval date."
    )


def test_latest_approved_date_ignores_non_accepted_reviews():
    reviews = [
        SimpleNamespace(reviewerStatus="rejected", dateModified=999),
        SimpleNamespace(reviewerStatus="added", dateModified=888),
        SimpleNamespace(reviewerStatus="commented", dateModified=777),
    ]
    assert latest_approved_date(reviews) is None, (
        "Only `accepted` reviews count toward the approval date; rejected "
        "and pending reviews must not contribute."
    )


def test_latest_approved_date_returns_max_modified_of_accepted():
    reviews = [
        SimpleNamespace(reviewerStatus="accepted", dateModified=100),
        SimpleNamespace(reviewerStatus="accepted", dateModified=300),
        SimpleNamespace(reviewerStatus="rejected", dateModified=999),
        SimpleNamespace(reviewerStatus="accepted", dateModified=200),
    ]
    assert latest_approved_date(reviews) == 300, (
        "The latest `dateModified` among `accepted` reviews wins; the "
        "later `rejected` review must be ignored."
    )


# ---------------------------------------------------------------------------
# `parse_repository_details`
# ---------------------------------------------------------------------------


def test_parse_repository_details_returns_empty_dict_for_none_repo():
    assert parse_repository_details(None) == {}, (
        "A missing repository should produce an empty dict rather than "
        "raising; downstream `.get(...)` calls treat it as no details."
    )


def test_parse_repository_details_returns_empty_dict_for_blank_details():
    target_repo = SimpleNamespace(details=None)
    assert parse_repository_details(target_repo) == {}, (
        "A repository with `details=None` should produce an empty dict."
    )

    target_repo = SimpleNamespace(details="")
    assert parse_repository_details(target_repo) == {}, (
        'A repository with `details=""` (falsy empty string) should '
        'also produce an empty dict; we never call `json.loads("")`.'
    )


def test_parse_repository_details_parses_valid_json():
    target_repo = SimpleNamespace(
        details=json.dumps({"default-branch": "main", "other": 42})
    )
    assert parse_repository_details(target_repo) == {
        "default-branch": "main",
        "other": 42,
    }, "A populated `details` JSON column should be parsed into a dict."


def test_parse_repository_details_raises_on_malformed_json():
    target_repo = SimpleNamespace(details="not-json")
    with pytest.raises(json.JSONDecodeError):
        parse_repository_details(target_repo)
