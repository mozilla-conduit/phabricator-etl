# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
from types import SimpleNamespace

from phabricator_etl.transforms import (
    PROJECT_HAS_MEMBER_EDGE_TYPE,
    decode_name_transaction_value,
    is_membership_edge_transaction,
    parse_edge_member_phids,
    transform_project_transaction_dict,
)


def test_is_membership_edge_transaction_true_for_member_edge():
    metadata = json.dumps({"edge:type": PROJECT_HAS_MEMBER_EDGE_TYPE})
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
    assert decode_name_transaction_value(json.dumps("My Project")) == "My Project"
    assert decode_name_transaction_value(None) is None
    assert decode_name_transaction_value("") is None
    assert decode_name_transaction_value("null") is None


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
        old_value="bob",
        new_value="carol",
    )
    assert row == {
        "project_id": 7,
        "project_name": "bmo-reviewers",
        "transaction_id": 99,
        "author_email": "alice@example.com",
        "author_username": "alice",
        "date_created": 1700000000,
        "transaction_type": "core:edge",
        "old_value": "bob",
        "new_value": "carol",
    }


def test_transform_project_transaction_dict_create_has_null_values():
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
        old_value=None,
        new_value=None,
    )
    assert row["old_value"] is None
    assert row["new_value"] is None
    assert row["transaction_type"] == "core:create"
