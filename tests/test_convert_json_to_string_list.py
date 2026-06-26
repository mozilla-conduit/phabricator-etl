# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Tests for `convert_json_to_string_list`, which resolves the JSON PHID->status
maps stored on `differential.revision.reviewers` transactions into a list of
reviewer names.

The fixtures below use real `oldValue`/`newValue` strings observed in the
Phabricator `differential_transaction` table. The reviewer name lookups are
backed by `Sessions.user_name_cache` / `Sessions.project_name_cache`, so the
tests pre-populate those caches and never touch the database.
"""

import logging
from types import SimpleNamespace

import pytest

from phabricator_etl import stats

# Names keyed by the PHIDs that appear in the sample data below. Keys are
# bytes because `convert_json_to_string_list` encodes each PHID before lookup.
USER_NAMES = {
    b"PHID-USER-m4uq3un4hnoj4j35o7xc": "alice",
    b"PHID-USER-zcy2d375tpinr3tfxa3d": "bob",
    b"PHID-USER-qsqyz242anhk5zywnmik": "carol",
    b"PHID-USER-n77lkspun6bajdj6gwx4": "dave",
    b"PHID-USER-2ywvyh74pb65x3jcj6n3": "erin",
    b"PHID-USER-myoawqp3kpkeeedt764l": "frank",
    # A known PHID that resolves to no name (e.g. lookup returned NoResultFound).
    b"PHID-USER-unknownnnnnnnnnnnnnn": None,
}
PROJECT_NAMES = {
    b"PHID-PROJ-rotationgroupxxxxxxx": "sheriffs",
}


@pytest.fixture
def sessions():
    """A `Sessions` stand-in carrying only the pre-populated name caches.

    Every PHID used by the tests is present in the caches, so the name-lookup
    helpers (`get_user_name` / `get_project_name`) short-circuit on the cache
    before issuing any query. Only `user_name_cache` and `project_name_cache`
    are touched on that path, so a `SimpleNamespace` with those two attributes
    is a sufficient (DB-free) double for `Sessions`.
    """
    return SimpleNamespace(
        user_name_cache=dict(USER_NAMES),
        project_name_cache=dict(PROJECT_NAMES),
    )


# --- value -> output -----------------------------------------------------
#
# Each case feeds a stored `oldValue`/`newValue` string through the function
# and asserts the list of reviewer names it produces. The empty/fallback
# inputs that resolve to `[]` live here too; their *logging* behaviour is
# exercised separately below.


@pytest.mark.parametrize(
    "value, expected, intent",
    [
        pytest.param(
            '{"PHID-USER-m4uq3un4hnoj4j35o7xc":"added"}',
            ["alice"],
            "a single reviewer PHID resolves to its user name",
            id="single-reviewer",
        ),
        pytest.param(
            '{"PHID-USER-qsqyz242anhk5zywnmik":"added",'
            '"PHID-USER-n77lkspun6bajdj6gwx4":"added"}',
            ["carol", "dave"],
            "multiple reviewers are listed in the JSON key order",
            id="multiple-reviewers-preserve-order",
        ),
        pytest.param(
            '{"PHID-USER-m4uq3un4hnoj4j35o7xc":"accepted"}',
            ["alice"],
            "only the PHID keys matter; the status value is not surfaced",
            id="status-value-is-ignored",
        ),
        pytest.param(
            '{"PHID-USER-2ywvyh74pb65x3jcj6n3":"added",'
            '"PHID-USER-n77lkspun6bajdj6gwx4":"added",'
            '"PHID-USER-m4uq3un4hnoj4j35o7xc":"added",'
            '"PHID-USER-myoawqp3kpkeeedt764l":"added"}',
            ["erin", "dave", "alice", "frank"],
            "four reviewers all resolve and stay in key order",
            id="four-reviewers",
        ),
        pytest.param(
            '{"PHID-PROJ-rotationgroupxxxxxxx":"added"}',
            ["sheriffs"],
            "PHID-PROJ-* keys resolve via the project cache, not the user cache",
            id="project-phid-resolves-via-project-cache",
        ),
        pytest.param(
            '{"PHID-USER-m4uq3un4hnoj4j35o7xc":"added",'
            '"PHID-USER-unknownnnnnnnnnnnnnn":"added"}',
            ["alice"],
            "a PHID resolving to None is dropped from the output list",
            id="unresolved-phid-is-dropped",
        ),
        pytest.param(
            "[]",
            [],
            "Phabricator's empty-reviewers `[]` becomes an empty list",
            id="empty-list",
        ),
        pytest.param(
            "not json at all",
            [],
            "unparseable input falls back to an empty list",
            id="malformed-json",
        ),
        pytest.param(
            None,
            [],
            "non-string input (json.loads raises TypeError) falls back to []",
            id="non-string-input",
        ),
    ],
)
def test_value_converts_to_reviewer_names(sessions, value, expected, intent):
    assert stats.convert_json_to_string_list(value, sessions) == expected, intent


# --- warning vs. no-warning ----------------------------------------------
#
# Every case still resolves to `[]`. They differ only in whether the function
# logs a warning: expected/fallback inputs stay quiet, while a non-empty,
# non-dict JSON value is logged for visibility. `extra_substring`, when set,
# is an additional fragment expected in the log (e.g. the offending type).


@pytest.mark.parametrize(
    "value, expects_warning, extra_substring, intent",
    [
        pytest.param(
            "[]",
            False,
            None,
            "the expected empty-reviewers `[]` resolves to [] without warning",
            id="empty-list-no-warning",
        ),
        pytest.param(
            "not json at all",
            False,
            None,
            "malformed JSON resolves to [] without warning",
            id="malformed-json-no-warning",
        ),
        pytest.param(
            None,
            False,
            None,
            "non-string input resolves to [] without warning",
            id="non-string-no-warning",
        ),
        pytest.param(
            '["PHID-USER-m4uq3un4hnoj4j35o7xc"]',
            True,
            "list",
            "an unexpected non-empty JSON list resolves to [] and is logged",
            id="unexpected-list-warns",
        ),
        pytest.param(
            "42",
            True,
            None,
            "an unexpected JSON scalar resolves to [] and is logged",
            id="unexpected-scalar-warns",
        ),
    ],
)
def test_warning_emitted_only_for_unexpected_json(
    sessions, caplog, value, expects_warning, extra_substring, intent
):
    with caplog.at_level(logging.WARNING):
        assert stats.convert_json_to_string_list(value, sessions) == [], intent

    if not expects_warning:
        assert caplog.records == [], intent
        return

    assert len(caplog.records) == 1, intent
    assert "Unexpected non-dict reviewers value" in caplog.text, intent
    if extra_substring is not None:
        assert extra_substring in caplog.text, intent
