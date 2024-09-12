#!/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import logging
import os
import pprint
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

import sqlalchemy
from google.cloud import bigquery
from sqlalchemy import desc, or_
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

BQ_REVISIONS_TABLE_ID = os.environ["BQ_REVISIONS_TABLE_ID"]
BQ_DIFFS_TABLE_ID = os.environ["BQ_DIFFS_TABLE_ID"]
BQ_CHANGESETS_TABLE_ID = os.environ["BQ_CHANGESETS_TABLE_ID"]
BQ_COMMENTS_TABLE_ID = os.environ["BQ_COMMENTS_TABLE_ID"]
BQ_REVIEW_REQUESTS_TABLE_ID = os.environ["BQ_REVIEW_REQUESTS_TABLE_ID"]

DEBUG = "DEBUG" in os.environ
PHAB_DB_URL = os.environ.get("PHAB_URL", "127.0.0.1")
PHAB_DB_NAMESPACE = os.environ.get("PHAB_NAMESPACE", "bitnami_phabricator")
PHAB_DB_PORT = os.environ.get("PHAB_PORT", "3307")
PHAB_DB_USER = os.environ.get("PHAB_USER", "root")
PHAB_DB_TOKEN = os.environ["PHAB_TOKEN"]

# Configure simple logging.
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    stream=sys.stdout,
)


def create_engine(table_suffix: str) -> Engine:
    return sqlalchemy.create_engine(
        f"mysql+mysqldb://{PHAB_DB_USER}:{PHAB_DB_TOKEN}@{PHAB_DB_URL}:{PHAB_DB_PORT}/{PHAB_DB_NAMESPACE}_{table_suffix}"
    )


def create_engines() -> dict[str, Engine]:
    return {
        "user": create_engine("user"),
        "project": create_engine("project"),
        "repository": create_engine("repository"),
        "differential": create_engine("differential"),
    }


def prepare_bases(engines: dict[str, Engine]) -> dict[str, Any]:
    Base = automap_base()
    bases = {}
    for key, engine in engines.items():
        Base.prepare(engine)
        bases[key] = Base
    return bases


engines = create_engines()
bases = prepare_bases(engines)


@dataclass
class UserDb:
    User = bases["user"].classes.user
    UserEmail = bases["user"].classes.user_email


@dataclass
class ProjectDb:
    Project = bases["project"].classes.project


@dataclass
class RepoDb:
    Repository = bases["repository"].classes.repository_uri


@dataclass
class DiffDb:
    Revision = bases["differential"].classes.differential_revision
    Differential = bases["differential"].classes.differential_diff
    Changeset = bases["differential"].classes.differential_changeset
    Transaction = bases["differential"].classes.differential_transaction
    TransactionComment = bases["differential"].classes.differential_transaction_comment
    Reviewer = bases["differential"].classes.differential_reviewer
    Edges = bases["differential"].classes.edge
    CustomFieldStorage = bases["differential"].classes.differential_customfieldstorage


def get_last_review_id(revision_phid: str, session_diff: Session) -> Optional[int]:
    last_review = (
        session_diff.query(DiffDb.Reviewer)
        .filter_by(revisionPHID=revision_phid)
        .order_by(desc("dateModified"))
        .first()
    )
    return last_review.id if last_review else None


def get_target_repository(repository_phid: str, session_repo: Session) -> Optional[str]:
    repository = (
        session_repo.query(RepoDb.Repository)
        .filter_by(repositoryPHID=repository_phid)
        .first()
    )
    return repository.uri if repository else None


def diff_phid_to_id(diff_phid: str, session_diff: Session) -> int:
    diff = session_diff.query(DiffDb.Differential).filter_by(phid=diff_phid).one()

    return diff.id


def get_diff_id_for_changeset(
    changeset_id: Optional[int], session_diff: Session
) -> Optional[int]:
    if changeset_id is None:
        return None

    changeset = session_diff.query(DiffDb.Changeset).filter_by(id=changeset_id).one()

    return changeset.diffID


PHAB_DEPENDS_ON_EDGE_CONST = 5
PHAB_DEPENDED_ON_EDGE_CONST = 6


def get_stack_size(
    revision: Any, all_revisions: Any, bug_id_query: Any, session_diff: Session
) -> int:
    stack = set()
    neighbors = {revision.phid}

    bug_id_query_result = bug_id_query.filter(
        DiffDb.CustomFieldStorage.objectPHID == revision.phid
    ).one_or_none()

    if not bug_id_query_result:
        return 1

    bug_id = bug_id_query_result.fieldValue

    while neighbors:
        # Query for all edges related to the current set of neighbors.
        edge_query_result = (
            session_diff.query(DiffDb.Edges)
            .filter(
                or_(DiffDb.Edges.src.in_(neighbors), DiffDb.Edges.dst.in_(neighbors)),
                DiffDb.Edges.type.in_(
                    [PHAB_DEPENDS_ON_EDGE_CONST, PHAB_DEPENDED_ON_EDGE_CONST]
                ),
            )
            .all()
        )

        # Create an empty list of revisions that have this as the bug.
        bug_matching_revlist = []
        # For each edge related to our current set of neighbors.
        for edge in edge_query_result:
            # For each end of the edge.
            for node_phid in (edge.src, edge.dst):
                # Get the revision from the set of revisions.
                for rev in all_revisions.filter_by(phid=node_phid):
                    # Get the bug ID for the neighbor.
                    neighbor_bug_id_query_result = bug_id_query.filter(
                        DiffDb.CustomFieldStorage.objectPHID == rev.phid
                    ).one_or_none()

                    if not neighbor_bug_id_query_result:
                        continue

                    neighbor_bug_id = neighbor_bug_id_query_result.fieldValue

                    if neighbor_bug_id == bug_id:
                        bug_matching_revlist.append(rev.phid)

        # Add neighbors to the stack.
        stack.update(neighbors)

        # Re-set neighbors to just the items in the bug match revlist that aren't in the stack.
        neighbors = set(bug_matching_revlist) - stack

    return len(stack)


def get_user_name(author_phid: str, session_users: Session) -> Optional[str]:
    try:
        user = session_users.query(UserDb.User).filter_by(phid=author_phid).one()
        return user.userName
    except NoResultFound:
        return None


def get_user_email(author_phid: str, session_users: Session) -> Optional[str]:
    try:
        user_email = (
            session_users.query(UserDb.UserEmail).filter_by(userPHID=author_phid).one()
        )
        return user_email.address
    except NoResultFound:
        return None


def get_review_requests(
    revision: DiffDb.Revision,
    session_diff: Session,
    session_projects: Session,
    session_users: Session,
) -> list[dict]:
    review_requests = []
    for review in session_diff.query(DiffDb.Reviewer).filter_by(
        revisionPHID=revision.phid
    ):
        is_reviewer_group = review.reviewerPHID.startswith(b"PHID-PROJ-")
        if is_reviewer_group:
            reviewer = (
                session_projects.query(ProjectDb.Project)
                .filter_by(phid=review.reviewerPHID)
                .one()
            )
            reviewer_name = reviewer.name
            reviewer_email = None
        else:
            reviewer_name = get_user_name(review.reviewerPHID, session_users)
            reviewer_email = get_user_email(review.reviewerPHID, session_users)

        review_obj = {
            "revision_id": revision.id,
            "reviewer_name": reviewer_name,
            "reviewer_email": reviewer_email,
            "is_group": is_reviewer_group,
            "date_created": review.dateCreated,
            "date_modified": review.dateModified,
            "status": review.reviewerStatus,
            "last_action_diff_id": diff_phid_to_id(
                review.lastActionDiffPHID, session_diff
            ),
            "last_comment_diff_id": diff_phid_to_id(
                review.lastCommentDiffPHID, session_diff
            ),
        }

        review_requests.append(review_obj)

    return review_requests


def get_diffs_changesets(
    revision: DiffDb.Revision,
    session_diff: Session,
    session_users: Session,
) -> tuple[list[dict], list[dict]]:
    diffs = []
    changesets = []
    for diff in session_diff.query(DiffDb.Differential).filter_by(
        revisionID=revision.id
    ):
        if diff.authorPHID == b"PHID-APPS-PhabricatorDiffusionApplication":
            # Ignore diffs that were created as a result of the commit landing.
            continue

        if diff.authorPHID.startswith(b"PHID-RIDT-"):
            # Ignore diffs that were created with repository identity.
            continue

        diff_obj = {
            "diff_id": diff.id,
            "revision_id": revision.id,
            "date_created": diff.dateCreated,
            "author_email": get_user_email(diff.authorPHID, session_users),
            "author_username": get_user_name(diff.authorPHID, session_users),
        }

        diffs.append(diff_obj)
        changesets.extend(get_changesets(revision, diff, session_diff))

    return diffs, changesets


def get_changesets(
    revision: DiffDb.Revision, diff: DiffDb.Differential, session_diff: Session
) -> list[dict]:
    changesets = []
    for changeset in session_diff.query(DiffDb.Changeset).filter_by(diffID=diff.id):
        changeset_obj = {
            "revision_id": revision.id,
            "diff_id": diff.id,
            "changeset_id": changeset.id,
            "lines_added": changeset.addLines,
            "lines_removed": changeset.delLines,
            "filename": changeset.filename,
        }

        changesets.append(changeset_obj)

    return changesets


def get_comments(
    revision: DiffDb.Revision, session_diff: Session, session_users: Session
) -> list[dict]:
    comments = []

    comment_transaction_phids_query = (
        session_diff.query(DiffDb.Transaction)
        .with_entities(DiffDb.Transaction.commentPHID)
        .filter_by(
            objectPHID=revision.phid,
            transactionType="core:comment",
        )
        .all()
    )

    comment_transaction_phids = [row[0] for row in comment_transaction_phids_query]

    for comment in session_diff.query(DiffDb.TransactionComment).filter(
        (DiffDb.TransactionComment.revisionPHID == revision.phid)
        | (DiffDb.TransactionComment.phid.in_(comment_transaction_phids))
    ):
        att = json.loads(comment.attributes)
        is_suggestion = (
            "inline.state.initial" in att
            and att["inline.state.initial"].get("hassuggestion") == "true"
        )

        comment_obj = {
            "revision_id": revision.id,
            "diff_id": get_diff_id_for_changeset(comment.changesetID, session_diff),
            "changeset_id": comment.changesetID,
            "author_email": get_user_email(comment.authorPHID, session_users),
            "author_username": get_user_name(comment.authorPHID, session_users),
            "date_created": comment.dateCreated,
            "character_count": len(comment.content),
            "is_suggestion": is_suggestion,
        }

        comments.append(comment_obj)

    return comments


def get_last_run_timestamp(bq_client: bigquery.Client) -> Optional[int]:
    """Get the timestamp of the most recently added entry in BigQuery.

    See https://github.com/googleapis/python-bigquery/blob/main/samples/query_script.py
    for more.
    """
    if DEBUG:
        return None

    # TODO write SQL to query the latest timestamp in BQ.
    most_recent_run_sql = f"SELECT MAX(date_modified) FROM `{BQ_REVISIONS_TABLE_ID}`"

    # TODO is this the correct way to query?
    parent_job = bq_client.query(most_recent_run_sql)
    rows = list(parent_job.result())

    if len(rows) != 1:
        logging.error("Only one row should be returned by timestamp query.")
        sys.exit(1)

    # TODO is this right?
    return rows[0]


def get_time_queries(now: datetime, bq_client: bigquery.Client) -> list:
    """
    Dont take the revisions created or modified after the start of the run
    Dont take the revisions created before the last run
    """
    queries = [
        DiffDb.Revision.dateCreated < now.timestamp(),
        DiffDb.Revision.dateModified < now.timestamp(),
    ]
    last_run_timestamp = get_last_run_timestamp(bq_client)

    if last_run_timestamp:
        logging.info(f"Using {last_run_timestamp} as the last run timestamp.")
        queries.extend(
            [
                DiffDb.Revision.dateCreated > last_run_timestamp,
                DiffDb.Revision.dateModified > last_run_timestamp,
            ]
        )
    else:
        logging.info("No last run found, starting from the beginning.")

    return queries


def submit_to_bigquery(bq_client: bigquery.Client, table_id: str, rows: list[dict]):
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        logging.error(f"Encountered errors while inserting rows to BigQuery: {errors}.")
        sys.exit(1)


def process():
    now = datetime.now()

    logging.info(f"Starting Phab-ETL with timestamp {now}.")

    session_users = Session(engines["user"])
    session_projects = Session(engines["project"])
    session_repo = Session(engines["repository"])
    session_diff = Session(engines["differential"])

    bq_client = bigquery.Client()

    time_queries = get_time_queries(now, bq_client)

    updated_revisions = session_diff.query(DiffDb.Revision).filter(*time_queries)
    all_revisions = session_diff.query(DiffDb.Revision)

    bug_id_query = session_diff.query(DiffDb.CustomFieldStorage).filter(
        # TODO I got this value from the DB, what is it?
        DiffDb.CustomFieldStorage.fieldIndex
        == b"zdMFYM6423ua"
    )

    logging.info(f"Found {updated_revisions.count()} revisions for processing.")

    for revision in updated_revisions:
        logging.info(f"Processing revision D{revision.id}.")

        revision_json = {
            "revision_id": revision.id,
            "date_created": revision.dateCreated,
            "date_modified": revision.dateModified,
            "last_review_id": get_last_review_id(revision.phid, session_diff),
            "current_status": revision.status,
            "target_repository": get_target_repository(
                revision.repositoryPHID, session_repo
            ),
            "stack_size": get_stack_size(
                revision, all_revisions, bug_id_query, session_diff
            ),
        }

        diffs, changesets = get_diffs_changesets(
            revision,
            session_diff,
            session_users,
        )

        review_requests = get_review_requests(
            revision, session_diff, session_projects, session_users
        )

        comments = get_comments(revision, session_diff, session_users)

        if DEBUG:
            pprint.pprint(revision_json)
            continue

        # Send data to BigQuery.
        submit_to_bigquery(bq_client, BQ_REVISIONS_TABLE_ID, [revision_json])
        submit_to_bigquery(bq_client, BQ_DIFFS_TABLE_ID, diffs)
        submit_to_bigquery(bq_client, BQ_CHANGESETS_TABLE_ID, changesets)
        submit_to_bigquery(bq_client, BQ_REVIEW_REQUESTS_TABLE_ID, review_requests)
        submit_to_bigquery(bq_client, BQ_COMMENTS_TABLE_ID, comments)

        logging.info(f"Submitted revision D{revision.id} in BigQuery.")


if __name__ == "__main__":
    process()
