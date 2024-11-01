#!/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import logging
import os
import pprint
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import Optional, Any

import sqlalchemy
from google.cloud import bigquery
from more_itertools import chunked
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


def diff_phid_to_id(diff_phid: Optional[str], session_diff: Session) -> Optional[int]:
    if diff_phid is None:
        return None

    diff = session_diff.query(DiffDb.Differential).filter_by(phid=diff_phid).one()

    return diff.id


def get_diff_id_for_changeset(
    changeset_id: Optional[int], session_diff: Session
) -> Optional[int]:
    if changeset_id is None:
        return None

    changeset = session_diff.query(DiffDb.Changeset).filter_by(id=changeset_id).one()

    return changeset.diffID


def get_bug_id(revision: DiffDb.Revision, bug_id_query) -> Optional[int]:
    bug_id_query_result = bug_id_query.filter(
        DiffDb.CustomFieldStorage.objectPHID == revision.phid
    ).one_or_none()

    if not bug_id_query_result:
        return None

    return bug_id_query_result.fieldValue


class PhabricatorEdgeConstant(IntEnum):
    """Enumeration of project edge constants."""

    DEPENDS_ON = 5
    DEPENDED_ON = 6
    OBJECT_HAS_PROJECT = 41


def get_revision_projects(
    revision: Any, session_diff: Session, projects_query: Any
) -> list[str]:
    """Return the project tags associated with a revision."""
    # Get all edges between the revision and a project.
    edge_query_result = (
        session_diff.query(DiffDb.Edges)
        .filter(
            DiffDb.Edges.src == revision.phid,
            DiffDb.Edges.type == PhabricatorEdgeConstant.OBJECT_HAS_PROJECT.value,
        )
        .all()
    )

    # Get the PHID of each project (the destination on the edge).
    project_phids = {edge.dst for edge in edge_query_result}

    # Get the project objects for the set of PHIDs.
    projects = projects_query.filter(ProjectDb.Project.phid.in_(project_phids)).all()

    # Convert the project PHID to the slug (name).
    return [project.primarySlug for project in projects]


def get_stack_size(
    revision: Any,
    bug_id: Optional[int],
    all_revisions: Any,
    bug_id_query: Any,
    session_diff: Session,
) -> int:
    # The stack size is always 1 for stacks without a bug ID.
    if not bug_id:
        return 1

    stack = set()
    neighbors = {revision.phid}

    while neighbors:
        # Query for all edges related to the current set of neighbors.
        edge_query_result = (
            session_diff.query(DiffDb.Edges)
            .filter(
                or_(DiffDb.Edges.src.in_(neighbors), DiffDb.Edges.dst.in_(neighbors)),
                DiffDb.Edges.type.in_(
                    [
                        PhabricatorEdgeConstant.DEPENDS_ON.value,
                        PhabricatorEdgeConstant.DEPENDED_ON.value,
                    ]
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
                    neighbor_bug_id = get_bug_id(rev, bug_id_query)
                    if not neighbor_bug_id:
                        continue

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
            session_users.query(UserDb.UserEmail)
            .filter_by(userPHID=author_phid, isPrimary=1)
            .one()
        )
        return user_email.address
    except NoResultFound:
        return None


def get_review_requests(
    revision: DiffDb.Revision,
    session_diff: Session,
    session_projects: Session,
    session_users: Session,
) -> tuple[list[dict], Optional[datetime]]:
    review_requests = []
    date_approved = None

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
            reviewer_username = reviewer.name
            reviewer_email = None
        else:
            reviewer_username = get_user_name(review.reviewerPHID, session_users)
            reviewer_email = get_user_email(review.reviewerPHID, session_users)

        # Set `date_approved` as the latest `accepted` review modified time.
        if review.reviewerStatus == "accepted" and (
            not date_approved or date_approved < review.dateModified
        ):
            date_approved = review.dateModified

        review_obj = {
            "revision_id": revision.id,
            "review_id": review.id,
            "reviewer_username": reviewer_username,
            "reviewer_email": reviewer_email,
            "is_group": is_reviewer_group,
            "date_created": datetime.fromtimestamp(review.dateCreated),
            "date_modified": datetime.fromtimestamp(review.dateModified),
            "status": review.reviewerStatus,
            "last_action_diff_id": diff_phid_to_id(
                review.lastActionDiffPHID, session_diff
            ),
            "last_comment_diff_id": diff_phid_to_id(
                review.lastCommentDiffPHID, session_diff
            ),
        }

        review_requests.append(review_obj)

    if date_approved:
        date_approved = datetime.fromtimestamp(date_approved)

    return review_requests, date_approved


def get_diffs_changesets(
    revision: DiffDb.Revision,
    session_diff: Session,
    session_users: Session,
) -> tuple[list[dict], list[dict], Optional[datetime]]:
    diffs = []
    changesets = []
    date_landed = None
    for diff in session_diff.query(DiffDb.Differential).filter_by(
        revisionID=revision.id
    ):
        if diff.creationMethod == "commit":
            # Set `date_landed` as the latest `commit` diff creation time.
            if not date_landed or date_landed < diff.dateCreated:
                date_landed = diff.dateCreated

            continue

        if diff.authorPHID.startswith(b"PHID-RIDT-"):
            # Ignore diffs that were created with repository identity.
            continue

        diff_obj = {
            "diff_id": diff.id,
            "revision_id": revision.id,
            "date_created": datetime.fromtimestamp(diff.dateCreated),
            "author_email": get_user_email(diff.authorPHID, session_users),
            "author_username": get_user_name(diff.authorPHID, session_users),
        }

        diffs.append(diff_obj)
        changesets.extend(get_changesets(revision, diff, session_diff))

    if date_landed:
        date_landed = datetime.fromtimestamp(date_landed)

    return diffs, changesets, date_landed


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
            "filename": changeset.filename.decode("utf-8"),
        }

        changesets.append(changeset_obj)

    return changesets


def get_comments(
    revision: DiffDb.Revision, session_diff: Session, session_users: Session
) -> list[dict]:
    comments = []

    # Query comments that are left on revisions but not specific diffs/changesets.
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
        # Query all TransactionComments that match our revision PHID
        # or the non-diff comments.
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
            "comment_id": comment.id,
            "author_email": get_user_email(comment.authorPHID, session_users),
            "author_username": get_user_name(comment.authorPHID, session_users),
            "date_created": datetime.fromtimestamp(comment.dateCreated),
            "character_count": len(comment.content),
            "is_suggestion": is_suggestion,
        }

        comments.append(comment_obj)

    return comments


def get_last_run_timestamp(bq_client: bigquery.Client) -> Optional[datetime]:
    """Get the timestamp of the most recently added entry in BigQuery.

    See https://github.com/googleapis/python-bigquery/blob/main/samples/query_script.py
    for more.
    """
    most_recent_run_sql = (
        f"SELECT MAX(date_modified) as last_run FROM `{BQ_REVISIONS_TABLE_ID}`"
    )

    parent_job = bq_client.query(most_recent_run_sql)
    rows = list(parent_job.result())

    if len(rows) != 1:
        logging.error("Only one row should be returned by timestamp query.")
        sys.exit(1)

    # The `last_run` field comes from the SQL query above.
    return rows[0].last_run


def load_bigquery_tables(
    bq_client: bigquery.Client,
) -> dict[str, bigquery.Table]:
    """Return a mapping of each table ID to the table field->type schema."""
    return {
        BQ_REVISIONS_TABLE_ID: bq_client.get_table(BQ_REVISIONS_TABLE_ID),
        BQ_DIFFS_TABLE_ID: bq_client.get_table(BQ_DIFFS_TABLE_ID),
        BQ_CHANGESETS_TABLE_ID: bq_client.get_table(BQ_CHANGESETS_TABLE_ID),
        BQ_COMMENTS_TABLE_ID: bq_client.get_table(BQ_COMMENTS_TABLE_ID),
        BQ_REVIEW_REQUESTS_TABLE_ID: bq_client.get_table(BQ_REVIEW_REQUESTS_TABLE_ID),
    }


def create_staging_tables(
    bq_client: bigquery.Client, tables: dict[str, bigquery.Table]
) -> dict[str, bigquery.Table]:
    """Create the staging tables for data insertion.

    Returns a mapping of the target table ID to the `Table` object for the
    staging table.
    """
    return {
        table.table_id: bq_client.create_table(
            staging_table_id(table.table_id),
            schema=table.schema,
            exists_ok=True,
        )
        for table in tables.values()
    }


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


BIGQUERY_TYPE_MAPPING = {
    "STRING": "STRING",
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIME": "TIME",
}


def staging_table_id(table_id: str) -> str:
    """Return a staging table ID for the given table ID."""
    return f"{table_id}_staging"


def merge_into_bigquery(
    bq_client: bigquery.Client,
    table_id: str,
    staging_table_id: str,
    id_column: str,
    bq_tables: dict[str, bigquery.Table],
):
    """Use a `MERGE` statement to upsert rows into BigQuery.

    Perform a `MERGE` statement to detect if an entry in the table has the matching ID.
    If there is a matching ID, update the existing entry with the new data.
    If there is no matching ID, insert a new row.
    """
    target_table = bq_tables[table_id]

    merge_query = f"""
        MERGE `{table_id}` as T
        USING `{staging_table_id}` as S
        ON T.{id_column} = S.{id_column}
        WHEN MATCHED THEN
          UPDATE SET {", ".join(f"{field.name} = S.{field.name}" for field in target_table.schema)}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(field.name for field in target_table.schema)})
          VALUES ({", ".join(f"S.{field.name}" for field in target_table.schema)});
    """

    merge_job = bq_client.query(merge_query)
    merge_job.result()

    logging.info(f"Merged staging table for {table_id}.")


def delete_staging_table(bq_client: bigquery.Client, table_id: str):
    """Delete the table with given ID."""
    bq_client.delete_table(table_id)
    logging.info(f"Deleted table {table_id}.")


def submit_to_bigquery(
    bq_client: bigquery.Client,
    table: bigquery.Table,
    rows: list[dict],
):
    if not rows:
        return

    # Insert rows into staging table.
    for chunk in chunked(rows, 500):
        errors = bq_client.insert_rows_json(table.table_id, chunk)
        if errors:
            logging.error(
                "Encountered errors while inserting rows to "
                f"{table.table_id}: {errors}."
            )
            sys.exit(1)


def process():
    now = datetime.now()

    logging.info(f"Starting Phab-ETL with timestamp {now}.")

    session_users = Session(engines["user"])
    session_projects = Session(engines["project"])
    session_repo = Session(engines["repository"])
    session_diff = Session(engines["differential"])

    bq_client = bigquery.Client()

    target_tables = load_bigquery_tables(bq_client)
    staging_tables = create_staging_tables(bq_client, target_tables)

    time_queries = get_time_queries(now, bq_client)

    updated_revisions = session_diff.query(DiffDb.Revision).filter(*time_queries)
    all_revisions = session_diff.query(DiffDb.Revision)

    projects_query = session_projects.query(ProjectDb.Project)

    bug_id_query = session_diff.query(DiffDb.CustomFieldStorage).filter(
        # TODO I got this value from the DB, what is it?
        DiffDb.CustomFieldStorage.fieldIndex
        == b"zdMFYM6423ua"
    )

    logging.info(f"Found {updated_revisions.count()} revisions for processing.")

    for revision in updated_revisions:
        logging.info(f"Processing revision D{revision.id}.")

        phab_querying_start = time.perf_counter()

        bug_id = get_bug_id(revision, bug_id_query)

        diffs, changesets, date_landed = get_diffs_changesets(
            revision,
            session_diff,
            session_users,
        )

        review_requests, date_approved = get_review_requests(
            revision, session_diff, session_projects, session_users
        )

        revision_json = {
            "bug_id": bug_id,
            "revision_id": revision.id,
            # Set `date_approved` only when a landing has been detected.
            "date_approved": date_approved if date_landed else None,
            "date_created": datetime.fromtimestamp(revision.dateCreated),
            "date_modified": datetime.fromtimestamp(revision.dateModified),
            "date_landed": date_landed,
            "last_review_id": get_last_review_id(revision.phid, session_diff),
            "current_status": revision.status,
            "target_repository": get_target_repository(
                revision.repositoryPHID, session_repo
            ),
            "stack_size": get_stack_size(
                revision, bug_id, all_revisions, bug_id_query, session_diff
            ),
            "project_tags": get_revision_projects(
                revision, session_diff, projects_query
            ),
        }

        comments = get_comments(revision, session_diff, session_users)

        phab_gathering_time = round(
            time.perf_counter() - phab_querying_start, ndigits=2
        )
        logging.info(
            f"Gathered relevant info for D{revision.id} in {phab_gathering_time}s."
        )

        if DEBUG:
            pprint.pprint(revision_json)
            continue

        bigquery_insert_start = time.perf_counter()

        # Send data to BigQuery staging tables.
        for target_table_id, data in (
            (BQ_REVISIONS_TABLE_ID, [revision_json]),
            (BQ_DIFFS_TABLE_ID, diffs),
            (BQ_CHANGESETS_TABLE_ID, changesets),
            (BQ_REVIEW_REQUESTS_TABLE_ID, review_requests),
            (BQ_COMMENTS_TABLE_ID, comments),
        ):
            submit_to_bigquery(bq_client, staging_tables[target_table_id], data)

        bigquery_insert_time = round(
            time.perf_counter() - bigquery_insert_start, ndigits=2
        )
        logging.info(
            f"Submitted revision D{revision.id} in BigQuery staging "
            f"in {bigquery_insert_time}s."
        )

    # Merge staging table changes into target tables.
    for target_table_id, id_column in (
        (BQ_REVISIONS_TABLE_ID, "revision_id"),
        (BQ_DIFFS_TABLE_ID, "diff_id"),
        (BQ_CHANGESETS_TABLE_ID, "changeset_id"),
        (BQ_REVIEW_REQUESTS_TABLE_ID, "review_id"),
        (BQ_COMMENTS_TABLE_ID, "comment_id"),
    ):
        staging_table_id = staging_tables[target_table_id].table_id
        merge_into_bigquery(
            bq_client,
            target_table_id,
            staging_table_id,
            id_column,
            target_tables,
        )

        delete_staging_table(bq_client, staging_table_id)


if __name__ == "__main__":
    process()
