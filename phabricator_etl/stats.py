#!/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from __future__ import annotations

import argparse
import itertools
import logging
import os
import pprint
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Optional

import sqlalchemy
from google.cloud import bigquery
from more_itertools import chunked
from sqlalchemy import desc, or_
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from phabricator_etl.transforms import (
    PhabricatorEdgeConstant,
    decode_name_transaction_value,
    is_membership_edge_transaction,
    latest_approved_date,
    latest_landed_date,
    parse_edge_member_phids,
    parse_repository_details,
    should_include_diff,
    transform_changeset_dict,
    transform_comment_dict,
    transform_project_transaction_dict,
    transform_review_dict,
    transform_transaction_dict,
)

# Differential transaction types tracked in the transactions table.
STATE_CHANGE_TYPES = [
    "differential.revision.abandon",
    "differential.revision.accept",
    "differential.revision.close",
    "differential.revision.commandeer",
    "differential.revision.reclaim",
    "differential.revision.reject",
    "differential.revision.reopen",
    "differential.revision.request",
    "differential.revision.resign",
    "differential.revision.status",
    "differential.revision.void",
    "differential.revision.wrong",
]

# Project transaction types tracked in the project transactions table:
# project creation, renames, and membership (`core:edge`) changes.
PROJECT_TRANSACTION_TYPES = [
    "core:create",
    "core:edge",
    "project:name",
]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    stream=sys.stdout,
)


@dataclass(frozen=True)
class Config:
    """Runtime configuration sourced from the environment."""

    bq_revisions_table_id: str
    bq_diffs_table_id: str
    bq_changesets_table_id: str
    bq_comments_table_id: str
    bq_review_requests_table_id: str
    bq_transactions_table_id: str
    bq_review_groups_table_id: str
    bq_project_transactions_table_id: str

    phab_db_url: str
    phab_db_namespace: str
    phab_db_port: str
    phab_db_user: str
    phab_db_token: str

    debug: bool


def get_config() -> Config:
    """Read the environment and return the resolved `Config`.

    Called once per run by `process`; the result is threaded through
    the rest of the codebase as an explicit parameter.
    """
    return Config(
        bq_revisions_table_id=os.environ["BQ_REVISIONS_TABLE_ID"],
        bq_diffs_table_id=os.environ["BQ_DIFFS_TABLE_ID"],
        bq_changesets_table_id=os.environ["BQ_CHANGESETS_TABLE_ID"],
        bq_comments_table_id=os.environ["BQ_COMMENTS_TABLE_ID"],
        bq_review_requests_table_id=os.environ["BQ_REVIEW_REQUESTS_TABLE_ID"],
        bq_transactions_table_id=os.environ["BQ_TRANSACTIONS_TABLE_ID"],
        bq_review_groups_table_id=os.environ["BQ_REVIEW_GROUPS_TABLE_ID"],
        bq_project_transactions_table_id=os.environ["BQ_PROJECT_TRANSACTIONS_TABLE_ID"],
        phab_db_url=os.environ.get("PHAB_URL", "127.0.0.1"),
        phab_db_namespace=os.environ.get("PHAB_NAMESPACE", "bitnami_phabricator"),
        phab_db_port=os.environ.get("PHAB_PORT", "3307"),
        phab_db_user=os.environ.get("PHAB_USER", "root"),
        phab_db_token=os.environ["PHAB_TOKEN"],
        debug="DEBUG" in os.environ,
    )


def create_engine(config: Config, table_suffix: str) -> Engine:
    return sqlalchemy.create_engine(
        f"mysql+mysqldb://{config.phab_db_user}:{config.phab_db_token}"
        f"@{config.phab_db_url}:{config.phab_db_port}"
        f"/{config.phab_db_namespace}_{table_suffix}"
    )


def create_engines(config: Config) -> dict[str, Engine]:
    return {
        "user": create_engine(config, "user"),
        "project": create_engine(config, "project"),
        "repository": create_engine(config, "repository"),
        "differential": create_engine(config, "differential"),
    }


def prepare_bases(engines: dict[str, Engine]) -> dict[str, Any]:
    Base = automap_base()
    bases = {}
    for key, engine in engines.items():
        Base.prepare(engine)
        bases[key] = Base
    return bases


@dataclass(frozen=True)
class Db:
    """ORM-class namespaces, one per Phabricator database."""

    user: SimpleNamespace
    project: SimpleNamespace
    repo: SimpleNamespace
    diff: SimpleNamespace

    @classmethod
    def from_bases(cls, bases: dict[str, Any]) -> Db:
        user_classes = bases["user"].classes
        project_classes = bases["project"].classes
        repo_classes = bases["repository"].classes
        diff_classes = bases["differential"].classes
        return cls(
            user=SimpleNamespace(
                User=user_classes.user,
                UserEmail=user_classes.user_email,
            ),
            project=SimpleNamespace(
                Project=project_classes.project,
                Edges=project_classes.edge,
                Transaction=project_classes.project_transaction,
            ),
            repo=SimpleNamespace(
                Repository=repo_classes.repository,
                RepositoryURI=repo_classes.repository_uri,
            ),
            diff=SimpleNamespace(
                Revision=diff_classes.differential_revision,
                Differential=diff_classes.differential_diff,
                Changeset=diff_classes.differential_changeset,
                Transaction=diff_classes.differential_transaction,
                TransactionComment=diff_classes.differential_transaction_comment,
                Reviewer=diff_classes.differential_reviewer,
                Edges=diff_classes.edge,
                CustomFieldStorage=diff_classes.differential_customfieldstorage,
            ),
        )


@dataclass
class Sessions:
    """SQLAlchemy sessions and ORM-class namespaces for the four Phab databases."""

    users: Session
    projects: Session
    repo: Session
    diff: Session
    db: Db

    @classmethod
    def from_config(cls, config: Config) -> Sessions:
        engines = create_engines(config)
        bases = prepare_bases(engines)
        return cls(
            users=Session(engines["user"]),
            projects=Session(engines["project"]),
            repo=Session(engines["repository"]),
            diff=Session(engines["differential"]),
            db=Db.from_bases(bases),
        )


def get_last_review_id(revision_phid: str, sessions: Sessions) -> Optional[int]:
    last_review = (
        sessions.diff.query(sessions.db.diff.Reviewer)
        .filter_by(revisionPHID=revision_phid)
        .order_by(desc("dateModified"))
        .first()
    )
    return last_review.id if last_review else None


def get_target_repository(repository_phid: str, sessions: Sessions) -> Optional[Any]:
    """Return the repository model object for a revision's target repository."""
    return (
        sessions.repo.query(sessions.db.repo.Repository)
        .filter_by(phid=repository_phid)
        .first()
    )


def get_target_repository_uri(
    repository_phid: str, sessions: Sessions
) -> Optional[str]:
    """Return the URI for a revision's target repository."""
    repository_uri = (
        sessions.repo.query(sessions.db.repo.RepositoryURI)
        .filter_by(repositoryPHID=repository_phid)
        .first()
    )
    return repository_uri.uri if repository_uri else None


def diff_phid_to_id(diff_phid: Optional[str], sessions: Sessions) -> Optional[int]:
    if diff_phid is None:
        return None

    diff = (
        sessions.diff.query(sessions.db.diff.Differential)
        .filter_by(phid=diff_phid)
        .one()
    )

    return diff.id


def get_diff_id_for_changeset(
    changeset_id: Optional[int], sessions: Sessions
) -> Optional[int]:
    if changeset_id is None:
        return None

    changeset = (
        sessions.diff.query(sessions.db.diff.Changeset).filter_by(id=changeset_id).one()
    )

    return changeset.diffID


def get_bug_id(revision: Any, sessions: Sessions, bug_id_query: Any) -> Optional[int]:
    bug_id_query_result = bug_id_query.filter(
        sessions.db.diff.CustomFieldStorage.objectPHID == revision.phid
    ).one_or_none()

    if not bug_id_query_result:
        return None

    return bug_id_query_result.fieldValue or None


def get_revision_projects(
    revision: Any, sessions: Sessions, projects_query: Any
) -> list[str]:
    """Return the project tag slugs associated with a revision."""
    edge_query_result = (
        sessions.diff.query(sessions.db.diff.Edges)
        .filter(
            sessions.db.diff.Edges.src == revision.phid,
            sessions.db.diff.Edges.type
            == PhabricatorEdgeConstant.OBJECT_HAS_PROJECT.value,
        )
        .all()
    )

    project_phids = {edge.dst for edge in edge_query_result}

    projects = projects_query.filter(
        sessions.db.project.Project.phid.in_(project_phids)
    ).all()

    return [project.primarySlug for project in projects]


def get_stack_size(
    revision: Any,
    bug_id: Optional[int],
    all_revisions: Any,
    bug_id_query: Any,
    sessions: Sessions,
) -> int:
    # Stacks without a bug ID have no detectable cross-revision links.
    if not bug_id:
        return 1

    stack = set()
    neighbors = {revision.phid}

    while neighbors:
        edge_query_result = (
            sessions.diff.query(sessions.db.diff.Edges)
            .filter(
                or_(
                    sessions.db.diff.Edges.src.in_(neighbors),
                    sessions.db.diff.Edges.dst.in_(neighbors),
                ),
                sessions.db.diff.Edges.type.in_(
                    [
                        PhabricatorEdgeConstant.DEPENDS_ON.value,
                        PhabricatorEdgeConstant.DEPENDED_ON.value,
                    ]
                ),
            )
            .all()
        )

        bug_matching_revlist = []
        for edge in edge_query_result:
            for node_phid in (edge.src, edge.dst):
                for rev in all_revisions.filter_by(phid=node_phid):
                    neighbor_bug_id = get_bug_id(rev, sessions, bug_id_query)
                    if not neighbor_bug_id:
                        continue

                    if neighbor_bug_id == bug_id:
                        bug_matching_revlist.append(rev.phid)

        stack.update(neighbors)

        neighbors = set(bug_matching_revlist) - stack

    return len(stack)


def get_user_name(author_phid: str, sessions: Sessions) -> Optional[str]:
    try:
        user = (
            sessions.users.query(sessions.db.user.User)
            .filter_by(phid=author_phid)
            .one()
        )
        return user.userName
    except NoResultFound:
        return None


def get_user_email(author_phid: str, sessions: Sessions) -> Optional[str]:
    try:
        user_email = (
            sessions.users.query(sessions.db.user.UserEmail)
            .filter_by(userPHID=author_phid, isPrimary=1)
            .one()
        )
        return user_email.address
    except NoResultFound:
        return None


def get_review_requests(
    revision: Any,
    sessions: Sessions,
) -> tuple[list[dict], Optional[int]]:
    reviews = list(
        sessions.diff.query(sessions.db.diff.Reviewer).filter_by(
            revisionPHID=revision.phid
        )
    )

    review_requests = []
    for review in reviews:
        is_reviewer_group = review.reviewerPHID.startswith(b"PHID-PROJ-")
        if is_reviewer_group:
            reviewer = (
                sessions.projects.query(sessions.db.project.Project)
                .filter_by(phid=review.reviewerPHID)
                .one()
            )
            reviewer_username = reviewer.name
            reviewer_email = None
        else:
            reviewer_username = get_user_name(review.reviewerPHID, sessions)
            reviewer_email = get_user_email(review.reviewerPHID, sessions)

        review_requests.append(
            transform_review_dict(
                review=review,
                revision_id=revision.id,
                reviewer_username=reviewer_username,
                reviewer_email=reviewer_email,
                is_group=is_reviewer_group,
                last_action_diff_id=diff_phid_to_id(
                    review.lastActionDiffPHID, sessions
                ),
                last_comment_diff_id=diff_phid_to_id(
                    review.lastCommentDiffPHID, sessions
                ),
            )
        )

    return review_requests, latest_approved_date(reviews)


def get_diffs_changesets(
    revision: Any,
    sessions: Sessions,
) -> tuple[list[dict], list[dict], Optional[int]]:
    all_diffs = list(
        sessions.diff.query(sessions.db.diff.Differential).filter_by(
            revisionID=revision.id
        )
    )

    diffs = []
    changesets = []
    for diff in all_diffs:
        if not should_include_diff(diff):
            continue

        diffs.append(
            {
                "creation_method": diff.creationMethod,
                "diff_id": diff.id,
                "revision_id": revision.id,
                "date_created": diff.dateCreated,
                "author_email": get_user_email(diff.authorPHID, sessions),
                "author_username": get_user_name(diff.authorPHID, sessions),
            }
        )
        changesets.extend(get_changesets(revision, diff, sessions))

    return diffs, changesets, latest_landed_date(all_diffs)


def get_changesets(revision: Any, diff: Any, sessions: Sessions) -> list[dict]:
    return [
        transform_changeset_dict(
            changeset=changeset,
            revision_id=revision.id,
            diff_id=diff.id,
        )
        for changeset in sessions.diff.query(sessions.db.diff.Changeset).filter_by(
            diffID=diff.id
        )
    ]


def get_comments(revision: Any, sessions: Sessions) -> list[dict]:
    comments = []

    # Top-level revision comments are stored as `core:comment` transactions
    # rather than against a specific diff or changeset.
    comment_transaction_phids_query = (
        sessions.diff.query(sessions.db.diff.Transaction)
        .with_entities(sessions.db.diff.Transaction.commentPHID)
        .filter_by(
            objectPHID=revision.phid,
            transactionType="core:comment",
        )
        .all()
    )

    comment_transaction_phids = [row[0] for row in comment_transaction_phids_query]

    for comment in sessions.diff.query(sessions.db.diff.TransactionComment).filter(
        (sessions.db.diff.TransactionComment.revisionPHID == revision.phid)
        | (sessions.db.diff.TransactionComment.phid.in_(comment_transaction_phids))
    ):
        comments.append(
            transform_comment_dict(
                comment=comment,
                revision_id=revision.id,
                diff_id=get_diff_id_for_changeset(comment.changesetID, sessions),
                author_email=get_user_email(comment.authorPHID, sessions),
                author_username=get_user_name(comment.authorPHID, sessions),
            )
        )

    return comments


def get_transactions(revision: Any, sessions: Sessions) -> list[dict]:
    transactions = []

    for transaction in (
        sessions.diff.query(sessions.db.diff.Transaction)
        .filter(
            sessions.db.diff.Transaction.objectPHID == revision.phid,
            sessions.db.diff.Transaction.transactionType.in_(STATE_CHANGE_TYPES),
        )
        .order_by(sessions.db.diff.Transaction.dateCreated)
    ):
        transactions.append(
            transform_transaction_dict(
                transaction=transaction,
                revision_id=revision.id,
                author_email=get_user_email(transaction.authorPHID, sessions),
                author_username=get_user_name(transaction.authorPHID, sessions),
            )
        )

    return transactions


def get_review_groups(sessions: Sessions) -> list[dict]:
    """Return one row per non-`bmo-` project, with usernames/emails of members."""
    groups = []

    projects = sessions.projects.query(sessions.db.project.Project).filter(
        ~sessions.db.project.Project.name.startswith("bmo-")
    )

    logging.info(f"Found {projects.count()} review groups for processing.")

    for project in projects:
        edge_query_result = (
            sessions.projects.query(sessions.db.project.Edges)
            .filter(
                sessions.db.project.Edges.src == project.phid,
                sessions.db.project.Edges.type
                == PhabricatorEdgeConstant.PROJECT_HAS_MEMBER.value,
            )
            .all()
        )

        member_phids = {edge.dst for edge in edge_query_result}

        member_names = []
        member_emails = []
        for phid in member_phids:
            name = get_user_name(phid, sessions)
            member_names.append(name)
            email = get_user_email(phid, sessions)
            member_emails.append(email)

        groups.append(
            {
                "group_id": project.id,
                "group_name": project.name,
                "group_usernames": member_names,
                "group_emails": member_emails,
            }
        )

    return groups


def get_project(project_phid: str, sessions: Sessions) -> Optional[Any]:
    """Return the project model object for a project PHID."""
    return (
        sessions.projects.query(sessions.db.project.Project)
        .filter_by(phid=project_phid)
        .first()
    )


def usernames_for_member_phids(member_phids: set[str], sessions: Sessions) -> list[str]:
    """Return a sorted list of usernames for member PHIDs.

    PHIDs that do not resolve to a user are skipped.
    """
    if not member_phids:
        return []

    rows = (
        sessions.users.query(sessions.db.user.User.userName)
        .filter(sessions.db.user.User.phid.in_(member_phids))
        .all()
    )
    return sorted({row[0] for row in rows})


def get_project_transactions(sessions: Sessions) -> list[dict]:
    """Return one row per tracked project transaction across all projects.

    Captures project creation (`core:create`), renames (`project:name`), and
    membership changes (`core:edge` filtered to `PROJECT_HAS_MEMBER` edges).
    For membership changes, `old_value`/`new_value` hold the lists of
    usernames removed/added by the transaction.
    """
    transactions = []

    project_transactions = (
        sessions.projects.query(sessions.db.project.Transaction)
        .filter(
            sessions.db.project.Transaction.transactionType.in_(
                PROJECT_TRANSACTION_TYPES
            )
        )
        .order_by(sessions.db.project.Transaction.dateCreated)
    )

    for transaction in project_transactions:
        project = get_project(transaction.objectPHID, sessions)

        if transaction.transactionType == "core:edge":
            if not is_membership_edge_transaction(transaction.metadata):
                continue

            old_phids = parse_edge_member_phids(transaction.oldValue)
            new_phids = parse_edge_member_phids(transaction.newValue)
            old_value = usernames_for_member_phids(old_phids - new_phids, sessions)
            new_value = usernames_for_member_phids(new_phids - old_phids, sessions)
        elif transaction.transactionType == "project:name":
            old_value = decode_name_transaction_value(transaction.oldValue)
            new_value = decode_name_transaction_value(transaction.newValue)
        else:
            # `core:create` marks the project's creation: there is no prior
            # value, and the new value is the name of the newly created project.
            old_value = []
            new_value = [project.name] if project else []

        transactions.append(
            transform_project_transaction_dict(
                transaction=transaction,
                project_id=project.id if project else None,
                project_name=project.name if project else None,
                author_email=get_user_email(transaction.authorPHID, sessions),
                author_username=get_user_name(transaction.authorPHID, sessions),
                old_value=old_value,
                new_value=new_value,
            )
        )

    return transactions


def get_revision(
    revision: Any,
    bug_id: Optional[int],
    date_approved: Optional[int],
    date_landed: Optional[int],
    sessions: Sessions,
    all_revisions: Any,
    bug_id_query: Any,
    projects_query: Any,
) -> dict[str, Any]:
    """Return the BigQuery row dict for a single revision."""
    target_repo = get_target_repository(revision.repositoryPHID, sessions)
    repo_details = parse_repository_details(target_repo)

    return {
        "bug_id": bug_id,
        "revision_id": revision.id,
        # `date_approved` is only meaningful once a landing exists.
        "date_approved": date_approved if date_landed else None,
        "date_created": revision.dateCreated,
        "date_modified": revision.dateModified,
        "date_landed": date_landed,
        "last_review_id": get_last_review_id(revision.phid, sessions),
        "current_status": revision.status,
        "target_repository": get_target_repository_uri(
            revision.repositoryPHID, sessions
        ),
        "target_repository_name": target_repo.name if target_repo else None,
        "target_repository_default_branch": repo_details.get("default-branch"),
        "stack_size": get_stack_size(
            revision, bug_id, all_revisions, bug_id_query, sessions
        ),
        "project_tags": get_revision_projects(revision, sessions, projects_query),
    }


def get_last_run_timestamp(
    bq_client: bigquery.Client, config: Config
) -> Optional[datetime]:
    """Return the most recent `date_modified` from the revisions table."""
    most_recent_run_sql = (
        f"SELECT MAX(date_modified) as last_run FROM `{config.bq_revisions_table_id}`"
    )

    parent_job = bq_client.query(most_recent_run_sql)
    rows = list(parent_job.result())

    if len(rows) != 1:
        logging.error("Only one row should be returned by timestamp query.")
        sys.exit(1)

    return rows[0].last_run


def load_bigquery_tables(
    bq_client: bigquery.Client, config: Config
) -> dict[str, bigquery.Table]:
    """Return `{table_id: Table}` for every BigQuery table the ETL writes to."""
    return {
        config.bq_revisions_table_id: bq_client.get_table(config.bq_revisions_table_id),
        config.bq_diffs_table_id: bq_client.get_table(config.bq_diffs_table_id),
        config.bq_changesets_table_id: bq_client.get_table(
            config.bq_changesets_table_id
        ),
        config.bq_comments_table_id: bq_client.get_table(config.bq_comments_table_id),
        config.bq_review_requests_table_id: bq_client.get_table(
            config.bq_review_requests_table_id
        ),
        config.bq_transactions_table_id: bq_client.get_table(
            config.bq_transactions_table_id
        ),
        config.bq_review_groups_table_id: bq_client.get_table(
            config.bq_review_groups_table_id
        ),
        config.bq_project_transactions_table_id: bq_client.get_table(
            config.bq_project_transactions_table_id
        ),
    }


def create_staging_tables(
    bq_client: bigquery.Client, tables: dict[str, bigquery.Table]
) -> dict[str, bigquery.Table]:
    """Create per-target staging tables and return `{target_id: staging_table}`."""
    return {
        sql_table_id(table): bq_client.create_table(
            bigquery.Table(staging_table_id(sql_table_id(table)), schema=table.schema),
            exists_ok=False,
        )
        for table in tables.values()
    }


def get_time_queries(
    now: datetime,
    bq_client: bigquery.Client,
    sessions: Sessions,
    config: Config,
    full: bool = False,
) -> list:
    """Build the timestamp filters that bound the revisions-to-process window.

    Excludes revisions created or modified at or after `now`; when not in
    `--full` mode also excludes those at or before the previous run's last
    `date_modified`.
    """
    queries = [
        or_(
            sessions.db.diff.Revision.dateCreated < now.timestamp(),
            sessions.db.diff.Revision.dateModified < now.timestamp(),
        ),
    ]

    if full:
        logging.info("Full mode enabled, processing all revisions from the beginning.")
        return queries

    last_run_datetime = get_last_run_timestamp(bq_client, config)

    if last_run_datetime:
        last_run_datetime = last_run_datetime.replace(tzinfo=timezone.utc)
        last_run_timestamp = last_run_datetime.timestamp()

        logging.info(
            f"Using {last_run_datetime} as the last run date ({last_run_timestamp})."
        )

        queries.extend(
            [
                or_(
                    sessions.db.diff.Revision.dateCreated > last_run_timestamp,
                    sessions.db.diff.Revision.dateModified > last_run_timestamp,
                ),
            ]
        )
    else:
        logging.info("No last run found, starting from the beginning.")

    return queries


def revision_year_month(revision) -> tuple[int, int]:
    """Return the `(year, month)` of a revision's `dateModified` in UTC."""
    modified_date = datetime.fromtimestamp(revision.dateModified, tz=timezone.utc)
    return (modified_date.year, modified_date.month)


def staging_table_id(table_id: str) -> str:
    """Return `<table_id>_staging`."""
    return f"{table_id}_staging"


def sql_table_id(table: bigquery.Table) -> str:
    """Return a standard-SQL identifier `project.dataset_id.table_id`.

    `bigquery.Table.full_table_id` uses the colon form `project:dataset.table`,
    which is not valid in a SQL query.
    """
    return f"{table.project}.{table.dataset_id}.{table.table_id}"


def merge_into_bigquery(
    bq_client: bigquery.Client,
    table_id: str,
    staging_table_id: str,
    id_column: str,
    bq_tables: dict[str, bigquery.Table],
    updated_at_column: Optional[str] = None,
):
    """Upsert rows from a staging table into its target table via `MERGE`.

    The staging table is deduplicated by `id_column` before the merge; when
    `updated_at_column` is given the row with the largest value wins,
    otherwise an arbitrary row is kept.
    """
    logging.info(
        f"Merging staging table {staging_table_id} into target table `{table_id}`."
    )
    target_table = bq_tables[table_id]

    if updated_at_column:
        order_clause = f"ORDER BY {updated_at_column} DESC"
    else:
        order_clause = "ORDER BY (SELECT NULL)"

    dedup_subquery = (
        f"SELECT * FROM `{staging_table_id}` "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_column} {order_clause}) = 1"
    )

    merge_query = f"""
        MERGE `{table_id}` as T
        USING ({dedup_subquery}) as S
        ON T.{id_column} = S.{id_column}
        WHEN MATCHED THEN
          UPDATE SET {", ".join(f"{column.name} = S.{column.name}" for column in target_table.schema)}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(column.name for column in target_table.schema)})
          VALUES ({", ".join(f"S.{column.name}" for column in target_table.schema)});
    """

    merge_job = bq_client.query(merge_query)
    merge_job.result()

    logging.info(f"Merged staging table for {table_id}.")


def truncate_staging_table(bq_client: bigquery.Client, table_id: str):
    """Truncate a staging table, refusing to touch anything not named `*_staging`."""
    if not table_id.endswith("_staging"):
        raise ValueError(
            f"Refusing to truncate `{table_id}`: table ID must end with `_staging`."
        )

    truncate_job = bq_client.query(f"TRUNCATE TABLE `{table_id}`")
    truncate_job.result()
    logging.info(f"Truncated staging table `{table_id}`.")


def truncate_staging_tables(
    bq_client: bigquery.Client,
    staging_tables: dict[str, bigquery.Table],
):
    """Truncate every staging table."""
    for target_table_id in staging_tables:
        truncate_staging_table(bq_client, sql_table_id(staging_tables[target_table_id]))


def delete_staging_table(bq_client: bigquery.Client, table_id: str):
    """Delete the BigQuery table with the given ID."""
    bq_client.delete_table(table_id)
    logging.info(f"Deleted table {table_id}.")


def merge_staging_tables(
    bq_client: bigquery.Client,
    staging_tables: dict[str, bigquery.Table],
    target_tables: dict[str, bigquery.Table],
    config: Config,
):
    """Merge every staging table into its corresponding target table."""
    for target_table_id, id_column, updated_at_column in (
        (config.bq_revisions_table_id, "revision_id", "date_modified"),
        (config.bq_diffs_table_id, "diff_id", "date_created"),
        (config.bq_changesets_table_id, "changeset_id", None),
        (config.bq_review_requests_table_id, "review_id", "date_modified"),
        (config.bq_comments_table_id, "comment_id", "date_created"),
        (config.bq_transactions_table_id, "transaction_id", "date_created"),
        (config.bq_review_groups_table_id, "group_id", None),
        (config.bq_project_transactions_table_id, "transaction_id", "date_created"),
    ):
        merge_into_bigquery(
            bq_client,
            target_table_id,
            sql_table_id(staging_tables[target_table_id]),
            id_column,
            target_tables,
            updated_at_column,
        )


def submit_to_bigquery(
    bq_client: bigquery.Client,
    table: bigquery.Table,
    rows: list[dict],
):
    if not rows:
        return

    for chunk in chunked(rows, 500):
        errors = bq_client.insert_rows_json(sql_table_id(table), chunk)
        if errors:
            logging.error(
                "Encountered errors while inserting rows to "
                f"{sql_table_id(table)}: {errors}."
            )
            sys.exit(1)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Phabricator ETL: extract revision data and load into BigQuery."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Process all revisions since the beginning, ignoring the last run timestamp.",
    )
    return parser.parse_args(argv)


def process_revision(
    revision,
    bq_client: bigquery.Client,
    sessions: Sessions,
    staging_tables: dict[str, bigquery.Table],
    all_revisions,
    bug_id_query,
    projects_query,
    config: Config,
):
    """Gather data for one revision and submit it to BigQuery staging tables."""
    logging.info(f"Processing revision D{revision.id}.")

    phab_querying_start = time.perf_counter()

    bug_id = get_bug_id(revision, sessions, bug_id_query)

    diffs, changesets, date_landed = get_diffs_changesets(
        revision,
        sessions,
    )

    review_requests, date_approved = get_review_requests(revision, sessions)

    revision_json = get_revision(
        revision,
        bug_id,
        date_approved,
        date_landed,
        sessions,
        all_revisions,
        bug_id_query,
        projects_query,
    )

    comments = get_comments(revision, sessions)

    transactions = get_transactions(revision, sessions)

    phab_gathering_time = round(time.perf_counter() - phab_querying_start, ndigits=2)
    logging.info(
        f"Gathered relevant info for D{revision.id} in {phab_gathering_time}s."
    )

    if config.debug:
        pprint.pprint(revision_json)
        return

    bigquery_insert_start = time.perf_counter()

    for target_table_id, data in (
        (config.bq_revisions_table_id, [revision_json]),
        (config.bq_diffs_table_id, diffs),
        (config.bq_changesets_table_id, changesets),
        (config.bq_review_requests_table_id, review_requests),
        (config.bq_comments_table_id, comments),
        (config.bq_transactions_table_id, transactions),
    ):
        submit_to_bigquery(bq_client, staging_tables[target_table_id], data)

    bigquery_insert_time = round(time.perf_counter() - bigquery_insert_start, ndigits=2)
    logging.info(
        f"Submitted revision D{revision.id} in BigQuery staging "
        f"in {bigquery_insert_time}s."
    )


def process():
    args = parse_args()

    now = datetime.now()

    logging.info(f"Starting Phab-ETL with timestamp {now}.")

    config = get_config()
    sessions = Sessions.from_config(config)

    bq_client = bigquery.Client()

    target_tables = load_bigquery_tables(bq_client, config)
    staging_tables = create_staging_tables(bq_client, target_tables)

    time_queries = get_time_queries(now, bq_client, sessions, config, full=args.full)

    updated_revisions = (
        sessions.diff.query(sessions.db.diff.Revision)
        .filter(*time_queries)
        .order_by(desc(sessions.db.diff.Revision.dateModified))
    )
    all_revisions = sessions.diff.query(sessions.db.diff.Revision)

    projects_query = sessions.projects.query(sessions.db.project.Project)

    # TODO `fieldIndex` here is the bug-id custom field; document its origin.
    bug_id_query = sessions.diff.query(sessions.db.diff.CustomFieldStorage).filter(
        sessions.db.diff.CustomFieldStorage.fieldIndex == b"zdMFYM6423ua"
    )

    review_groups = get_review_groups(sessions)
    submit_to_bigquery(
        bq_client,
        staging_tables[config.bq_review_groups_table_id],
        review_groups,
    )

    project_transactions = get_project_transactions(sessions)
    logging.info(f"Found {len(project_transactions)} project transactions.")
    submit_to_bigquery(
        bq_client,
        staging_tables[config.bq_project_transactions_table_id],
        project_transactions,
    )

    logging.info(f"Found {updated_revisions.count()} revisions for processing.")

    for (year, month), revisions in itertools.groupby(
        updated_revisions, key=revision_year_month
    ):
        logging.info(f"Processing revisions for {year}-{month:02d}.")

        for revision in revisions:
            process_revision(
                revision,
                bq_client,
                sessions,
                staging_tables,
                all_revisions,
                bug_id_query,
                projects_query,
                config,
            )

        logging.info(f"Finished processing {year}-{month:02d}, merging staging tables.")
        merge_staging_tables(bq_client, staging_tables, target_tables, config)
        truncate_staging_tables(bq_client, staging_tables)

    for target_table_id in staging_tables:
        delete_staging_table(bq_client, sql_table_id(staging_tables[target_table_id]))


if __name__ == "__main__":
    process()
