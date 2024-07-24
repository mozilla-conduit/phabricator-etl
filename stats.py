#!/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

import sqlalchemy
from sqlalchemy import desc, or_
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

DB_URL = os.environ.get("PHAB_URL", "127.0.0.1")
DB_NAMESPACE = os.environ.get("PHAB_NAMESPACE", "bitnami_phabricator")
DB_PORT = os.environ.get("PHAB_PORT", "3307")
DB_USER = os.environ.get("PHAB_USER", "root")
DB_TOKEN = os.environ["PHAB_TOKEN"]

# Configure simple logging.
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    stream=sys.stdout,
)


def create_engine(table_suffix: str) -> Engine:
    return sqlalchemy.create_engine(
        f"mysql+mysqldb://{DB_USER}:{DB_TOKEN}@{DB_URL}:{DB_PORT}/{DB_NAMESPACE}_{table_suffix}"
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


PHAB_DEPENDS_ON_EDGE_CONST = 5
PHAB_DEPENDED_ON_EDGE_CONST = 6


def get_stack_size(revision: Any, all_revisions: Any, session_diff: Session) -> int:
    stack = set()
    neighbors = {revision.phid}
    bug_id = revision.title.split("-")[0]
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
                    if rev.title.split("-")[0] == bug_id:
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


def get_review_requests(
    revision_phid: str,
    session_diff: Session,
    session_projects: Session,
    session_users: Session,
) -> dict[str, Any]:
    review_requests = {}
    for review in session_diff.query(DiffDb.Reviewer).filter_by(
        revisionPHID=revision_phid
    ):
        is_reviewer_group = review.reviewerPHID.startswith(b"PHID-PROJ-")
        if is_reviewer_group:
            reviewer = (
                session_projects.query(ProjectDb.Project)
                .filter_by(phid=review.reviewerPHID)
                .one()
            )
            reviewer_name = reviewer.name
        else:
            reviewer = (
                session_users.query(UserDb.User)
                .filter_by(phid=review.reviewerPHID)
                .one()
            )
            reviewer_name = reviewer.userName

        review_requests[f"review-{review.id}"] = {
            "reviewer": reviewer_name,
            "is group": is_reviewer_group,
            "creation timestamp": review.dateCreated,
            "review timestamp": review.dateModified,
            "status": review.reviewerStatus,
        }

    return review_requests


def get_diffs(
    revision: Any,
    session_diff: Session,
    session_users: Session,
    session_projects: Session,
) -> dict[str, Any]:
    diffs = {}
    for diff in session_diff.query(DiffDb.Differential).filter_by(
        revisionID=revision.id
    ):
        if diff.authorPHID == b"PHID-APPS-PhabricatorDiffusionApplication":
            # Ignore diffs that were created as a result of the commit landing.
            continue

        if diff.authorPHID.startswith(b"PHID-RIDT-"):
            # Ignore diffs that were created with repository identity.
            continue

        diffs[f"diff-{diff.id}"] = {
            "submission time (dateCreated)": diff.dateCreated,
            "author (userName)": get_user_name(diff.authorPHID, session_users),
            "changesets": get_changesets(diff, session_diff, session_users),
            "review requests": get_review_requests(
                revision.phid, session_diff, session_projects, session_users
            ),
        }

    return diffs


def get_changesets(
    diff: Any, session_diff: Session, session_users: Session
) -> dict[str, Any]:
    changesets = {}
    for changeset in session_diff.query(DiffDb.Changeset).filter_by(diffID=diff.id):
        changesets[f"changeset-{changeset.id}"] = {
            "lines added": changeset.addLines,
            "lines removed": changeset.delLines,
            "comments": get_changeset_comments(changeset, session_diff, session_users),
        }
    return changesets


def get_changeset_comments(
    changeset: Any, session_diff: Session, session_users: Session
) -> dict[str, Any]:
    comments = {}
    for comment in session_diff.query(DiffDb.TransactionComment).filter_by(
        changesetID=changeset.id
    ):
        att = json.loads(comment.attributes)
        is_suggestion = (
            "inline.state.initial" in att
            and att["inline.state.initial"].get("hassuggestion") == "true"
        )
        comments[f"comment-{comment.id}"] = {
            "author": get_user_name(comment.authorPHID, session_users),
            "timestamp (dateCreated)": comment.dateCreated,
            "character count": len(comment.content),
            "is_suggestion": is_suggestion,
        }
    return comments


def get_comments(
    revision_phid: str, session_diff: Session, session_users: Session
) -> dict[str, Any]:
    comments = {}
    for transaction in session_diff.query(DiffDb.Transaction).filter_by(
        objectPHID=revision_phid,
        transactionType="core:comment",
    ):
        comment = (
            session_diff.query(DiffDb.TransactionComment)
            .filter_by(phid=transaction.commentPHID)
            .one()
        )
        comments[f"comment-{comment.id}"] = {
            "author": get_user_name(comment.authorPHID, session_users),
            "timestamp (dateCreated)": comment.dateCreated,
            "character count": len(comment.content),
        }
    return comments


def get_last_run_timestamp() -> Optional[int]:
    """
    We retrieve the date of the last run in the result file name
    """
    last_results_filepath = Path(".").glob("revisions_*_*.json")
    timestamps = [
        int(filepath.stem.split("_")[-1]) for filepath in last_results_filepath
    ]
    return max(timestamps) if timestamps else None


def get_time_queries(now: datetime) -> list:
    """
    Dont take the revisions created or modified after the start of the run
    Dont take the revisions created before the last run
    """
    queries = [
        DiffDb.Revision.dateCreated < now.timestamp(),
        DiffDb.Revision.dateModified < now.timestamp(),
    ]
    last_run_timestamp = get_last_run_timestamp()
    if last_run_timestamp:
        queries.extend(
            [
                DiffDb.Revision.dateCreated > last_run_timestamp,
                DiffDb.Revision.dateModified > last_run_timestamp,
            ]
        )
    return queries


def export_to_json(output: dict[str, Any], date: datetime):
    """
    We add the timestamp to the filename to keep the last run date
    We round the timestamp to the nearest second
    """
    Path(
        f"revisions_{date.strftime('%Y%m%d')}_{int(date.timestamp())}.json"
    ).write_text(json.dumps(output, indent=2))


def process():
    now = datetime.now()
    logging.info(f"Starting Phab-ETL with timestamp {now}.")

    session_users = Session(engines["user"])
    session_projects = Session(engines["project"])
    session_repo = Session(engines["repository"])
    session_diff = Session(engines["differential"])

    output = {}
    time_queries = get_time_queries(now)
    updated_revisions = session_diff.query(DiffDb.Revision).filter(*time_queries)
    all_revisions = session_diff.query(DiffDb.Revision).all()

    logging.info(f"Found {len(updated_revisions)} for processing.")

    for revision in updated_revisions:
        logging.info(f"Processing revision D{revision.id}.")

        output[f"D{revision.id}"] = {
            "first submission timestamp (dateCreated)": revision.dateCreated,
            "last review id": get_last_review_id(revision.phid, session_diff),
            "current status": revision.status,
            "target repository": get_target_repository(
                revision.repositoryPHID, session_repo
            ),
            "stack size": get_stack_size(revision, all_revisions, session_diff),
            "diffs": get_diffs(
                revision,
                session_diff,
                session_users,
                session_projects,
            ),
            "comments": get_comments(revision.phid, session_diff, session_users),
        }

    logging.info(f"Exporting JSON.")
    export_to_json(output, now)


if __name__ == "__main__":
    process()
