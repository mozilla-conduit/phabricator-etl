#!/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import os
from pathlib import Path

import sqlalchemy
from sqlalchemy import desc, or_
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

DB_URL = os.environ.get("PHAB_URL", "127.0.0.1")
DB_NAMESPACE = os.environ.get("PHAB_NAMESPACE", "bitnami_phabricator")
DB_PORT = os.environ.get("PHAB_PORT", "3307")
DB_USER = os.environ.get("PHAB_USER", "root")
DB_TOKEN = os.environ["PHAB_TOKEN"]

Base = automap_base()

# Users
engine_user = sqlalchemy.create_engine(
    f"mysql+mysqldb://{DB_USER}:{DB_TOKEN}@{DB_URL}:{DB_PORT}/{DB_NAMESPACE}_user"
)
Base.prepare(engine_user)
session_users = Session(engine_user)
User = Base.classes.user

# Projects
engine_project = sqlalchemy.create_engine(
    f"mysql+mysqldb://{DB_USER}:{DB_TOKEN}@{DB_URL}:{DB_PORT}/{DB_NAMESPACE}_project"
)
Base.prepare(engine_project)
session_projects = Session(engine_project)
Project = Base.classes.project

# Repositories
engine_repo = engine_user = sqlalchemy.create_engine(
    f"mysql+mysqldb://{DB_USER}:{DB_TOKEN}@{DB_URL}:{DB_PORT}/{DB_NAMESPACE}_repository"
)
Base.prepare(engine_repo)
session_repo = Session(engine_repo)
Repo = Base.classes.repository_uri

# Diffs
engine_diffenrential = sqlalchemy.create_engine(
    f"mysql+mysqldb://{DB_USER}:{DB_TOKEN}@{DB_URL}:{DB_PORT}/{DB_NAMESPACE}_differential"
)
Base.prepare(engine_diffenrential)
session_diff = Session(engine_diffenrential)
Revision = Base.classes.differential_revision
Diff = Base.classes.differential_diff
Changeset = Base.classes.differential_changeset
Transaction = Base.classes.differential_transaction
TransactionComment = Base.classes.differential_transaction_comment
Reviewer = Base.classes.differential_reviewer
Edges = Base.classes.edge

# Results
output = {}
revisions = session_diff.query(Revision)
for revision in revisions:
    rev_key = f"D{revision.id}"
    output[rev_key] = {}
    output[rev_key]["first submission timestamp (dateCreated)"] = revision.dateCreated
    last_review = (
        session_diff.query(Reviewer)
        .filter_by(revisionPHID=revision.phid)
        .order_by(desc("dateModified"))
        .first()
    )
    last_review_id = None
    if last_review:
        last_review_id = last_review.id
    output[rev_key]["last review id"] = last_review_id
    output[rev_key]["current status"] = revision.status
    repository = session_repo.query(Repo).filter_by(
        repositoryPHID=revision.repositoryPHID
    )
    output[rev_key]["target repository"] = repository.first().uri
    # stack (edge dependencies)
    stack = set()
    neighbors = {revision.phid}
    bug_id = revision.title.split("-")[0]
    while len(neighbors) > 0:
        query_result = session_diff.query(Edges).filter(
            or_(Edges.src.in_(neighbors), Edges.src.in_(neighbors)),
            or_(Edges.type.in_([5, 6])),
        )
        revlist = []
        for edge in query_result.all():
            for rev_src in revisions.filter_by(phid=edge.src):
                if rev_src.title.split("-")[0] == bug_id:
                    revlist.append(rev_src.phid)
            for rev_dst in revisions.filter_by(phid=edge.dst):
                if rev_dst.title.split("-")[0] == bug_id:
                    revlist.append(rev_dst.phid)
        stack = stack | neighbors
        neighbors = set(revlist) - stack
    stack_size = len(stack)
    output[rev_key]["stack size"] = stack_size
    # diffs
    output[rev_key]["diffs"] = {}
    for diff in session_diff.query(Diff).filter_by(revisionID=revision.id):
        diff_id = f"diff-{diff.id}"
        current_diff = output[rev_key]["diffs"][diff_id] = {}
        current_diff["submission time (dateCreated)"] = diff.dateCreated
        user = session_users.query(User).filter_by(phid=diff.authorPHID).one()
        current_diff["author (userName)"] = user.userName
        # changesets
        current_diff["changesets"] = {}
        for changeset in session_diff.query(Changeset).filter_by(diffID=diff.id):
            changeset_id = f"changeset-{changeset.id}"
            current_diff["changesets"][changeset_id] = {
                "lines added": changeset.addLines,
                "lines removed": changeset.delLines,
            }
            # comments
            current_diff["changesets"][changeset_id]["comments"] = {}
            for comment in session_diff.query(TransactionComment).filter_by(
                changesetID=changeset.id
            ):
                comment_id = f"comment-{comment.id}"
                user = (
                    session_users.query(User).filter_by(phid=comment.authorPHID).one()
                )
                current_diff["changesets"][changeset_id]["comments"][comment_id] = {
                    "author": user.userName,
                    "timestamp (dateCreated)": comment.dateCreated,
                    "character count": len(comment.content),
                }
                att = json.loads(comment.attributes)
                is_suggestion = False
                if "inline.state.initial" in att:
                    hassuggestion = att["inline.state.initial"].get("hassuggestion")
                    if hassuggestion == "true":
                        is_suggestion = True
                        break
                current_diff["changesets"][changeset_id]["comments"][comment_id][
                    "is_suggestion"
                ] = is_suggestion
        # reviews
        current_diff["review requests"] = {}
        for review in session_diff.query(Reviewer).filter_by(
            revisionPHID=revision.phid
        ):
            is_reviewer_group = "PHID-PROJ-" in review.reviewerPHID.decode()
            if is_reviewer_group:
                reviewer = (
                    session_projects.query(Project)
                    .filter_by(phid=review.reviewerPHID)
                    .one()
                )
                reviewer_name = reviewer.name
            else:
                reviewer = (
                    session_users.query(User).filter_by(phid=review.reviewerPHID).one()
                )
                reviewer_name = reviewer.userName
            current_diff["review requests"][f"review-{review.id}"] = {
                "reviewer": reviewer_name,
                "is group": is_reviewer_group,
                "creation timestamp": review.dateCreated,
                "review timestamp": review.dateModified,
                "status": review.reviewerStatus,
            }

    # comments
    output[rev_key]["comments"] = {}
    for transaction in session_diff.query(Transaction).filter_by(
        objectPHID=revision.phid
    ):
        if transaction.transactionType == "core:comment":
            comment = (
                session_diff.query(TransactionComment)
                .filter_by(phid=transaction.commentPHID)
                .one()
            )
            comment_id = f"comment-{comment.id}"
            output[rev_key]["comments"][comment_id] = {
                "author": user.userName,
                "timestamp (dateCreated)": comment.dateCreated,
                "character count": len(comment.content),
            }
Path("revisions.json").write_text(json.dumps(output, indent=2))
