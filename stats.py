#!/bin/env python3
import json
import os
from pathlib import Path

import sqlalchemy
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

# Results
output = {}
revisions = session_diff.query(Revision)
for revision in revisions:
    output[revision.title] = {}
    output[revision.title][
        "first submission timestamp (dateCreated)"
    ] = revision.dateCreated
    output[revision.title][
        "last review id (lastReviewerPHID)"
    ] = revision.lastReviewerPHID
    output[revision.title]["current status"] = revision.status
    output[revision.title]["stack size (bug-id)"] = revision.id
    repository = session_repo.query(Repo).filter_by(
        repositoryPHID=revision.repositoryPHID
    )
    output[revision.title]["target repository"] = repository.first().uri
    # diffs
    output[revision.title]["diffs"] = {}
    for diff in session_diff.query(Diff).filter_by(revisionID=revision.id):
        diff_id = f"diff-{diff.id}"
        current_diff = output[revision.title]["diffs"][diff_id] = {}
        current_diff["submission time (dateCreated)"] = diff.dateCreated
        user = session_users.query(User).filter_by(phid=diff.authorPHID).one()
        current_diff["author (userName)"] = user.userName
        current_diff["group (isMailingList)"] = bool(user.isMailingList)
        # changesets
        for changeset in session_diff.query(Changeset).filter_by(diffID=diff.id):
            changeset_id = f"changeset-{changeset.id}"
            current_diff[changeset_id] = {
                "lines added": changeset.addLines,
                "lines removed": changeset.delLines,
            }
            # comments
            current_diff[changeset_id]["comments"] = {}
            for comment in session_diff.query(TransactionComment).filter_by(
                changesetID=changeset.id
            ):
                comment_id = f"comment-{comment.id}"
                user = (
                    session_users.query(User).filter_by(phid=comment.authorPHID).one()
                )
                current_diff[changeset_id]["comments"][comment_id] = {
                    "author": user.userName,
                    "timestamp (dateCreated)": comment.dateCreated,
                    "content": comment.content,
                    "character count": len(comment.content),
                }
                att = json.loads(comment.attributes)
                is_suggestion = False
                if "inline.state.initial" in att:
                    hassuggestion = att["inline.state.initial"].get("hassuggestion")
                    if hassuggestion == "true":
                        is_suggestion = True
                        break
                current_diff[changeset_id]["comments"][comment_id][
                    "is_suggestion"
                ] = is_suggestion
    # comments
    output[revision.title]["comments"] = {}
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
            output[revision.title]["comments"][comment_id] = {
                "author": user.userName,
                "timestamp (dateCreated)": comment.dateCreated,
                "content": comment.content,
                "character count": len(comment.content),
            }
Path("revisions.json").write_text(json.dumps(output, indent=2))
