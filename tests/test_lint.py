# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import subprocess


def test_ruff_check():
    """Verify that `ruff check .` passes with no lint violations."""
    result = subprocess.run(
        ["ruff", "check", "."],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"`ruff check .` failed with:\n{result.stdout}\n{result.stderr}"
    )


def test_ruff_format():
    """Verify that `ruff format --check .` passes with no formatting violations."""
    result = subprocess.run(
        ["ruff", "format", "--check", "."],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"`ruff format --check .` failed with:\n{result.stdout}\n{result.stderr}"
    )
