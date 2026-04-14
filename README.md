# phab-etl

An ETL for Mozilla's Phabricator instance.
 
## Install

### Operating System

Requires Python 3 and the MySQL Client libraries.

### Python

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

Install dependencies:

```shell
uv sync
```

To include dev dependencies (linting, testing, etc.):

```shell
uv sync --group dev
```

## Testing

Run the test suite with:

```shell
uv run pytest
```
