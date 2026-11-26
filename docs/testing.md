# Testing and CI

## Verify

```bash
uv run --locked ruff check .
uv run --locked mypy src tests
uv run --locked pytest -q
```

Enable the integration suite against a disposable Compose project running these same images and role setup:

```bash
USAGE_TEST_INTEGRATION=1 uv run --locked pytest -q
```

Each integration case uses a new topic and initialized database. The suite includes a transaction marker that the real consumer must skip, read-only reporting checks, and an independent reference for the seeded fixture. Recovery tests run the consumer in a child process that pauses at private points around the database commit, then kill it, race a second worker against it, or replace and truncate the topic while it waits. Backfill tests cover archive limits, in-file conflict rejection, a missing topic, retry after partial publication, and a rebuild into a fresh topic and database. The opt-in performance suite reuses the same stack; see [Performance](performance.md).

## Continuous integration

`.github/workflows/ci.yml` runs on pull requests and pushes to `main`. One job runs Ruff, mypy and the unit tests. A second job starts `compose.yaml` on the default ports, runs the integration suite with `USAGE_TEST_INTEGRATION=1`, prints the Compose logs on failure, and removes the stack. Both jobs use `uv sync --locked` on Python 3.12. Actions are pinned to commit SHAs.
