# AGENTS.md

Compact guidance for OpenCode sessions working in this repo. Read `CONTRIBUTING.md` and `.github/agents/superjoe.agent.md` for the full conventions.

## Project

Queue-agnostic worker pool for Django's task framework. Targets Django 6.1 (alpha/dev) and Python >=3.12. Package: `threadmill/`. Tests: `tests/` with a Django test app at `tests/testapp/`.

## Commands

All commands run via `uv`:

```bash
uv run pytest                              # full suite (incl. benchmarks, coverage)
uv run pytest -m "not benchmark"           # what CI runs by default
uv run pytest -m integration               # integration tests only
uv run pytest -m "integration and benchmark"
uv run pytest --benchmark-compare          # compare vs main baseline (run main first)
uv run ruff check --fix                    # lint (auto-fix)
uv run ruff format                          # format
uv run manage.py threadmill                # run the worker pool (the app's CLI)
```

CI additionally pins Django per matrix step: `uv run --with django~=6.1a1 pytest -m "not benchmark"`.

Run a single test by node ID, e.g. `uv run pytest tests/test_command.py::TestCommand::test_add_arguments__register_all_worker_options`.

## Setup

- Install pre-commit hooks before first commit: `uvx prek install` (not `pre-commit install`).
- `DJANGO_SETTINGS_MODULE=tests.testapp.settings` is already set in `.env` and in `pyproject.toml`. Pytest auto-loads it.
- CI's Linux job starts a Redis service and sets `REDIS_URL`; some integration tests may rely on it. Local runs of `-m integration` may need Redis if a backend test targets it.

## Code & style (repo-specific, beyond PEP 8)

- Follow the `naming-things` guidelines — fetch at session start:
  ```bash
  curl -sSL https://raw.githubusercontent.com/codingjoe/naming-things/refs/heads/main/README.md
  ```
- Ruff config: `select` includes E,W,F,I,S,D,UP,B,C; `ignore` includes `B904`, `D1`, `E501`, `S101` (so `assert` in tests is fine, and missing docstrings on private members are allowed).
- Docstrings use `pep257` convention: present-tense imperative, start with capital, end with period, describe external behavior only. Class docstrings must not repeat the class name or start with a verb.
- Prefer generators over building lists/arrays, `match`/`switch` over `if` chains, EAFP over LBYL, dataclasses for plain data, context managers for resources, walrus `:=` when it aids readability.
- Avoid code comments except for third-party behavior or complex algorithms. Avoid nested functions and loops-in-loops where a generator/recursion works.
- Do not add dependencies unless widely adopted and well-maintained; prefer BSD/MIT-style permissive licenses.

## Coverage

Codecov requires 100% patch coverage on PRs (`pyproject.toml`, `.codecov.yml`). New code must be fully tested; remove unreachable branches rather than excluding them.

## Generated files — do not edit

- `threadmill/_version.py` is written by `setuptools_scm` from git tags.
- `uv.lock`, `.coverage`, `coverage.xml`, `.benchmarks/` are build/test artifacts.

## Architecture notes

- Entry point for end users: `threadmill/management/commands/threadmill.py` (Django management command, Gunicorn-style CLI).
- Core runtime: `threadmill/executor.py` (`TaskExecutor`) — process/thread pool, graceful shutdown, worker recycling, task timeout/backlog.
- Integration point for queue authors: `threadmill/backends.py` (`AcknowledgeableTaskBackend`) — subclasses implement `acquire` (lock-without-remove) and `acknowledge` (remove + publish). Requires late-ack support from the underlying queue.
- Test app backend `tests/testapp/backends.py` (`GeneratingTaskBackend`) generates tasks in-process for benchmarks; reset between runs via `default_task_backend.reset()`.

## Pre-commit

`.pre-commit-config.yaml` runs ruff (check + format), django-upgrade, pyupgrade, mdformat (excludes `.github/agents/`), yamlfmt, and `no-commit-to-branch` (protects `main`). Hooks auto-fix; ruff is configured `--exit-non-zero-on-fix`, so commit any fixes before pushing.

## PR / release

CI runs on `main` pushes and PRs. Releases are published to PyPI via `.github/workflows/release.yml` on GitHub release. Commits to `main` are blocked by `no-commit-to-branch`; work on a branch.
