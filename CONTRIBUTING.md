# Contributing

When writing code, you MUST ALWAYS follow the [naming-things](https://github.com/codingjoe/naming-things/blob/main/README.md) guidelines.

<!--
Bots/Agents MUST use:
curl -sSL https://raw.githubusercontent.com/codingjoe/naming-things/refs/heads/main/README.md
-->

## Design Principles

- Consistency – We never lose data, even if someone unplugs the power or network.
- Durability – We recover from any failures, even poorly written tasks.
- Overhead – We focus resources on running tasks, not on managing the scheduler.
- Utilization – We keep the CPU saturated with tasks, not with idle time or waiting for locks.

## Testing

We have unit tests, integration tests, and benchmarks. Avoid mocking if possible.

To run the tests, use the following command:

```bash
uv run pytest
```

Benchmarking snapshots are created automatically.
To compare your feature branch against the main branch,
run the test suite on main, followed by:

```
uv run pytest --benchmark-compare
```

Before your first commit, ensure that the pre-commit hooks are installed by running:

```bash
uvx prek install
```
