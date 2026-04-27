# Django Grinder

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/codingjoe/django-grinder/raw/main/images/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/codingjoe/django-grinder/raw/main/images/logo-light.svg">
    <img alt="Django Grinder: A queue agnostic worker for Django's task framework." src="https://github.com/codingjoe/django-grinder/raw/main/images/logo-light.svg">
  </picture>
<br>
  <a href="https://codingjoe.dev/django-grinder/">Documentation</a> |
  <a href="https://github.com/codingjoe/django-grinder/issues/new/choose">Issues</a> |
  <a href="https://github.com/codingjoe/django-grinder/releases">Changelog</a> |
  <a href="https://github.com/sponsors/codingjoe">Funding</a> 💚
</p>

**A queue agnostic worker for Django's task framework.**

- self-healing workers
- graceful shutdown
- CPU, IO, or memory optimized workers

[![PyPi Version](https://img.shields.io/pypi/v/django-grinder.svg)](https://pypi.python.org/pypi/django-grinder/)
[![Test Coverage](https://codecov.io/gh/codingjoe/django-grinder/branch/main/graph/badge.svg)](https://codecov.io/gh/codingjoe/django-grinder)
[![GitHub License](https://img.shields.io/github/license/codingjoe/django-grinder)](https://raw.githubusercontent.com/codingjoe/django-grinder/master/LICENSE)

## Setup

You need to have [Django's Task framework][django-tasks] setup properly.

```console
uv add django-grinder
```

Add `grinder` to your `INSTALLED_APPS` in `settings.py`:

```python
# settings.py
INSTALLED_APPS = [
    "grinder",
    # ...
]
```

Finally, you launch the scheduler in a separate process:

```console
uv run manage.py grinder
```

[django-tasks]: https://docs.djangoproject.com/en/6.0/topics/tasks/
