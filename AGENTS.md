# Contributor Guidelines

This repository contains a proof-of-concept pipeline that integrates Google Ads and Google Play data
with OpenAI. Code is written in **Python 3.12** and primarily stored under `app/`, `airflow/` and
`scripts/`.

## Coding Style

- Use type hints where possible.
- Format all Python code with `black` (88 character line length). Run `black --check .` before
  committing.
- Lint using `ruff` via `ruff check .`.
- Follow PEP 8 and keep imports sorted.

## Commit Messages

Use [Conventional Commit](https://www.conventionalcommits.org/) prefixes:
`feat:`, `fix:`, `docs:`, `chore:`. Keep the summary concise.

## Tests

Run unit tests with `pytest -q`. Ensure tests pass and linting succeeds before committing. If a command
fails because a tool or dependency is missing, include that fact in the PR description.

## Documentation

Update `README.md` whenever functionality or setup steps change. Store secrets in `.env` (see
`.env.example`).
