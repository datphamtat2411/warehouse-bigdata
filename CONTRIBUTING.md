# Contributing

This repository uses a small, task-oriented workflow for a two-person development
team. The repository boundaries, PLAN/BUILD process, and technical constraints in
[`AGENTS.md`](AGENTS.md) remain authoritative; this file summarizes the Git and
pull request workflow without duplicating those rules.

## Branches

- `main` is the stable baseline. Do not develop features directly on it.
- `develop` is the integration branch for completed task pull requests. Do not
  develop directly on it.
- Use one task-specific branch per change. Use these prefixes:
  - `chore/fnd-XX-short-description` for foundation work.
  - `feature/short-description` for new behavior.
  - `fix/short-description` for corrections.
  - `docs/short-description` for documentation-only changes.
- Keep names lowercase, short, and separated with hyphens. Keep a branch focused
  on one task and use one focused pull request for that task.

Start a branch from the current remote `develop` branch:

```bash
git fetch origin
git switch develop
git pull --ff-only origin develop
git switch -c feature/short-description
```

Before requesting review, update the branch from `develop` and resolve conflicts
locally. For a branch used only by you, rebasing is preferred:

```bash
git fetch origin
git rebase origin/develop
```

Do not rewrite a branch that another contributor is using without coordinating
first.

## Local Checks

Use Python 3.11 from [`.python-version`](.python-version) and the committed lockfile:

```bash
uv python install 3.11
uv sync --locked --group spark-test
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -q
```

Run the focused checks before opening a pull request. The default CI path is
intentionally lightweight and does not start Docker, Hadoop, Spark, ClickHouse,
or other external services. If infrastructure-dependent tests are added later,
they must use an explicit test marker and remain outside the default lightweight
CI selection through an approved tooling change.

## Pull Requests and Reviews

1. Push the task branch and open a pull request targeting `develop`.
2. Complete [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md),
   including actual verification results and any cross-module impact.
3. Request one peer review when repository permissions support it. Reviewers should
   check scope, contracts, compatibility, documentation, and test evidence.
4. Resolve review conversations and keep the branch current with `develop`.
5. Merge only when the required checks pass and the configured review requirements
   are satisfied. Prefer a squash merge for one focused task.

If CI fails, read the failing check output, reproduce the same command locally,
correct the task branch, and push the fix. Do not bypass a required check or merge
while a required check is failing.

For changes that cross module boundaries, identify every affected module and shared
interface, configuration key, schema, provenance rule, or compatibility concern in
the pull request. Update the owning documentation when the contract changes.

## Repository Protection Handoff

Branch protection is a GitHub repository setting and is not enabled by this file.
After the workflow has run successfully on the remote `develop` branch, a repository
owner should configure one classic branch protection rule for `develop` with:

- Require a pull request before merging.
- Require one approving review from a collaborator with write access, where supported.
- Require conversation resolution.
- Require the `python-quality` status check and require the branch to be up to date.
- Restrict force pushes and branch deletion.
- Enforce the rule for administrators where the repository plan supports it.

The repository currently needs a second write collaborator before peer-review
enforcement can be exercised. GitHub Free personal repositories may not expose every
branch restriction option; retain the required pull request, review, conversation,
and status-check requirements that are available.

## Prohibited Repository Content

Never commit secrets, credentials, raw datasets, `.env` files or other local
environment files, virtual environments, generated artifacts, logs, or runtime data.
Follow [`.gitignore`](.gitignore) and the repository rules in [`AGENTS.md`](AGENTS.md).
