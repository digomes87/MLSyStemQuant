# CI/CD Process Automation Documentation

This document outlines the automated pipeline for the **QuantML** project. The goal is to ensure code quality, security, and seamless integration through automated Pull Requests.

##  Workflow Overview

The automation is triggered whenever code is pushed to any of the following branch patterns:
- `feature/*`
- `bugfix/*`
- `hotfix/*`

### Flowchart

```mermaid
graph TD
    A[Push to feature/*] --> B{Quality Gate}
    B -->|Fail| C[Pipeline Failed ]
    B -->|Pass| D[Linting (Ruff)]
    D --> E[Security Scan (Bandit)]
    E --> F[Unit Tests (Pytest)]
    F --> G{Conflict Check}
    G -->|Conflict| H[Fail & Notify ]
    G -->|No Conflict| I[Check Existing PR]
    I -->|Exists| J[Update PR & Exit]
    I -->|New| K[Create PR to 'develop']
    K --> L[Assign Reviewers]
    L --> M[Notify Team (Slack) ]
```

## Quality Gate Criteria

Before a PR is created, the code must pass the following checks:

1.  **Linting**: Uses `ruff` to enforce PEP8 and best practices.
    *   *Command*: `ruff check .`
2.  **Security**: Uses `bandit` to scan for common security vulnerabilities.
    *   *Command*: `bandit -r . -lll` (Fails on High Severity)
3.  **Testing**: Uses `pytest` to ensure logic correctness.
    *   *Command*: `pytest`
    *   *Coverage*: Must be 100% executable (pipeline ensures tests run, coverage threshold to be configured).

## Automatic PR Creation

If the Quality Gate passes:
1.  **Target Branch**: `develop`
2.  **Title**: Taken from the last commit message.
3.  **Body**: Populated using `.github/pull_request_template.md` + recent commit history.
4.  **Reviewers**: Automatically assigned (Configured in workflow).

## 🔧 Troubleshooting

### 1. "GitHub Actions is not permitted to create or approve pull requests"
**Cause**: Missing repository permissions.
**Fix**:
1.  Go to **Settings** > **Actions** > **General**.
2.  Scroll to **Workflow permissions**.
3.  Check **Allow GitHub Actions to create and approve pull requests**.
4.  Click **Save**.

### 2. Pipeline fails on "Linting"
**Cause**: Code style violations.
**Fix**: Run `ruff check . --fix` locally before pushing.

### 3. "Merge conflicts detected"
**Cause**: The feature branch is behind `develop` and has conflicting changes.
**Fix**:
```bash
git checkout feature/my-feature
git pull origin develop
# Resolve conflicts manually
git commit -am "resolve conflicts"
git push
```

## Success Criteria
- **Speed**: PR created < 2 mins after push (dependent on runner availability).
- **Quality**: 0 Critical/High security issues.
- **Reliability**: 100% of tests passed before PR creation.

## Notifications
Notifications are sent to Slack channel `#dev-updates` (Requires `SLACK_WEBHOOK_URL` secret).
