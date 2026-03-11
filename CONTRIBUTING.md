# Contributing Guide

## Branching
- Use short-lived branches from `main`.
- Branch naming:
  - `feature/<ticket-or-topic>`
  - `bugfix/<ticket-or-topic>`
  - `task/<topic>`

## Commit Standards
- Use clear descriptive commit messages.
- Keep commits focused and atomic.

## Pull Requests
- Link the business/issue context.
- Include testing evidence for backend and/or frontend changes.
- Note schema/data changes explicitly.
- Require at least one reviewer before merge.

## Data and Secrets
- Never commit `.env`, database files, exports, or generated reports.
- Keep all secrets in environment variables or secret managers.

## Quality Gate
- Frontend changes: `npm --prefix j2-platform/client run -s build`
- Backend changes: run unit/integration checks before merge.
