# Agent Instructions

Use `bd` for task tracking.

This project uses **Beads (`bd`)** for task management.

## Required bd Workflow

- Always run `bd ready` to check for open tasks before starting work.
- Create new tasks with `bd create "Task Name"`.
- Claim a task with `bd update <id> --claim`.
- Close finished work with `bd close <id>`.
- For full usage, see: <https://github.com/steveyegge/beads>

## Quick Reference

```bash
bd onboard
bd ready
bd show <id>
bd create "Task Name"
bd update <id> --claim
bd close <id>
bd sync
```

## Landing the Plane (Session Completion)

When ending a work session, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

### Mandatory Workflow

1. File issues for remaining work: Create issues for anything that needs follow-up.
2. Run quality gates (if code changed): tests, linters, builds.
3. Update issue status: close finished work, update in-progress items.
4. Push to remote (MANDATORY):
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. Clean up: clear stashes, prune remote branches.
6. Verify: all changes committed AND pushed.
7. Hand off: provide context for next session.

### Critical Rules

- Work is NOT complete until `git push` succeeds.
- NEVER stop before pushing; that leaves work stranded locally.
- NEVER say "ready to push when you are"; YOU must push.
- If push fails, resolve and retry until it succeeds.
