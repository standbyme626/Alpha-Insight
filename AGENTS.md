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

## MCP-First Workflow (Mandatory)

Every task in this repo must start with MCP-first code reading and scoping.

### Required tool order

1. `list_project_files`: narrow candidate files by directory.
2. `search_code`: locate symbols/routes/fields.
3. `read_file_excerpt`: read only relevant ranges.
4. `summarize_for_change` (optional): summarize 3-8 candidate files before editing.
5. Implement changes only after the above steps.

### Required behavior

- Do not begin with full-file traversal when MCP tools can locate context first.
- Keep one investigation round to 3-8 candidate files.
- Read excerpts in small ranges (prefer <= 200 lines per chunk).
- After changes, re-read only impacted files/ranges for verification.

### Preferred instruction templates

```text
先不要改代码。优先使用 MCP（list_project_files -> search_code -> read_file_excerpt）。
先给我 3-8 个最相关文件和最小改动方案。
```

```text
优先使用 MCP 控制 token。先检索并局部读取，再直接修改并跑测试。
输出修改文件清单、测试结果和风险点。
```

```text
必须先调用 loco_explorer 的 list_project_files、search_code、read_file_excerpt，
不要直接全文件遍历。
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
