# brownfield-codebase-cartographer

## Day 1 Usage

Run analysis on a local repository path:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python -m src.cli analyze <repo-path>
```

Run quality gates:

```bash
./check.sh
PRE_COMMIT_HOME=/tmp/pre-commit-cache UV_CACHE_DIR=/tmp/uv-cache uv run pre-commit run --all-files
```
