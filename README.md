# brownfield-codebase-cartographer

## Day 1 Usage

Run analysis on a local repository path:

```bash
uv run python -m src.cli analyze <repo-path>
```

Run analysis by cloning a GitHub repo (use `--repo` and point it at a production-grade repository):

```bash
uv run python -m src.cli analyze --repo https://github.com/ORG/PRODUCTION_REPO
```

Artifacts are written to `.cartography/` inside the analyzed repo. When using `--repo`, the repo is cloned into `.cartography_repos/<repo-name>/`, and artifacts live at `.cartography_repos/<repo-name>/.cartography/`.

Run quality gates:

```bash
./check.sh
uv run pre-commit run --all-files
```
