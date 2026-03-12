# Brownfield Codebase Cartographer

A resilient, multi-agent pipeline for mapping and understanding complex data engineering codebases.

## Installation

Ensure you have [uv](https://github.com/astral-sh/uv) installed.

```bash
uv sync
```

## Usage

### 1. Analysis

Run a full analysis on a local repository:

```bash
uv run cartographer analyze /path/to/repo
```

**Incremental Analysis:**
To skip unchanged files and resume from a previous state:

```bash
uv run cartographer analyze /path/to/repo --incremental
```

**Remote Repository:**
Analyze a GitHub repository by URL:

```bash
uv run cartographer analyze --repo https://github.com/dbt-labs/jaffle_shop
```

Analysis artifacts (graphs, purpose statements, state) are stored in the `.cartography/` directory of the target repository.

### 2. Querying (The Navigator)

Once a repository has been analyzed, you can ask questions using the interactive Navigator agent:

```bash
uv run cartographer query /path/to/repo
```

The Navigator supports:

- **Similarity Search**: Find implementations of concepts (e.g., "Where is the payment logic?").
- **Lineage Tracing**: Follow data flow upstream to sources or downstream to sinks.
- **Blast Radius**: See which modules depend on a specific file.
- **Detailed Explanation**: Get an LLM-powered breakdown of source code and purpose.

Every answer includes grounded citations to the source files.

## Development

Run quality checks and tests:

```bash
uv run ruff format .
uv run ruff check --fix .
uv run mypy src
uv run pytest
```
