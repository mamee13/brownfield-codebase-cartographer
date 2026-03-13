import asyncio
import json
from pathlib import Path
from typing import AsyncGenerator, Dict, Any, List

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

from src.orchestrator import Orchestrator
from src.agents.navigator import Navigator
from src.api_models import AnalysisRequest, QueryRequest

app = FastAPI(title="Cartographer API")

# Enable CORS for Next.js development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared state
progress_queues: Dict[str, asyncio.Queue[Any]] = {}
analysis_locks: Dict[str, bool] = {}


def get_repo_id(repo_url: str) -> str:
    # Basic slugify
    return repo_url.split("/")[-1].replace(".git", "")


async def run_analysis(
    repo_url: str,
    incremental: bool,
    queue: asyncio.Queue[Any],
    loop: asyncio.AbstractEventLoop,
) -> None:
    repo_id = get_repo_id(repo_url)
    repo_path = Path(".cartography_repos") / repo_id
    analysis_locks[repo_id] = True

    try:
        # Ensure clone if not exists (simplified for demo)
        if not repo_path.exists():
            repo_path.parent.mkdir(parents=True, exist_ok=True)
            import subprocess

            await loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    ["git", "clone", repo_url, str(repo_path)], check=True
                ),
            )

        def on_progress(msg: str) -> None:
            # Use the loop captured from the main thread
            loop.call_soon_threadsafe(
                queue.put_nowait, {"event": "progress", "data": msg}
            )

        orchestrator = Orchestrator(str(repo_path))

        # Run in a threadpool to not block the event loop
        await loop.run_in_executor(
            None,
            lambda: orchestrator.analyze(
                incremental=incremental, on_progress=on_progress
            ),
        )
        await queue.put({"event": "complete", "data": "Analysis finished successfully"})
    except Exception as e:
        await queue.put({"event": "error", "data": str(e)})
    finally:
        analysis_locks[repo_id] = False


@app.post("/analyze")
async def start_analysis(
    request: AnalysisRequest, background_tasks: BackgroundTasks
) -> Dict[str, str]:
    repo_id = get_repo_id(request.repo_url)

    if analysis_locks.get(repo_id):
        return {"status": "already_running", "repo_id": repo_id}

    queue: asyncio.Queue[Any] = asyncio.Queue()
    progress_queues[repo_id] = queue

    loop = asyncio.get_running_loop()
    background_tasks.add_task(
        run_analysis, request.repo_url, request.incremental, queue, loop
    )
    return {"status": "started", "repo_id": repo_id}


@app.get("/analyze/stream/{repo_id}")
async def stream_analysis(repo_id: str) -> EventSourceResponse:
    if repo_id not in progress_queues:
        raise HTTPException(status_code=404, detail="Analysis session not found")

    async def event_generator() -> AsyncGenerator[Dict[str, Any], None]:
        queue = progress_queues[repo_id]
        try:
            while True:
                item = await queue.get()
                # Yield as a default message event with data as JSON string
                yield {"data": json.dumps(item)}
                if item["event"] in ["complete", "error"]:
                    break
        finally:
            # Cleanup queue after stream ends
            if repo_id in progress_queues:
                del progress_queues[repo_id]

    return EventSourceResponse(event_generator())


@app.get("/graph/{repo_id}")
async def get_graph(repo_id: str) -> Any:
    graph_path = (
        Path(".cartography_repos") / repo_id / ".cartography" / "module_graph.json"
    )
    if not graph_path.exists():
        raise HTTPException(status_code=404, detail="Graph not found")

    with open(graph_path, "r") as f:
        return json.load(f)


@app.get("/repos")
async def list_repos() -> List[str]:
    repos_base = Path(".cartography_repos")
    if not repos_base.exists():
        return []

    analyzed_repos = []
    for repo_dir in repos_base.iterdir():
        if repo_dir.is_dir() and (repo_dir / ".cartography").exists():
            analyzed_repos.append(repo_dir.name)

    return analyzed_repos


@app.get("/docs/{repo_id}/{doc_name}")
async def get_doc(repo_id: str, doc_name: str) -> Dict[str, str]:
    # doc_name should be 'codebase.md' or 'onboarding_brief.md' - accommodate case sensitivity
    valid_docs = ["codebase.md", "onboarding_brief.md", "CODEBASE.md"]
    if doc_name not in valid_docs:
        raise HTTPException(status_code=400, detail="Invalid document name")

    cartography_dir = Path(".cartography_repos") / repo_id / ".cartography"
    doc_path = cartography_dir / doc_name

    # Try alternate case if not found
    if not doc_path.exists():
        alt_name = "CODEBASE.md" if doc_name == "codebase.md" else "codebase.md"
        doc_path = cartography_dir / alt_name

    if not doc_path.exists():
        raise HTTPException(
            status_code=404, detail=f"Document {doc_name} not found for {repo_id}"
        )

    with open(doc_path, "r") as f:
        content = f.read()
        return {"content": content}


@app.post("/query")
async def query_repo(request: QueryRequest) -> Any:
    repo_path = Path(request.cartography_dir)
    if not repo_path.exists():
        raise HTTPException(status_code=404, detail="Repository artifacts not found")

    navigator = Navigator(str(repo_path))
    try:
        # Navigator.ask returns a JSON string now
        response_json = navigator.ask(request.query)
        parsed_response = json.loads(response_json)
        return parsed_response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
