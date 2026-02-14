# systolic_orchestrator.py
import os
import time
import uuid
import json
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, validator

SHUTDOWN_TOKEN = os.getenv("SHUTDOWN_TOKEN", "")


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    return int(v)


# -----------------------------
# Defaults / config
# -----------------------------
N_DEFAULT = env_int("N", 3)

# Addressing mode:
#   PE_MODE=local -> PE_BASE_URL_TEMPLATE + PE_BASE_PORT
#   PE_MODE=k8s   -> Indexed Job DNS + headless service
PE_MODE = os.getenv("PE_MODE", "").strip().lower()
if not PE_MODE:
    # sensible default: if a local template is provided, assume local, otherwise k8s
    PE_MODE = "local" if os.getenv("PE_BASE_URL_TEMPLATE") else "k8s"

# Local mode (example for Podman macOS):
#   PE_BASE_URL_TEMPLATE="http://host.containers.internal:{port}"
#   PE_BASE_PORT=9000
PE_BASE_URL_TEMPLATE = os.getenv("PE_BASE_URL_TEMPLATE", "")
PE_BASE_PORT = env_int("PE_BASE_PORT", 9000)

# K8s DNS mode:
# http://{jobName}-{idx}.{headlessSvc}.{ns}.svc.cluster.local:{port}
PE_JOB_NAME = os.getenv("PE_JOB_NAME", "systolic-mm")
PE_SVC_NAME = os.getenv("PE_SVC_NAME", "systolic-mm")
PE_NAMESPACE = os.getenv("PE_NAMESPACE", "default")
PE_PORT = env_int("PE_PORT", 8000)

# HTTP tuning
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "3.0"))
HTTP_CONCURRENCY = env_int("HTTP_CONCURRENCY", 32)

# Run retention
RUN_TTL_S = env_int("RUN_TTL_S", 3600)

# SSE/run event retention
MAX_RUN_EVENTS = env_int("MAX_RUN_EVENTS", 5000)


# -----------------------------
# Helpers
# -----------------------------
def ij_to_idx(i: int, j: int, n: int) -> int:
    return i * n + j


def idx_to_ij(idx: int, n: int) -> Tuple[int, int]:
    return idx // n, idx % n


def pe_base_url(idx: int) -> str:
    if PE_MODE == "local":
        if not PE_BASE_URL_TEMPLATE:
            raise RuntimeError("PE_MODE=local requires PE_BASE_URL_TEMPLATE to be set")
        return PE_BASE_URL_TEMPLATE.format(port=PE_BASE_PORT + idx).rstrip("/")

    host = f"{PE_JOB_NAME}-{idx}.{PE_SVC_NAME}.{PE_NAMESPACE}.svc.cluster.local"
    return f"http://{host}:{PE_PORT}"


def sse_format(event: str, data: Any) -> str:
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"


def exception_details(e: Exception) -> str:
    """
    Produce a useful error string for SSE/debug without leaking huge payloads.
    """
    try:
        if isinstance(e, httpx.HTTPStatusError):
            body = ""
            try:
                body = e.response.text
            except Exception:
                body = "<unavailable>"
            body = (body or "")[:500]
            return f"{repr(e)} status={e.response.status_code} url={str(e.request.url)} body={body}"
        if isinstance(e, httpx.RequestError):
            url = getattr(e.request, "url", None)
            return f"{repr(e)} url={url}"
        s = str(e)
        return s if s else repr(e)
    except Exception:
        return repr(e)


# -----------------------------
# API models
# -----------------------------
class RunRequest(BaseModel):
    n: int = Field(default=N_DEFAULT, ge=1, le=32)
    A: List[List[float]]
    B: List[List[float]]
    total_cycles: Optional[int] = Field(default=None, ge=1, le=256)
    # NEW: visualization pacing (seconds between cycles)
    tick_period_s: Optional[float] = Field(default=None, ge=0.0, le=30.0)

    @validator("A")
    def validate_A(cls, v, values):
        n = values.get("n")
        if n is None:
            return v
        if len(v) != n or any(len(row) != n for row in v):
            raise ValueError(f"A must be {n}x{n}")
        return v

    @validator("B")
    def validate_B(cls, v, values):
        n = values.get("n")
        if n is None:
            return v
        if len(v) != n or any(len(row) != n for row in v):
            raise ValueError(f"B must be {n}x{n}")
        return v


class RunResponse(BaseModel):
    run_id: str
    n: int
    total_cycles: int
    C: List[List[float]]


class RunStatus(BaseModel):
    run_id: str
    n: int
    total_cycles: int
    state: str  # "running" | "done" | "failed"
    created_at: float
    finished_at: Optional[float] = None
    error: Optional[str] = None
    C: Optional[List[List[float]]] = None


# -----------------------------
# App + in-memory store
# -----------------------------
app = FastAPI(title="Systolic Orchestrator (Option B1 + SSE)")

UI_DIR = os.getenv("UI_DIR", "ui")

if os.path.isdir(UI_DIR):
    app.mount("/ui/static", StaticFiles(directory=UI_DIR), name="ui-static")

    @app.get("/ui", response_class=HTMLResponse)
    async def ui_index():
        # serve ui/index.html
        with open(os.path.join(UI_DIR, "index.html"), "r", encoding="utf-8") as f:
            return f.read()

_runs_lock = asyncio.Lock()
_runs: Dict[str, Dict[str, Any]] = {}


async def _cleanup_runs_periodically():
    while True:
        await asyncio.sleep(30)
        now = time.time()
        async with _runs_lock:
            dead = [rid for rid, rec in _runs.items() if now - rec["created_at"] > RUN_TTL_S]
            for rid in dead:
                _runs.pop(rid, None)


@app.on_event("startup")
async def startup():
    asyncio.create_task(_cleanup_runs_periodically())


# -----------------------------
# HTTP helpers
# -----------------------------
async def post_json(client: httpx.AsyncClient, url: str, path: str, payload: dict) -> None:
    r = await client.post(f"{url}{path}", json=payload)
    r.raise_for_status()


async def get_json(client: httpx.AsyncClient, url: str, path: str, params: Optional[dict] = None) -> Any:
    r = await client.get(f"{url}{path}", params=params)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Run event helpers (SSE + replay)
# -----------------------------
def _append_run_event(rec: Dict[str, Any], evt: Dict[str, Any]) -> None:
    rec["event_seq"] += 1
    evt["id"] = rec["event_seq"]
    rec["events"].append(evt)
    if len(rec["events"]) > MAX_RUN_EVENTS:
        rec["events"] = rec["events"][-MAX_RUN_EVENTS:]


async def _emit(run_id: str, event_type: str, data: Any) -> None:
    async with _runs_lock:
        rec = _runs.get(run_id)
        if not rec:
            return
        evt = {"type": event_type, "ts": time.time(), "data": data}
        _append_run_event(rec, evt)
        q: asyncio.Queue = rec["queue"]
        q.put_nowait(evt)


# -----------------------------
# Visualization helper: snapshot states after each cycle
# -----------------------------
async def _snapshot_states(client: httpx.AsyncClient, n: int) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(HTTP_CONCURRENCY)

    async def one(pe_idx: int):
        async with sem:
            url = pe_base_url(pe_idx)
            try:
                st = await get_json(client, url, "/state")
                return {
                    "idx": int(st["job_index"]),
                    "i": int(st["i"]),
                    "j": int(st["j"]),
                    "acc": float(st["acc"]),
                    "last_tick": int(st.get("last_tick", -1)),
                    "done": bool(st.get("done", False)),
                }
            except Exception as e:
                return {"idx": pe_idx, "error": exception_details(e), "url": url}

    states = await asyncio.gather(*[one(i) for i in range(n * n)])
    states.sort(key=lambda x: x.get("idx", 10**9))
    return states


# -----------------------------
# Core execution (async run)
# -----------------------------
async def _execute_run(run_id: str, req: RunRequest) -> None:
    n = req.n
    total_cycles = req.total_cycles if req.total_cycles is not None else (3 * n - 2)
    tick_period = req.tick_period_s
    if tick_period is None:
        tick_period = float(os.getenv("DEFAULT_TICK_PERIOD_S", "0.0"))
    tick_period = max(0.0, min(float(tick_period), 30.0))
    min_cycles = 3 * n - 2
    if total_cycles < min_cycles:
        err = f"total_cycles too small; use at least {min_cycles} for n={n}"
        async with _runs_lock:
            rec = _runs.get(run_id)
            if rec:
                rec["state"] = "failed"
                rec["finished_at"] = time.time()
                rec["error"] = err
        await _emit(run_id, "error", {"message": err})
        return

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
            await _emit(run_id, "start", {"run_id": run_id, "n": n, "total_cycles": total_cycles})

            for t in range(total_cycles):
                # 1) Inject boundary tokens
                inject_calls: List[asyncio.Task] = []

                # A into left edge (i,0)
                for i in range(n):
                    k = t - i
                    aval = req.A[i][k] if 0 <= k < n else 0.0
                    url = pe_base_url(ij_to_idx(i, 0, n))
                    inject_calls.append(
                        asyncio.create_task(
                            post_json(client, url, "/ingest/a", {"run_id": run_id, "cycle": t, "value": float(aval)})
                        )
                    )

                # B into top edge (0,j)
                for j in range(n):
                    k = t - j
                    bval = req.B[k][j] if 0 <= k < n else 0.0
                    url = pe_base_url(ij_to_idx(0, j, n))
                    inject_calls.append(
                        asyncio.create_task(
                            post_json(client, url, "/ingest/b", {"run_id": run_id, "cycle": t, "value": float(bval)})
                        )
                    )

                await asyncio.gather(*inject_calls)

                # 2) Tick all PEs (barrier)
                tick_payload = {"run_id": run_id, "cycle": t}
                await asyncio.gather(*[
                    post_json(client, pe_base_url(pe), "/tick", tick_payload)
                    for pe in range(n * n)
                ])

                # 3) Snapshot accumulators for visualization
                states = await _snapshot_states(client, n)
                await _emit(run_id, "cycle", {"cycle": t, "states": states})
                if tick_period > 0:
                    await asyncio.sleep(tick_period)

            # Collect results
            C = [[0.0 for _ in range(n)] for _ in range(n)]
            for pe in range(n * n):
                data = await get_json(client, pe_base_url(pe), "/result", params={"run_id": run_id})
                i, j, val = int(data["i"]), int(data["j"]), float(data["value"])
                C[i][j] = val

        async with _runs_lock:
            rec = _runs.get(run_id)
            if rec:
                rec["state"] = "done"
                rec["finished_at"] = time.time()
                rec["C"] = C

        await _emit(run_id, "done", {"C": C})

    except Exception as e:
        err = exception_details(e)
        async with _runs_lock:
            rec = _runs.get(run_id)
            if rec:
                rec["state"] = "failed"
                rec["finished_at"] = time.time()
                rec["error"] = err
        await _emit(run_id, "error", {"message": err})


# -----------------------------
# Core execution (sync run) - kept for compatibility
# -----------------------------
async def _run_sync(req: RunRequest) -> RunResponse:
    n = req.n
    total_cycles = req.total_cycles if req.total_cycles is not None else (3 * n - 2)
    min_cycles = 3 * n - 2
    if total_cycles < min_cycles:
        raise HTTPException(status_code=400, detail=f"total_cycles too small; use at least {min_cycles} for n={n}")

    run_id = str(uuid.uuid4())
    created_at = time.time()

    async with _runs_lock:
        _runs[run_id] = {
            "run_id": run_id,
            "n": n,
            "total_cycles": total_cycles,
            "state": "running",
            "created_at": created_at,
            "finished_at": None,
            "error": None,
            "C": None,
            "events": [],
            "event_seq": 0,
            "queue": asyncio.Queue(),
        }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
            for t in range(total_cycles):
                inject_calls = []

                for i in range(n):
                    k = t - i
                    aval = req.A[i][k] if 0 <= k < n else 0.0
                    url = pe_base_url(ij_to_idx(i, 0, n))
                    inject_calls.append(
                        post_json(client, url, "/ingest/a", {"run_id": run_id, "cycle": t, "value": float(aval)})
                    )

                for j in range(n):
                    k = t - j
                    bval = req.B[k][j] if 0 <= k < n else 0.0
                    url = pe_base_url(ij_to_idx(0, j, n))
                    inject_calls.append(
                        post_json(client, url, "/ingest/b", {"run_id": run_id, "cycle": t, "value": float(bval)})
                    )

                await asyncio.gather(*inject_calls)

                tick_payload = {"run_id": run_id, "cycle": t}
                await asyncio.gather(*[
                    post_json(client, pe_base_url(pe), "/tick", tick_payload)
                    for pe in range(n * n)
                ])

            C = [[0.0 for _ in range(n)] for _ in range(n)]
            for pe in range(n * n):
                data = await get_json(client, pe_base_url(pe), "/result", params={"run_id": run_id})
                i, j, val = int(data["i"]), int(data["j"]), float(data["value"])
                C[i][j] = val

        async with _runs_lock:
            rec = _runs.get(run_id)
            if rec:
                rec["state"] = "done"
                rec["finished_at"] = time.time()
                rec["C"] = C

        return RunResponse(run_id=run_id, n=n, total_cycles=total_cycles, C=C)

    except Exception as e:
        err = exception_details(e)
        async with _runs_lock:
            rec = _runs.get(run_id)
            if rec:
                rec["state"] = "failed"
                rec["finished_at"] = time.time()
                rec["error"] = err
        raise


# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "systolic-orchestrator",
        "pe_mode": PE_MODE,
        "pe_base_url_template": PE_BASE_URL_TEMPLATE if PE_MODE == "local" else None,
        "pe_job_name": PE_JOB_NAME if PE_MODE == "k8s" else None,
        "pe_svc_name": PE_SVC_NAME if PE_MODE == "k8s" else None,
        "pe_namespace": PE_NAMESPACE if PE_MODE == "k8s" else None,
        "pe_port": PE_PORT if PE_MODE == "k8s" else None,
    }


@app.post("/runs", response_model=RunResponse)
async def create_run_sync(req: RunRequest):
    return await _run_sync(req)


@app.post("/runs/async")
async def create_run_async(req: RunRequest):
    run_id = str(uuid.uuid4())
    created_at = time.time()
    n = req.n
    total_cycles = req.total_cycles if req.total_cycles is not None else (3 * n - 2)

    async with _runs_lock:
        _runs[run_id] = {
            "run_id": run_id,
            "n": n,
            "total_cycles": total_cycles,
            "state": "running",
            "created_at": created_at,
            "finished_at": None,
            "error": None,
            "C": None,
            "events": [],
            "event_seq": 0,
            "queue": asyncio.Queue(),
        }

    asyncio.create_task(_execute_run(run_id, req))
    return {"run_id": run_id, "n": n, "total_cycles": total_cycles}


@app.get("/runs/{run_id}", response_model=RunStatus)
async def get_run(run_id: str):
    async with _runs_lock:
        rec = _runs.get(run_id)
        if not rec:
            raise HTTPException(status_code=404, detail="run_id not found")
        return RunStatus(
            run_id=rec["run_id"],
            n=rec["n"],
            total_cycles=rec["total_cycles"],
            state=rec["state"],
            created_at=rec["created_at"],
            finished_at=rec["finished_at"],
            error=rec["error"],
            C=rec["C"],
        )


@app.get("/runs/{run_id}/events")
async def get_run_events(run_id: str, limit: int = 2000):
    limit = max(1, min(limit, 20000))
    async with _runs_lock:
        rec = _runs.get(run_id)
        if not rec:
            raise HTTPException(status_code=404, detail="run_id not found")
        ev = rec.get("events", [])
        return {"run_id": run_id, "count": min(len(ev), limit), "events": ev[-limit:]}


@app.get("/runs/{run_id}/debug")
async def run_debug(run_id: str):
    """
    Error introspection endpoint:
    - status + stored error string
    - last events (including SSE error payload)
    """
    async with _runs_lock:
        rec = _runs.get(run_id)
        if not rec:
            raise HTTPException(status_code=404, detail="run_id not found")
        return {
            "run_id": run_id,
            "state": rec["state"],
            "error": rec["error"],
            "created_at": rec["created_at"],
            "finished_at": rec["finished_at"],
            "n": rec["n"],
            "total_cycles": rec["total_cycles"],
            "last_events": rec.get("events", [])[-30:],
        }


@app.get("/runs/{run_id}/stream")
async def stream_run_events(run_id: str, request: Request):
    """
    SSE stream of run progress.
    Emits:
      - start
      - cycle (after each tick; includes per-PE acc snapshot)
      - done
      - error
    Fixes:
      - no duplicate replay (replay up to cursor; then skip old queued events)
      - includes detailed error strings
    """
    async with _runs_lock:
        rec = _runs.get(run_id)
        if not rec:
            raise HTTPException(status_code=404, detail="run_id not found")
        q: asyncio.Queue = rec["queue"]
        replay = list(rec.get("events", []))
        last_replay_id = replay[-1]["id"] if replay else 0

    async def gen():
        # Replay historical events first
        for evt in replay:
            if await request.is_disconnected():
                return
            yield sse_format(evt["type"], {"run_id": run_id, **evt})

        # Then live stream, skipping anything already replayed
        while True:
            if await request.is_disconnected():
                return

            try:
                evt = await asyncio.wait_for(q.get(), timeout=10.0)
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
                continue

            # Skip events already replayed (prevents duplicates)
            if evt.get("id", 0) <= last_replay_id:
                continue

            yield sse_format(evt["type"], {"run_id": run_id, **evt})

            if evt["type"] in ("done", "error"):
                return

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/debug/pe-connectivity")
async def pe_connectivity(n: int = N_DEFAULT):
    """
    Quick connectivity check to each PE's /health.
    Works in both PE_MODE=local and PE_MODE=k8s.
    """
    if n < 1 or n > 32:
        raise HTTPException(status_code=400, detail="n out of range (1..32)")

    async with httpx.AsyncClient(timeout=2.0) as client:
        results: Dict[str, Any] = {}
        for idx in range(n * n):
            url = pe_base_url(idx)
            try:
                r = await client.get(f"{url}/health")
                results[str(idx)] = {"status": r.status_code}
            except Exception as e:
                results[str(idx)] = {"error": exception_details(e), "url": url}
        return {"pe_mode": PE_MODE, "n": n, "count": n * n, "results": results}

@app.post("/grid/shutdown")
async def shutdown_grid(n: int = N_DEFAULT):
    if n < 1 or n > 32:
        raise HTTPException(status_code=400, detail="n out of range (1..32)")

    headers = {}
    if SHUTDOWN_TOKEN:
        headers["X-Shutdown-Token"] = SHUTDOWN_TOKEN

    async with httpx.AsyncClient(timeout=3.0) as client:
        async def one(idx: int):
            url = pe_base_url(idx)
            try:
                r = await client.post(f"{url}/shutdown", headers=headers)
                return {"idx": idx, "status": r.status_code}
            except Exception as e:
                return {"idx": idx, "error": repr(e)}

        results = await asyncio.gather(*[one(i) for i in range(n * n)])

    return {"ok": True, "n": n, "count": n * n, "results": results}
