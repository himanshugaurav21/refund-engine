"""Genie Space proxy routes — start conversations, send messages, poll results."""

import os
import time
import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.config import get_workspace_host, get_oauth_token, refresh_databricks_token
from server.warehouse import execute_query

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

router = APIRouter(prefix="/api/genie")

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


def _headers():
    return {
        "Authorization": f"Bearer {get_oauth_token()}",
        "Content-Type": "application/json",
    }


def _base_url():
    return f"{get_workspace_host().rstrip('/')}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"


class AskRequest(BaseModel):
    question: str
    conversation_id: str | None = None


@router.get("/space")
def get_space_info():
    if not GENIE_SPACE_ID:
        raise HTTPException(status_code=503, detail="GENIE_SPACE_ID not configured")
    return {"space_id": GENIE_SPACE_ID}


def _genie_query(question: str, conversation_id: str | None = None) -> dict:
    """Send a question to Genie and poll until the answer is ready."""
    pipeline_start = time.time()
    base = _base_url()
    headers = _headers()

    if not HAS_MLFLOW:
        return _genie_impl(question, conversation_id, base, headers, pipeline_start)

    client = mlflow.MlflowClient()
    root = client.start_trace(
        name="genie_query",
        inputs={"question": question, "conversation_id": conversation_id},
    )
    request_id = getattr(root, 'trace_id', None) or root.request_id

    try:
        result = _genie_with_spans(question, conversation_id, base, headers,
                                    pipeline_start, client, request_id, root.span_id)
        total_ms = int((time.time() - pipeline_start) * 1000)
        perf = result.get("performance", {})

        client.end_trace(
            request_id=request_id,
            outputs=result,
            attributes={
                "genie.space_id": GENIE_SPACE_ID,
                "genie.status": result.get("status", "UNKNOWN"),
                "genie.total_ms": total_ms,
                "genie.api_call_ms": perf.get("api_call_ms", 0),
                "genie.poll_ms": perf.get("poll_ms", 0),
                "genie.poll_count": perf.get("poll_count", 0),
                "genie.has_sql": bool(result.get("sql")),
                "genie.row_count": len(result.get("rows", [])),
                "genie.column_count": len(result.get("columns", [])),
            },
        )

        # Set trace tags
        try:
            for k, v in {
                "genie.question": question[:200],
                "genie.status": result.get("status", ""),
                "genie.total_ms": str(total_ms),
                "genie.api_call_ms": str(perf.get("api_call_ms", 0)),
                "genie.poll_ms": str(perf.get("poll_ms", 0)),
                "genie.row_count": str(len(result.get("rows", []))),
                "genie.has_sql": str(bool(result.get("sql"))),
            }.items():
                client.set_trace_tag(request_id, k, v)
        except Exception:
            pass

        return result
    except Exception as e:
        client.end_trace(request_id=request_id, outputs={"error": str(e)}, attributes={"error": True})
        raise


def _genie_with_spans(question, conversation_id, base, headers,
                       pipeline_start, client, request_id, parent_span_id):
    """Execute Genie query with explicit client-based spans."""

    # --- API Call Span ---
    api_span = client.start_span(
        request_id=request_id, name="genie_api_call", span_type="TOOL",
        parent_id=parent_span_id,
        inputs={"question": question, "action": "send_message" if conversation_id else "start_conversation"},
    )
    if conversation_id:
        url = f"{base}/conversations/{conversation_id}/messages"
    else:
        url = f"{base}/start-conversation"

    t0 = time.time()
    resp = requests.post(url, json={"content": question}, headers=headers, timeout=30)
    api_ms = int((time.time() - t0) * 1000)

    client.end_span(
        request_id=request_id, span_id=api_span.span_id,
        outputs={"http_status": resp.status_code, "latency_ms": api_ms},
        attributes={"http_status": resp.status_code, "latency_ms": api_ms},
    )

    if resp.status_code >= 400:
        return {"status": "ERROR", "error": f"Genie API error: {resp.status_code}",
                "performance": {"total_ms": api_ms, "api_call_ms": api_ms, "poll_ms": 0, "poll_count": 0}}

    data = resp.json()
    conv_id = data.get("conversation_id", conversation_id)
    message_id = data.get("message_id", data.get("id"))

    if not conv_id or not message_id:
        return {"conversation_id": conv_id, "status": "ERROR", "error": "Missing IDs",
                "performance": {"total_ms": api_ms, "api_call_ms": api_ms, "poll_ms": 0, "poll_count": 0}}

    # --- Poll Span ---
    poll_span = client.start_span(
        request_id=request_id, name="genie_poll_for_result", span_type="TOOL",
        parent_id=parent_span_id,
        inputs={"conversation_id": conv_id, "message_id": message_id},
    )
    poll_url = f"{base}/conversations/{conv_id}/messages/{message_id}"
    poll_count = 0
    msg = None
    t_poll = time.time()

    for _ in range(30):
        time.sleep(2)
        poll_count += 1
        poll = requests.get(poll_url, headers=headers, timeout=15)
        if poll.status_code >= 400:
            continue
        msg = poll.json()
        status = msg.get("status", "")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break

    poll_ms = int((time.time() - t_poll) * 1000)
    final_status = msg.get("status", "TIMEOUT") if msg else "TIMEOUT"

    client.end_span(
        request_id=request_id, span_id=poll_span.span_id,
        outputs={"poll_count": poll_count, "poll_ms": poll_ms, "status": final_status},
        attributes={"poll_count": poll_count, "poll_ms": poll_ms, "final_status": final_status},
    )

    if msg and msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
        result = _format_response(msg, conv_id, headers)
    else:
        result = {"conversation_id": conv_id, "status": "TIMEOUT", "text": "Query is still processing."}

    total_ms = int((time.time() - pipeline_start) * 1000)
    result["performance"] = {
        "total_ms": total_ms,
        "api_call_ms": api_ms,
        "poll_ms": poll_ms,
        "poll_count": poll_count,
    }
    return result


def _genie_impl(question, conversation_id, base, headers, pipeline_start):
    """Non-traced fallback."""
    if conversation_id:
        url = f"{base}/conversations/{conversation_id}/messages"
    else:
        url = f"{base}/start-conversation"
    resp = requests.post(url, json={"content": question}, headers=headers, timeout=30)
    if resp.status_code >= 400:
        return {"status": "ERROR", "error": f"Genie API error: {resp.status_code}"}
    data = resp.json()
    conv_id = data.get("conversation_id", conversation_id)
    message_id = data.get("message_id", data.get("id"))
    if not conv_id or not message_id:
        return {"conversation_id": conv_id, "status": "ERROR", "error": "Missing IDs"}
    poll_url = f"{base}/conversations/{conv_id}/messages/{message_id}"
    for _ in range(30):
        time.sleep(2)
        poll = requests.get(poll_url, headers=headers, timeout=15)
        if poll.status_code >= 400:
            continue
        msg = poll.json()
        if msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            return _format_response(msg, conv_id, headers)
    return {"conversation_id": conv_id, "status": "TIMEOUT", "text": "Query is still processing."}


@router.post("/ask")
def ask_genie(body: AskRequest):
    if not GENIE_SPACE_ID:
        raise HTTPException(status_code=503, detail="GENIE_SPACE_ID not configured")
    refresh_databricks_token()
    result = _genie_query(body.question, body.conversation_id)
    if result.get("status") == "ERROR" and "Genie API error" in result.get("error", ""):
        raise HTTPException(status_code=500, detail=result["error"])
    return result


def _fetch_query_result(conversation_id, message_id, headers):
    url = f"{_base_url()}/conversations/{conversation_id}/messages/{message_id}/query-result"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code < 400:
            return resp.json()
    except Exception:
        pass
    return {}


def _fetch_statement_result(statement_id, headers):
    host = get_workspace_host().rstrip("/")
    url = f"{host}/api/2.0/sql/statements/{statement_id}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code < 400:
            return resp.json()
    except Exception:
        pass
    return {}


def _format_response(msg, conversation_id, headers):
    status = msg.get("status", "UNKNOWN")
    message_id = msg.get("id") or msg.get("message_id")
    result = {"conversation_id": conversation_id, "message_id": message_id, "status": status}
    if status == "FAILED":
        result["text"] = msg.get("error", {}).get("message", "Query failed.")
        return result
    sql = description = statement_id = ""
    suggested = []
    for att in msg.get("attachments", []):
        if "query" in att:
            q = att["query"]
            sql, description, statement_id = q.get("query", ""), q.get("description", ""), q.get("statement_id", "")
        if "suggested_questions" in att:
            suggested = att["suggested_questions"].get("questions", [])
        if "text" in att:
            result["text"] = att["text"].get("content", "")
    if sql: result["sql"] = sql
    if description: result["description"] = description
    if suggested: result["suggested_questions"] = suggested
    columns, rows = [], []
    if message_id:
        qr = _fetch_query_result(conversation_id, message_id, headers)
        if qr:
            columns, rows = _extract_table(qr.get("statement_response", qr))
    if not columns and statement_id:
        columns, rows = _extract_table(_fetch_statement_result(statement_id, headers))
    if not rows and sql:
        try:
            wr = execute_query(sql)
            if wr:
                columns = list(wr[0].keys())
                rows = [[str(r.get(c, "")) for c in columns] for r in wr]
        except Exception:
            pass
    if columns:
        result["columns"] = columns
        result["rows"] = rows
    if "text" not in result:
        result["text"] = description or "Query completed."
    return result


def _extract_table(data):
    columns, rows = [], []
    manifest = data.get("manifest", {})
    cols = manifest.get("schema", {}).get("columns", [])
    if cols: columns = [c.get("name", "") for c in cols]
    da = data.get("result", {}).get("data_array", [])
    if da: rows = da
    if not columns:
        cols = data.get("columns", [])
        if cols: columns = [c.get("name", c) if isinstance(c, dict) else str(c) for c in cols]
        da = data.get("data_array", [])
        if da: rows = da
    return columns, rows
