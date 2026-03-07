"""Genie Space proxy routes — start conversations, send messages, poll results."""

import os
import time
import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.config import get_workspace_host, get_oauth_token
from server.warehouse import execute_query

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


@router.post("/ask")
def ask_genie(body: AskRequest):
    """Send a question to Genie and poll until the answer is ready."""
    if not GENIE_SPACE_ID:
        raise HTTPException(status_code=503, detail="GENIE_SPACE_ID not configured")
    base = _base_url()
    headers = _headers()

    # Start or continue conversation
    if body.conversation_id:
        url = f"{base}/conversations/{body.conversation_id}/messages"
    else:
        url = f"{base}/start-conversation"

    resp = requests.post(url, json={"content": body.question}, headers=headers, timeout=30)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    data = resp.json()
    conversation_id = data.get("conversation_id", body.conversation_id)
    message_id = data.get("message_id", data.get("id"))

    if not conversation_id or not message_id:
        return {"conversation_id": conversation_id, "status": "ERROR", "error": "Missing IDs"}

    # Poll for result (up to 60s)
    poll_url = f"{base}/conversations/{conversation_id}/messages/{message_id}"
    for _ in range(30):
        time.sleep(2)
        poll = requests.get(poll_url, headers=headers, timeout=15)
        if poll.status_code >= 400:
            continue
        msg = poll.json()
        status = msg.get("status", "")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            return _format_response(msg, conversation_id, headers)

    return {"conversation_id": conversation_id, "status": "TIMEOUT", "text": "Query is still processing. Try again shortly."}


def _fetch_query_result(conversation_id: str, message_id: str, headers: dict) -> dict:
    """Fetch query result data for a completed Genie message."""
    url = f"{_base_url()}/conversations/{conversation_id}/messages/{message_id}/query-result"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code < 400:
            return resp.json()
    except Exception:
        pass
    return {}


def _fetch_statement_result(statement_id: str, headers: dict) -> dict:
    """Fetch results via SQL statements API as fallback."""
    host = get_workspace_host().rstrip("/")
    url = f"{host}/api/2.0/sql/statements/{statement_id}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code < 400:
            return resp.json()
    except Exception:
        pass
    return {}


def _format_response(msg: dict, conversation_id: str, headers: dict) -> dict:
    """Extract the useful parts from a Genie message response."""
    status = msg.get("status", "UNKNOWN")
    message_id = msg.get("id") or msg.get("message_id")
    result: dict = {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "status": status,
    }

    if status == "FAILED":
        result["text"] = msg.get("error", {}).get("message", "Query failed.")
        return result

    sql = ""
    description = ""
    statement_id = ""
    suggested = []

    for attachment in msg.get("attachments", []):
        # Query attachment
        if "query" in attachment:
            q = attachment["query"]
            sql = q.get("query", "")
            description = q.get("description", "")
            statement_id = q.get("statement_id", "")

        # Suggested follow-ups
        if "suggested_questions" in attachment:
            suggested = attachment["suggested_questions"].get("questions", [])

        # Text attachment
        if "text" in attachment:
            result["text"] = attachment["text"].get("content", "")

    if sql:
        result["sql"] = sql
    if description:
        result["description"] = description
    if suggested:
        result["suggested_questions"] = suggested

    # Fetch query results
    columns = []
    rows = []

    if message_id:
        qr = _fetch_query_result(conversation_id, message_id, headers)
        if qr:
            # query-result endpoint returns statement result directly
            stmt_data = qr.get("statement_response", qr)
            columns, rows = _extract_table(stmt_data)

    # Fallback: fetch via statement ID
    if not columns and statement_id:
        stmt_data = _fetch_statement_result(statement_id, headers)
        columns, rows = _extract_table(stmt_data)

    # Fallback: re-execute the SQL via warehouse if we have SQL but no rows
    if not rows and sql:
        try:
            warehouse_rows = execute_query(sql)
            if warehouse_rows:
                columns = list(warehouse_rows[0].keys())
                rows = [[str(row.get(c, "")) for c in columns] for row in warehouse_rows]
        except Exception:
            pass

    if columns:
        result["columns"] = columns
        result["rows"] = rows

    # Set text if not already set
    if "text" not in result:
        result["text"] = description or "Query completed."

    return result


def _extract_table(data: dict) -> tuple[list[str], list[list[str]]]:
    """Extract columns and rows from a SQL statement result or Genie query result."""
    columns = []
    rows = []

    # Try manifest + result (SQL statements API format)
    manifest = data.get("manifest", {})
    schema = manifest.get("schema", {})
    cols = schema.get("columns", [])
    if cols:
        columns = [c.get("name", "") for c in cols]

    result = data.get("result", {})
    data_array = result.get("data_array", [])
    if data_array:
        rows = data_array

    # Try columns + data_array directly (Genie query-result format)
    if not columns:
        cols = data.get("columns", [])
        if cols:
            columns = [c.get("name", c) if isinstance(c, dict) else str(c) for c in cols]
        da = data.get("data_array", [])
        if da:
            rows = da

    return columns, rows
