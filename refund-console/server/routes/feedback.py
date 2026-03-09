"""POST /api/feedback - Capture false positive/missed abuse feedback."""

import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.warehouse import execute_query, get_case_detail
from server.config import get_catalog, refresh_databricks_token

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

router = APIRouter(prefix="/api")


class FeedbackRequest(BaseModel):
    refund_id: str
    feedback_type: str  # false_positive, missed_abuse
    notes: str = ""


@router.get("/feedback")
def list_feedback():
    try:
        cat = get_catalog()
        rows = execute_query(f"""
            SELECT feedback_id, refund_id, feedback_type, notes, submitted_at
            FROM {cat}.refund_serving.refund_feedback
            ORDER BY submitted_at DESC
            LIMIT 100
        """)
        return {"feedback": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _submit_feedback(refund_id: str, feedback_type: str, notes: str) -> dict:
    """Insert feedback record with full case context for tracing."""
    pipeline_start = time.time()

    if not HAS_MLFLOW:
        return _submit_feedback_impl(refund_id, feedback_type, notes)

    client = mlflow.MlflowClient()
    root = client.start_trace(
        name="submit_feedback",
        inputs={"refund_id": refund_id, "feedback_type": feedback_type, "notes": notes[:200]},
    )
    request_id = root.request_id

    try:
        result = _submit_feedback_with_spans(refund_id, feedback_type, notes,
                                              client, request_id, root.span_id, pipeline_start)
        total_ms = int((time.time() - pipeline_start) * 1000)

        client.end_trace(
            request_id=request_id,
            outputs=result,
            attributes={
                "feedback.refund_id": refund_id,
                "feedback.type": feedback_type,
                "feedback.total_ms": total_ms,
            },
        )

        # Set trace tags
        try:
            perf = result.get("performance", {})
            for k, v in {
                "feedback.refund_id": refund_id,
                "feedback.type": feedback_type,
                "feedback.notes": notes[:200],
                "feedback.total_ms": str(total_ms),
                "feedback.case_lookup_ms": str(perf.get("case_lookup_ms", 0)),
                "feedback.db_insert_ms": str(perf.get("db_insert_ms", 0)),
                "feedback.risk_score": str(result.get("_risk_score", 0)),
                "feedback.amount": str(result.get("_amount", 0)),
            }.items():
                client.set_trace_tag(request_id, k, v)
        except Exception:
            pass

        # Remove internal fields
        result.pop("_risk_score", None)
        result.pop("_amount", None)
        return result
    except Exception as e:
        client.end_trace(request_id=request_id, outputs={"error": str(e)}, attributes={"error": True})
        raise


def _submit_feedback_with_spans(refund_id, feedback_type, notes,
                                 client, request_id, parent_span_id, pipeline_start):
    """Execute feedback submission with explicit client-based spans."""

    # --- Case Lookup Span ---
    case_span = client.start_span(
        request_id=request_id, name="case_lookup", span_type="TOOL",
        parent_id=parent_span_id,
        inputs={"refund_id": refund_id},
    )
    t_case = time.time()
    case = None
    try:
        case = get_case_detail(refund_id)
    except Exception:
        pass
    case_ms = int((time.time() - t_case) * 1000)

    risk_score = float(case.get("risk_score") or 0) if case else 0
    amount = float(case.get("amount") or 0) if case else 0

    client.end_span(
        request_id=request_id, span_id=case_span.span_id,
        outputs={
            "found": case is not None,
            "risk_score": risk_score,
            "amount": amount,
            "risk_tier": str(case.get("abuse_risk_tier", "")) if case else "",
            "recommended_action": str(case.get("recommended_action", "")) if case else "",
        },
        attributes={
            "latency_ms": case_ms,
            "customer_id": str(case.get("customer_id", "")) if case else "",
            "reason_code": str(case.get("reason_code", "")) if case else "",
            "channel": str(case.get("channel", "")) if case else "",
            "product_category": str(case.get("product_category", "")) if case else "",
        },
    )

    # --- DB Insert Span ---
    insert_span = client.start_span(
        request_id=request_id, name="db_insert_feedback", span_type="TOOL",
        parent_id=parent_span_id,
        inputs={"refund_id": refund_id, "feedback_type": feedback_type, "notes": notes[:200]},
    )
    t_insert = time.time()
    cat = get_catalog()
    safe_notes = notes.replace("'", "''")
    execute_query(f"""
        INSERT INTO {cat}.refund_serving.refund_feedback
        SELECT uuid() AS feedback_id,
               '{refund_id}' AS refund_id,
               '{feedback_type}' AS feedback_type,
               '{safe_notes}' AS notes,
               current_timestamp() AS submitted_at
    """)
    insert_ms = int((time.time() - t_insert) * 1000)

    client.end_span(
        request_id=request_id, span_id=insert_span.span_id,
        outputs={"inserted": True},
        attributes={"latency_ms": insert_ms},
    )

    total_ms = int((time.time() - pipeline_start) * 1000)

    return {
        "status": "ok",
        "refund_id": refund_id,
        "feedback_type": feedback_type,
        "_risk_score": risk_score,
        "_amount": amount,
        "performance": {
            "total_ms": total_ms,
            "case_lookup_ms": case_ms,
            "db_insert_ms": insert_ms,
        },
    }


def _submit_feedback_impl(refund_id: str, feedback_type: str, notes: str) -> dict:
    """No-tracing fallback."""
    cat = get_catalog()
    safe_notes = notes.replace("'", "''")
    execute_query(f"""
        INSERT INTO {cat}.refund_serving.refund_feedback
        SELECT uuid() AS feedback_id,
               '{refund_id}' AS refund_id,
               '{feedback_type}' AS feedback_type,
               '{safe_notes}' AS notes,
               current_timestamp() AS submitted_at
    """)
    return {"status": "ok", "refund_id": refund_id, "feedback_type": feedback_type}


@router.post("/feedback")
def submit_feedback(body: FeedbackRequest):
    if body.feedback_type not in ("false_positive", "missed_abuse"):
        raise HTTPException(status_code=400, detail="Invalid feedback_type")

    refresh_databricks_token()

    try:
        return _submit_feedback(body.refund_id, body.feedback_type, body.notes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
