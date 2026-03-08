"""POST /api/feedback - Capture false positive/missed abuse feedback."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.warehouse import execute_query
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


@router.post("/feedback")
def submit_feedback(body: FeedbackRequest):
    if body.feedback_type not in ("false_positive", "missed_abuse"):
        raise HTTPException(status_code=400, detail="Invalid feedback_type")

    refresh_databricks_token()

    try:
        cat = get_catalog()
        safe_notes = body.notes.replace("'", "''")

        def _do_insert():
            execute_query(f"""
                INSERT INTO {cat}.refund_serving.refund_feedback
                SELECT uuid() AS feedback_id,
                       '{body.refund_id}' AS refund_id,
                       '{body.feedback_type}' AS feedback_type,
                       '{safe_notes}' AS notes,
                       current_timestamp() AS submitted_at
            """)

        if HAS_MLFLOW:
            with mlflow.start_span(name="submit_feedback", span_type="CHAIN") as span:
                span.set_attributes({
                    "refund_id": body.refund_id,
                    "feedback_type": body.feedback_type,
                })
                _do_insert()
        else:
            _do_insert()

        return {"status": "ok", "refund_id": body.refund_id, "feedback_type": body.feedback_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
