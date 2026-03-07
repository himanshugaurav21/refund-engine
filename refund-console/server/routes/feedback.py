"""POST /api/feedback - Capture false positive/missed abuse feedback."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.warehouse import execute_query
from server.config import get_catalog

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

    try:
        cat = get_catalog()
        safe_notes = body.notes.replace("'", "''")
        execute_query(f"""
            INSERT INTO {cat}.refund_serving.refund_feedback
            SELECT uuid() AS feedback_id,
                   '{body.refund_id}' AS refund_id,
                   '{body.feedback_type}' AS feedback_type,
                   '{safe_notes}' AS notes,
                   current_timestamp() AS submitted_at
        """)
        return {"status": "ok", "refund_id": body.refund_id, "feedback_type": body.feedback_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
