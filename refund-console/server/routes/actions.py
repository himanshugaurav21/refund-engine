"""POST /api/cases/{refund_id}/action - CSR action on a case."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.warehouse import update_case_action
from server.config import refresh_databricks_token

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

router = APIRouter(prefix="/api")


class ActionRequest(BaseModel):
    action: str  # approved, rejected, escalated
    reason: str


@router.post("/cases/{refund_id}/action")
def take_action(refund_id: str, body: ActionRequest):
    if body.action not in ("approved", "rejected", "escalated"):
        raise HTTPException(status_code=400, detail="Invalid action")

    refresh_databricks_token()

    if HAS_MLFLOW:
        with mlflow.start_span(name="csr_action", span_type="CHAIN") as span:
            span.set_attributes({
                "refund_id": refund_id,
                "csr_action": body.action,
                "csr_reason": body.reason,
            })
            success = update_case_action(refund_id, body.action, body.reason)
            span.set_attributes({"success": success})
    else:
        success = update_case_action(refund_id, body.action, body.reason)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to update case")

    return {"status": "ok", "refund_id": refund_id, "action": body.action}
