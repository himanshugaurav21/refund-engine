"""POST /api/cases/{refund_id}/action - CSR action on a case."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.warehouse import update_case_action

router = APIRouter(prefix="/api")


class ActionRequest(BaseModel):
    action: str  # approved, rejected, escalated
    reason: str


@router.post("/cases/{refund_id}/action")
def take_action(refund_id: str, body: ActionRequest):
    if body.action not in ("approved", "rejected", "escalated"):
        raise HTTPException(status_code=400, detail="Invalid action")

    success = update_case_action(refund_id, body.action, body.reason)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update case")

    return {"status": "ok", "refund_id": refund_id, "action": body.action}
