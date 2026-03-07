"""POST /api/agent/decide - Run AI agent on a case."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.agent import decide

router = APIRouter(prefix="/api")


class DecideRequest(BaseModel):
    refund_id: str


@router.post("/agent/decide")
def agent_decide(body: DecideRequest):
    try:
        result = decide(body.refund_id)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
