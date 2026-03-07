"""GET /api/cases - Paginated case list with filters.
   GET /api/cases/{refund_id} - Full case detail."""

from fastapi import APIRouter, HTTPException, Query

from server.warehouse import get_cases, get_case_detail

router = APIRouter(prefix="/api")


@router.get("/cases")
def list_cases(
    status: str | None = Query(None),
    risk_tier: str | None = Query(None),
    channel: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        cases = get_cases(status=status, risk_tier=risk_tier,
                         channel=channel, limit=limit, offset=offset)
        return {"cases": cases, "count": len(cases)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cases/{refund_id}")
def case_detail(refund_id: str):
    try:
        case = get_case_detail(refund_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case not found")
        return case
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
