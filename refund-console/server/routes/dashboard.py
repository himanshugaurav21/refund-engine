"""GET /api/dashboard - KPI metrics and risk distribution."""

from fastapi import APIRouter, HTTPException

from server.warehouse import get_dashboard_metrics, get_risk_distribution

router = APIRouter(prefix="/api")


@router.get("/dashboard")
def dashboard():
    try:
        metrics = get_dashboard_metrics()
        risk_dist = get_risk_distribution()
        return {"metrics": metrics, "risk_distribution": risk_dist}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
