"""POST /api/cases/{refund_id}/action - CSR action on a case."""

import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.warehouse import update_case_action, get_case_detail
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


def _do_action(refund_id: str, action: str, reason: str) -> dict:
    """Execute the CSR action with full context and override detection."""
    pipeline_start = time.time()

    if not HAS_MLFLOW:
        return _do_action_impl(refund_id, action, reason)

    client = mlflow.MlflowClient()
    root = client.start_trace(
        name="csr_action",
        inputs={"refund_id": refund_id, "action": action, "reason": reason},
    )
    request_id = root.request_id

    try:
        result = _do_action_with_spans(refund_id, action, reason,
                                        client, request_id, root.span_id, pipeline_start)
        total_ms = int((time.time() - pipeline_start) * 1000)

        client.end_trace(
            request_id=request_id,
            outputs=result,
            attributes={
                "csr.refund_id": refund_id,
                "csr.action": action,
                "csr.is_override": result.get("is_override", False),
                "csr.ai_recommended": result.get("ai_recommended", ""),
                "csr.risk_score": result.get("risk_score", 0),
                "csr.amount": result.get("amount", 0),
                "csr.total_ms": total_ms,
            },
        )

        # Set trace tags
        try:
            perf = result.get("performance", {})
            for k, v in {
                "csr.refund_id": refund_id,
                "csr.action": action,
                "csr.reason": reason[:200],
                "csr.ai_recommended_action": result.get("ai_recommended", ""),
                "csr.is_override": str(result.get("is_override", False)),
                "csr.risk_score": str(result.get("risk_score", 0)),
                "csr.risk_tier": result.get("risk_tier", ""),
                "csr.amount": str(result.get("amount", 0)),
                "csr.total_ms": str(total_ms),
                "csr.case_lookup_ms": str(perf.get("case_lookup_ms", 0)),
                "csr.db_update_ms": str(perf.get("db_update_ms", 0)),
            }.items():
                client.set_trace_tag(request_id, k, v)
        except Exception:
            pass

        return result
    except Exception as e:
        client.end_trace(request_id=request_id, outputs={"error": str(e)}, attributes={"error": True})
        raise


def _do_action_with_spans(refund_id, action, reason, client, request_id, parent_span_id, pipeline_start):
    """Execute CSR action with explicit client-based spans."""

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

    ai_recommended = ""
    risk_score = 0.0
    risk_tier = ""
    amount = 0.0
    is_override = False

    if case:
        ai_recommended = str(case.get("recommended_action", "")).lower()
        risk_score = float(case.get("risk_score") or 0)
        risk_tier = str(case.get("abuse_risk_tier", ""))
        amount = float(case.get("amount") or 0)
        action_map = {"approve": "approved", "reject": "rejected", "escalate": "escalated"}
        ai_normalized = action_map.get(ai_recommended, ai_recommended)
        is_override = bool(ai_recommended and ai_normalized != action)

    client.end_span(
        request_id=request_id, span_id=case_span.span_id,
        outputs={
            "found": case is not None,
            "ai_recommended": ai_recommended,
            "risk_score": risk_score,
            "risk_tier": risk_tier,
            "amount": amount,
            "is_override": is_override,
        },
        attributes={
            "latency_ms": case_ms,
            "customer_id": str(case.get("customer_id", "")) if case else "",
            "reason_code": str(case.get("reason_code", "")) if case else "",
            "channel": str(case.get("channel", "")) if case else "",
        },
    )

    # --- Override Detection Span ---
    override_span = client.start_span(
        request_id=request_id, name="override_detection", span_type="TOOL",
        parent_id=parent_span_id,
        inputs={"csr_action": action, "ai_recommended": ai_recommended},
    )
    client.end_span(
        request_id=request_id, span_id=override_span.span_id,
        outputs={"is_override": is_override, "ai_recommended": ai_recommended, "csr_action": action},
        attributes={"is_override": is_override},
    )

    # --- DB Update Span ---
    update_span = client.start_span(
        request_id=request_id, name="db_update_case_action", span_type="TOOL",
        parent_id=parent_span_id,
        inputs={"refund_id": refund_id, "action": action, "reason": reason[:200]},
    )
    t_update = time.time()
    success = update_case_action(refund_id, action, reason)
    update_ms = int((time.time() - t_update) * 1000)
    client.end_span(
        request_id=request_id, span_id=update_span.span_id,
        outputs={"success": success},
        attributes={"latency_ms": update_ms, "success": success},
    )

    total_ms = int((time.time() - pipeline_start) * 1000)

    return {
        "success": success,
        "refund_id": refund_id,
        "action": action,
        "reason": reason,
        "is_override": is_override,
        "ai_recommended": ai_recommended,
        "risk_score": risk_score,
        "risk_tier": risk_tier,
        "amount": amount,
        "performance": {
            "total_ms": total_ms,
            "case_lookup_ms": case_ms,
            "db_update_ms": update_ms,
        },
    }


def _do_action_impl(refund_id: str, action: str, reason: str) -> dict:
    """No-tracing fallback."""
    case = None
    try:
        case = get_case_detail(refund_id)
    except Exception:
        pass

    ai_recommended = ""
    is_override = False
    risk_score = 0.0
    amount = 0.0

    if case:
        ai_recommended = str(case.get("recommended_action", "")).lower()
        risk_score = float(case.get("risk_score") or 0)
        amount = float(case.get("amount") or 0)
        action_map = {"approve": "approved", "reject": "rejected", "escalate": "escalated"}
        ai_normalized = action_map.get(ai_recommended, ai_recommended)
        is_override = bool(ai_recommended and ai_normalized != action)

    success = update_case_action(refund_id, action, reason)
    return {
        "success": success, "refund_id": refund_id, "action": action,
        "reason": reason, "is_override": is_override, "ai_recommended": ai_recommended,
        "risk_score": risk_score, "amount": amount,
    }


@router.post("/cases/{refund_id}/action")
def take_action(refund_id: str, body: ActionRequest):
    if body.action not in ("approved", "rejected", "escalated"):
        raise HTTPException(status_code=400, detail="Invalid action")

    refresh_databricks_token()
    result = _do_action(refund_id, body.action, body.reason)

    if not result["success"]:
        raise HTTPException(status_code=500, detail="Failed to update case")

    return {"status": "ok", "refund_id": refund_id, "action": body.action}
