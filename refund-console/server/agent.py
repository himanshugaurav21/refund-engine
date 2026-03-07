"""4-step refund decisioning agent with MLflow tracing."""

import json
import os
from server.warehouse import execute_query, get_case_detail
from server.llm import chat_completion
from server.config import get_catalog

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False


def _trace(name, span_type="CHAIN"):
    """Decorator that applies mlflow.trace if available."""
    if HAS_MLFLOW:
        return mlflow.trace(name=name, span_type=span_type)
    def noop(fn):
        return fn
    return noop


@_trace("step1_transaction_validation")
def step1_validate(case: dict) -> dict:
    """Step 1: Transaction Validation."""
    issues = []
    if case.get("order_id") is None:
        issues.append("No matching order found")
    if case.get("within_return_window") in ("false", "False", False):
        issues.append(f"Outside return window ({case.get('days_since_order')} days)")
    if case.get("item_condition") == "missing":
        issues.append("Item reported as missing")
    return {"valid": len(issues) == 0, "issues": issues}


@_trace("step2_policy_compliance")
def step2_policy(case: dict) -> dict:
    """Step 2: Policy Compliance."""
    amount = float(case.get("amount") or 0)
    is_high_value = case.get("is_high_value") in ("true", "True", True)
    within_window = case.get("within_return_window") in ("true", "True", True)

    result = {"auto_approvable": False, "policy_flags": []}

    if amount <= 25 and within_window and not is_high_value:
        result["auto_approvable"] = True
        result["policy_flags"].append("Below auto-approve threshold ($25)")

    if is_high_value:
        result["policy_flags"].append("High-value item requires manual review")
    if not within_window:
        result["policy_flags"].append("Outside return window")
    if case.get("reason_code") == "UNAUTHORIZED_PURCHASE":
        result["policy_flags"].append("Unauthorized purchase - fraud team review")

    return result


@_trace("step3_abuse_risk_scoring")
def step3_risk(case: dict, customer_360: dict) -> dict:
    """Step 3: Abuse Risk Scoring."""
    risk_score = float(case.get("risk_score") or 0)
    signals = []

    risk_tier = customer_360.get("risk_tier", "UNKNOWN")
    if risk_tier in ("HIGH", "CRITICAL"):
        signals.append(f"Customer risk tier: {risk_tier}")

    refund_rate = float(customer_360.get("refund_rate") or 0)
    if refund_rate > 0.25:
        signals.append(f"High refund rate: {refund_rate:.1%}")

    refunds_90d = int(customer_360.get("refunds_90d") or 0)
    if refunds_90d > 5:
        signals.append(f"Frequent refunder: {refunds_90d} in 90 days")

    if customer_360.get("coordinated_timing_flag") in ("true", "True", True):
        signals.append("Household coordination detected")

    if (case.get("reason_code") == "ITEM_NOT_RECEIVED" and
            case.get("delivery_confirmed") in ("true", "True", True)):
        signals.append("Claims non-delivery but delivery confirmed with photo")

    return {
        "risk_score": risk_score,
        "risk_tier": risk_tier,
        "signals": signals,
    }


@_trace("step4_llm_recommendation", span_type="LLM")
def step4_recommend(validation: dict, policy: dict, risk: dict, case: dict) -> dict:
    """Step 4: LLM Recommendation."""
    context = f"""You are a refund abuse analyst. Provide a structured recommendation.

REFUND REQUEST:
- Refund ID: {case.get('refund_id')}
- Amount: ${case.get('amount')}
- Reason: {case.get('reason_code')}
- Product: {case.get('product_name')} ({case.get('product_category')})
- Channel: {case.get('channel', 'N/A')}
- Days since order: {case.get('days_since_order')}
- Item condition: {case.get('item_condition')}

VALIDATION: Valid={validation.get('valid')}, Issues={', '.join(validation.get('issues', [])) or 'None'}
POLICY: Auto-approvable={policy.get('auto_approvable')}, Flags={', '.join(policy.get('policy_flags', [])) or 'None'}
RISK: Score={risk.get('risk_score')}/1.0, Tier={risk.get('risk_tier')}, Signals={', '.join(risk.get('signals', [])) or 'None'}

Respond with EXACTLY this JSON:
{{"action": "APPROVE" or "REJECT" or "ESCALATE", "confidence": 0.0-1.0, "explanation": "2-3 sentence explanation", "key_factors": ["factor1", "factor2"]}}"""

    try:
        raw = chat_completion([{"role": "user", "content": context}], max_tokens=512, temperature=0.2)
        start = raw.index("{")
        end = raw.rindex("}") + 1
        return json.loads(raw[start:end])
    except Exception as e:
        print(f"LLM recommendation error: {e}")
        return {
            "action": case.get("recommended_action", "ESCALATE"),
            "confidence": risk.get("risk_score", 0.5),
            "explanation": f"Rule-based recommendation (LLM unavailable): {risk.get('signals', [])}",
            "key_factors": risk.get("signals", []),
        }


@_trace("refund_decisioning_pipeline")
def decide(refund_id: str) -> dict:
    """Run the full 4-step decisioning pipeline."""
    case = get_case_detail(refund_id)
    if not case:
        return {"error": "Refund not found", "refund_id": refund_id}

    customer_360 = case.get("customer_360", {})

    validation = step1_validate(case)
    policy = step2_policy(case)
    risk = step3_risk(case, customer_360)
    recommendation = step4_recommend(validation, policy, risk, case)

    result = {
        "refund_id": refund_id,
        "validation": validation,
        "policy": policy,
        "risk": risk,
        "recommendation": recommendation,
    }

    # Log the decision as an MLflow trace attribute
    if HAS_MLFLOW:
        try:
            span = mlflow.get_current_active_span()
            if span:
                span.set_attributes({
                    "refund_id": refund_id,
                    "risk_score": risk.get("risk_score", 0),
                    "risk_tier": risk.get("risk_tier", "UNKNOWN"),
                    "recommended_action": recommendation.get("action", "UNKNOWN"),
                    "confidence": recommendation.get("confidence", 0),
                    "num_abuse_signals": len(risk.get("signals", [])),
                    "validation_passed": validation.get("valid", False),
                    "auto_approvable": policy.get("auto_approvable", False),
                })
        except Exception:
            pass

    return result
