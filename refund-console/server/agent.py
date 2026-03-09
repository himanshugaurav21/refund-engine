"""4-step refund decisioning agent with MLflow tracing."""

import json
import os
import time
from server.warehouse import execute_query, get_case_detail
from server.llm import chat_completion, get_last_llm_metrics
from server.config import get_catalog

try:
    import mlflow
    from mlflow.entities import SpanType
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False


def decide(refund_id: str) -> dict:
    """Run the full 4-step decisioning pipeline with client-based tracing."""
    t0 = time.time()

    if not HAS_MLFLOW:
        return _decide_impl(refund_id)

    client = mlflow.MlflowClient()

    # Start the root trace span
    root = client.start_trace(
        name="refund_decisioning_pipeline",
        inputs={"refund_id": refund_id},
    )
    request_id = getattr(root, 'trace_id', None) or root.request_id

    try:
        result = _decide_with_spans(refund_id, client, request_id, root.span_id)
        total_ms = int((time.time() - t0) * 1000)

        # Set root span outputs + attributes
        client.end_trace(
            request_id=request_id,
            outputs=result,
            attributes={
                "refund_id": refund_id,
                "total_ms": total_ms,
                "risk_score": result.get("risk", {}).get("risk_score", 0),
                "risk_tier": result.get("risk", {}).get("risk_tier", "UNKNOWN"),
                "recommended_action": result.get("recommendation", {}).get("action", "UNKNOWN"),
                "confidence": result.get("recommendation", {}).get("confidence", 0),
                "num_abuse_signals": len(result.get("risk", {}).get("signals", [])),
                "validation_passed": result.get("validation", {}).get("valid", False),
                "auto_approvable": result.get("policy", {}).get("auto_approvable", False),
            },
        )

        # Set trace-level tags
        try:
            perf = result.get("performance", {})
            rec = result.get("recommendation", {})
            risk = result.get("risk", {})
            llm = rec.get("llm_stats", {})
            for k, v in {
                "refund_id": refund_id,
                "amount": str(result.get("_case", {}).get("amount", "")),
                "risk_score": str(risk.get("risk_score", "")),
                "risk_tier": str(risk.get("risk_tier", "")),
                "recommended_action": str(rec.get("action", "")),
                "confidence": str(rec.get("confidence", "")),
                "total_ms": str(perf.get("total_ms", "")),
                "case_lookup_ms": str(perf.get("case_lookup_ms", "")),
                "step1_validation_ms": str(perf.get("step1_validation_ms", "")),
                "step2_policy_ms": str(perf.get("step2_policy_ms", "")),
                "step3_risk_ms": str(perf.get("step3_risk_ms", "")),
                "step4_llm_ms": str(perf.get("step4_llm_ms", "")),
                "llm_model": str(llm.get("model", "")),
                "llm_latency_ms": str(llm.get("latency_ms", "")),
                "llm_input_tokens": str(llm.get("input_tokens", "")),
                "llm_output_tokens": str(llm.get("output_tokens", "")),
                "llm_total_tokens": str(llm.get("total_tokens", "")),
                "llm_fallback": str(llm.get("fallback", False)),
            }.items():
                client.set_trace_tag(request_id, k, v)
        except Exception:
            pass

        # Remove internal _case from output
        result.pop("_case", None)
        return result

    except Exception as e:
        client.end_trace(
            request_id=request_id,
            outputs={"error": str(e)},
            attributes={"error": True},
        )
        raise


def _decide_with_spans(refund_id: str, client, request_id: str, parent_span_id: str) -> dict:
    """Execute pipeline with explicit client-based child spans."""

    # --- Case Lookup Span ---
    case_span = client.start_span(
        request_id=request_id,
        name="case_lookup",
        parent_id=parent_span_id,
        inputs={"refund_id": refund_id},
    )
    t_case = time.time()
    case = get_case_detail(refund_id)
    case_ms = int((time.time() - t_case) * 1000)

    if not case:
        client.end_span(request_id=request_id, span_id=case_span.span_id,
                        outputs={"found": False}, attributes={"latency_ms": case_ms})
        return {"error": "Refund not found", "refund_id": refund_id}

    client.end_span(request_id=request_id, span_id=case_span.span_id,
                    outputs={"found": True, "customer_id": case.get("customer_id")},
                    attributes={"latency_ms": case_ms, "customer_id": str(case.get("customer_id", ""))})

    customer_360 = case.get("customer_360", {})

    # --- Step 1: Validation Span ---
    s1 = client.start_span(
        request_id=request_id,
        name="step1_transaction_validation",
        parent_id=parent_span_id,
        inputs={"order_id": case.get("order_id"), "within_window": case.get("within_return_window"),
                "days_since_order": case.get("days_since_order"), "item_condition": case.get("item_condition")},
    )
    t1 = time.time()
    validation = _step1(case)
    ms1 = int((time.time() - t1) * 1000)
    client.end_span(request_id=request_id, span_id=s1.span_id,
                    outputs=validation, attributes={"latency_ms": ms1, "passed": validation["valid"]})

    # --- Step 2: Policy Span ---
    s2 = client.start_span(
        request_id=request_id,
        name="step2_policy_compliance",
        parent_id=parent_span_id,
        inputs={"amount": case.get("amount"), "is_high_value": case.get("is_high_value"),
                "within_window": case.get("within_return_window"), "reason_code": case.get("reason_code")},
    )
    t2 = time.time()
    policy = _step2(case)
    ms2 = int((time.time() - t2) * 1000)
    client.end_span(request_id=request_id, span_id=s2.span_id,
                    outputs=policy, attributes={"latency_ms": ms2, "auto_approvable": policy["auto_approvable"]})

    # --- Step 3: Risk Scoring Span ---
    s3 = client.start_span(
        request_id=request_id,
        name="step3_abuse_risk_scoring",
        parent_id=parent_span_id,
        inputs={"risk_score": case.get("risk_score"), "risk_tier": customer_360.get("risk_tier"),
                "refund_rate": customer_360.get("refund_rate"), "refunds_90d": customer_360.get("refunds_90d")},
    )
    t3 = time.time()
    risk = _step3(case, customer_360)
    ms3 = int((time.time() - t3) * 1000)
    client.end_span(request_id=request_id, span_id=s3.span_id,
                    outputs=risk,
                    attributes={"latency_ms": ms3, "risk_score": risk["risk_score"],
                                "risk_tier": risk["risk_tier"], "signal_count": len(risk["signals"])})

    # --- Step 4: LLM Recommendation Span ---
    model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4")
    s4 = client.start_span(
        request_id=request_id,
        name="step4_llm_recommendation",
        span_type=SpanType.LLM,
        parent_id=parent_span_id,
        inputs={"model": model, "validation": validation, "policy": policy, "risk": risk},
    )
    t4 = time.time()
    recommendation = _step4(validation, policy, risk, case)
    ms4 = int((time.time() - t4) * 1000)

    llm_stats = recommendation.get("llm_stats", {})
    client.end_span(
        request_id=request_id, span_id=s4.span_id,
        outputs=recommendation,
        attributes={
            "latency_ms": ms4,
            "llm.model": model,
            "llm.latency_ms": llm_stats.get("latency_ms", 0),
            "llm.input_tokens": llm_stats.get("input_tokens", 0),
            "llm.output_tokens": llm_stats.get("output_tokens", 0),
            "llm.total_tokens": llm_stats.get("total_tokens", 0),
            "llm.prompt_length": llm_stats.get("prompt_length", 0),
            "llm.response_length": llm_stats.get("response_length", 0),
            "llm.fallback": llm_stats.get("fallback", False),
            "recommendation_action": recommendation.get("action", ""),
            "recommendation_confidence": recommendation.get("confidence", 0),
        },
    )

    total_ms = int((time.time() - t_case) * 1000)

    return {
        "refund_id": refund_id,
        "validation": validation,
        "policy": policy,
        "risk": risk,
        "recommendation": recommendation,
        "performance": {
            "total_ms": total_ms,
            "case_lookup_ms": case_ms,
            "step1_validation_ms": ms1,
            "step2_policy_ms": ms2,
            "step3_risk_ms": ms3,
            "step4_llm_ms": ms4,
        },
        "_case": case,  # Internal, removed before returning
    }


# --- Pure business logic (no tracing) ---

def _step1(case: dict) -> dict:
    issues = []
    if case.get("order_id") is None:
        issues.append("No matching order found")
    if case.get("within_return_window") in ("false", "False", False):
        issues.append(f"Outside return window ({case.get('days_since_order')} days)")
    if case.get("item_condition") == "missing":
        issues.append("Item reported as missing")
    return {"valid": len(issues) == 0, "issues": issues}


def _step2(case: dict) -> dict:
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


def _step3(case: dict, customer_360: dict) -> dict:
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
    return {"risk_score": risk_score, "risk_tier": risk_tier, "signals": signals}


def _step4(validation: dict, policy: dict, risk: dict, case: dict) -> dict:
    model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4")
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
        t0 = time.time()
        raw = chat_completion([{"role": "user", "content": context}], max_tokens=512, temperature=0.2)
        latency_ms = int((time.time() - t0) * 1000)
        llm_metrics = get_last_llm_metrics()
        start = raw.index("{")
        end = raw.rindex("}") + 1
        rec = json.loads(raw[start:end])
        rec["llm_stats"] = {
            "model": model, "latency_ms": latency_ms,
            "prompt_length": len(context), "response_length": len(raw), "fallback": False,
            "input_tokens": llm_metrics.get("input_tokens", 0),
            "output_tokens": llm_metrics.get("output_tokens", 0),
            "total_tokens": llm_metrics.get("total_tokens", 0),
        }
        return rec
    except Exception as e:
        print(f"LLM recommendation error: {e}")
        return {
            "action": case.get("recommended_action", "ESCALATE"),
            "confidence": risk.get("risk_score", 0.5),
            "explanation": f"Rule-based recommendation (LLM unavailable): {risk.get('signals', [])}",
            "key_factors": risk.get("signals", []),
            "llm_stats": {"model": model, "fallback": True, "error": str(e)},
        }


def _decide_impl(refund_id: str) -> dict:
    """No-tracing fallback."""
    case = get_case_detail(refund_id)
    if not case:
        return {"error": "Refund not found", "refund_id": refund_id}
    customer_360 = case.get("customer_360", {})
    validation = _step1(case)
    policy = _step2(case)
    risk = _step3(case, customer_360)
    recommendation = _step4(validation, policy, risk, case)
    return {
        "refund_id": refund_id, "validation": validation,
        "policy": policy, "risk": risk, "recommendation": recommendation,
    }
