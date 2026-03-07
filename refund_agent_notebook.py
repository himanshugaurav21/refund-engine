# Databricks notebook source
# MAGIC %md
# MAGIC # Refund Decisioning Agent
# MAGIC 4-step deterministic pipeline with LLM explanation synthesis.
# MAGIC Logged as an MLflow PythonModel for observability.

# COMMAND ----------

# MAGIC %pip install mlflow openai databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

import json
import os
import mlflow
from mlflow.pyfunc import PythonModel

# COMMAND ----------

CATALOG = "refund_decisioning"
SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4")

# Set MLflow experiment
experiment_path = "/Users/himanshu.gaurav@databricks.com/refund-engine/refund-agent"
mlflow.set_experiment(experiment_path)

# COMMAND ----------

class RefundDecisioningAgent(PythonModel):
    """4-step refund decisioning agent with MLflow tracing."""

    def __init__(self):
        self.catalog = CATALOG
        self.endpoint = SERVING_ENDPOINT

    def _get_sql_client(self):
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        return w

    def _query(self, w, sql):
        """Execute SQL and return rows as dicts."""
        from databricks.sdk.service.sql import StatementState
        wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "2dba0d7005ff019f")
        resp = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=sql,
            wait_timeout="30s",
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            raise RuntimeError(f"SQL failed: {resp.status.error.message}")
        if not resp.result or not resp.manifest:
            return []
        cols = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in resp.result.data_array]

    @mlflow.trace(name="step1_transaction_validation")
    def step1_validate(self, w, refund_id):
        """Step 1: Transaction Validation - Order exists? Within return window? Item eligible?"""
        rows = self._query(w, f"""
            SELECT refund_id, order_id, customer_id, amount, reason_code,
                   order_amount, order_date, request_date,
                   days_since_order, within_return_window,
                   product_name, product_category, is_high_value,
                   item_condition
            FROM {self.catalog}.refund_gold.refund_case_decisioning
            WHERE refund_id = '{refund_id}'
        """)
        if not rows:
            return {"valid": False, "reason": "Refund request not found"}

        case = rows[0]
        issues = []
        if case.get("order_id") is None:
            issues.append("No matching order found")
        if case.get("within_return_window") == "false":
            issues.append(f"Outside return window ({case.get('days_since_order')} days)")
        if case.get("item_condition") == "missing":
            issues.append("Item reported as missing - special handling required")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "case": case,
        }

    @mlflow.trace(name="step2_policy_compliance")
    def step2_policy(self, case):
        """Step 2: Policy Compliance - Auto-approve threshold? Category rules?"""
        amount = float(case.get("amount", 0))
        is_high_value = case.get("is_high_value") == "true"
        within_window = case.get("within_return_window") == "true"

        result = {
            "auto_approvable": False,
            "policy_flags": [],
        }

        if amount <= 25 and within_window and not is_high_value:
            result["auto_approvable"] = True
            result["policy_flags"].append("Below auto-approve threshold ($25)")

        if is_high_value:
            result["policy_flags"].append("High-value item requires manual review")

        if not within_window:
            result["policy_flags"].append("Outside return window - policy exception needed")

        if case.get("reason_code") == "UNAUTHORIZED_PURCHASE":
            result["policy_flags"].append("Unauthorized purchase - fraud team review required")

        return result

    @mlflow.trace(name="step3_abuse_risk_scoring")
    def step3_risk(self, w, case):
        """Step 3: Abuse Risk Scoring - Composite weighted score."""
        customer_id = case.get("customer_id")

        # Get customer 360 data
        c360_rows = self._query(w, f"""
            SELECT risk_tier, refund_rate, refunds_90d, total_refund_amount_90d,
                   coordinated_timing_flag, household_refund_count_30d,
                   lifetime_orders, lifetime_refunds
            FROM {self.catalog}.refund_gold.refund_customer_360
            WHERE customer_id = {customer_id}
        """)

        risk_data = c360_rows[0] if c360_rows else {}
        risk_score = float(case.get("risk_score", 0))

        signals = []
        if risk_data.get("risk_tier") in ("HIGH", "CRITICAL"):
            signals.append(f"Customer risk tier: {risk_data.get('risk_tier')}")
        if float(risk_data.get("refund_rate", 0)) > 0.25:
            signals.append(f"High refund rate: {risk_data.get('refund_rate')}")
        if int(risk_data.get("refunds_90d", 0)) > 5:
            signals.append(f"Frequent refunder: {risk_data.get('refunds_90d')} in 90 days")
        if risk_data.get("coordinated_timing_flag") == "true":
            signals.append("Household coordination detected")
        if (case.get("reason_code") == "ITEM_NOT_RECEIVED" and
                case.get("delivery_confirmed") == "true"):
            signals.append("Claims non-delivery but delivery confirmed with photo")

        return {
            "risk_score": risk_score,
            "risk_tier": risk_data.get("risk_tier", "UNKNOWN"),
            "signals": signals,
            "customer_360": risk_data,
        }

    @mlflow.trace(name="step4_llm_recommendation")
    def step4_recommend(self, validation, policy, risk, case):
        """Step 4: LLM Recommendation - Structured approve/reject/escalate."""
        from openai import OpenAI

        w = self._get_sql_client()
        host = w.config.host
        token = w.config.token or w.config.authenticate().get("Authorization", "").replace("Bearer ", "")

        client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")

        context = f"""
You are a refund abuse analyst. Based on the following data, provide a structured recommendation.

REFUND REQUEST:
- Refund ID: {case.get('refund_id')}
- Amount: ${case.get('amount')}
- Reason: {case.get('reason_code')}
- Product: {case.get('product_name')} ({case.get('product_category')})
- Channel: {case.get('channel', 'N/A')}
- Days since order: {case.get('days_since_order')}
- Item condition: {case.get('item_condition')}

VALIDATION:
- Valid: {validation.get('valid')}
- Issues: {', '.join(validation.get('issues', [])) or 'None'}

POLICY:
- Auto-approvable: {policy.get('auto_approvable')}
- Flags: {', '.join(policy.get('policy_flags', [])) or 'None'}

RISK ASSESSMENT:
- Risk Score: {risk.get('risk_score')} / 1.0
- Risk Tier: {risk.get('risk_tier')}
- Abuse Signals: {', '.join(risk.get('signals', [])) or 'None'}

Respond with EXACTLY this JSON format:
{{
    "action": "APPROVE" or "REJECT" or "ESCALATE",
    "confidence": 0.0-1.0,
    "explanation": "2-3 sentence explanation for the CSR",
    "key_factors": ["factor1", "factor2", "factor3"]
}}
"""

        response = client.chat.completions.create(
            model=self.endpoint,
            messages=[{"role": "user", "content": context}],
            max_tokens=512,
            temperature=0.2,
        )

        raw = response.choices[0].message.content
        try:
            # Extract JSON from response
            start = raw.index("{")
            end = raw.rindex("}") + 1
            return json.loads(raw[start:end])
        except (ValueError, json.JSONDecodeError):
            return {
                "action": case.get("recommended_action", "ESCALATE"),
                "confidence": float(risk.get("risk_score", 0.5)),
                "explanation": raw[:500],
                "key_factors": risk.get("signals", []),
            }

    @mlflow.trace(name="refund_agent_decide")
    def decide(self, refund_id):
        """Run the full 4-step decisioning pipeline."""
        w = self._get_sql_client()

        # Step 1: Validate
        validation = self.step1_validate(w, refund_id)
        if not validation.get("valid") and not validation.get("case"):
            return {"error": "Refund not found", "refund_id": refund_id}

        case = validation.get("case", {})

        # Step 2: Policy
        policy = self.step2_policy(case)

        # Step 3: Risk
        risk = self.step3_risk(w, case)

        # Step 4: LLM Recommendation
        recommendation = self.step4_recommend(validation, policy, risk, case)

        return {
            "refund_id": refund_id,
            "validation": validation,
            "policy": policy,
            "risk": risk,
            "recommendation": recommendation,
        }

    def predict(self, context, model_input):
        """MLflow PythonModel predict interface."""
        if isinstance(model_input, dict):
            refund_id = model_input.get("refund_id")
        else:
            refund_id = model_input.iloc[0].get("refund_id")
        return self.decide(refund_id)

# COMMAND ----------

# Test the agent
agent = RefundDecisioningAgent()

with mlflow.start_run(run_name="agent-test"):
    result = agent.decide("REF-0000001")
    print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# Log the model
with mlflow.start_run(run_name="refund-agent-model"):
    mlflow.pyfunc.log_model(
        artifact_path="refund-agent",
        python_model=RefundDecisioningAgent(),
        registered_model_name="refund_decisioning_agent",
    )
    print("Model logged successfully")
