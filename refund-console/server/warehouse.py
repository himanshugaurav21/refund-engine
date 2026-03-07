"""SQL Warehouse query layer with TTL caching."""

import json
from typing import Any
from cachetools import TTLCache
from databricks.sdk.service.sql import StatementState

from server.config import get_workspace_client, get_warehouse_id, get_catalog

_cache: TTLCache = TTLCache(maxsize=256, ttl=30)

CATALOG = None


def _catalog():
    global CATALOG
    if CATALOG is None:
        CATALOG = get_catalog()
    return CATALOG


def execute_query(sql: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """Execute a SQL statement and return rows as dicts. Cached for 30s."""
    cache_key = (sql, json.dumps(params, sort_keys=True) if params else "")
    if cache_key in _cache:
        return _cache[cache_key]

    w = get_workspace_client()
    warehouse_id = get_warehouse_id()

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
    except Exception as e:
        print(f"SQL execution error: {e}")
        raise RuntimeError(f"SQL execution error: {e}")

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        print(f"SQL query failed: {error_msg}\nSQL: {sql}")
        raise RuntimeError(f"SQL query failed: {error_msg}")

    if not response.result or not response.manifest:
        _cache[cache_key] = []
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows = []
    if response.result.data_array:
        for row_data in response.result.data_array:
            rows.append(dict(zip(columns, row_data)))

    _cache[cache_key] = rows
    return rows


def get_dashboard_metrics() -> dict[str, Any]:
    """KPI metrics for the dashboard."""
    cat = _catalog()
    rows = execute_query(f"""
        SELECT
            COUNT(*) as total_cases,
            SUM(CASE WHEN workflow_state = 'pending_review' THEN 1 ELSE 0 END) as pending_count,
            SUM(CASE WHEN workflow_state = 'auto_approved' THEN 1 ELSE 0 END) as auto_approved_count,
            SUM(CASE WHEN csr_action = 'approved' THEN 1 ELSE 0 END) as approved_count,
            SUM(CASE WHEN csr_action = 'rejected' THEN 1 ELSE 0 END) as rejected_count,
            SUM(CASE WHEN csr_action = 'escalated' THEN 1 ELSE 0 END) as escalated_count,
            ROUND(AVG(risk_score), 3) as avg_risk_score,
            SUM(CASE WHEN recommended_action = 'REJECT' THEN amount ELSE 0 END) as potential_leakage,
            SUM(amount) as total_refund_amount
        FROM {cat}.refund_serving.refund_live_cases
    """)
    if not rows:
        return {}
    return rows[0]


def get_risk_distribution() -> list[dict[str, Any]]:
    """Risk tier distribution for charts."""
    cat = _catalog()
    return execute_query(f"""
        SELECT abuse_risk_tier, COUNT(*) as count,
               ROUND(AVG(risk_score), 3) as avg_score,
               SUM(amount) as total_amount
        FROM {cat}.refund_serving.refund_live_cases
        GROUP BY abuse_risk_tier
        ORDER BY CASE abuse_risk_tier
            WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3 ELSE 4 END
    """)


def get_cases(
    status: str | None = None,
    risk_tier: str | None = None,
    channel: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Paginated case list with filters."""
    cat = _catalog()
    conditions = []
    if status:
        conditions.append(f"workflow_state = '{status}'")
    if risk_tier:
        conditions.append(f"abuse_risk_tier = '{risk_tier}'")
    if channel:
        conditions.append(f"channel = '{channel}'")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    return execute_query(f"""
        SELECT refund_id, order_id, customer_id, first_name, last_name,
               tier, reason_code, amount, channel, request_date,
               risk_score, recommended_action, workflow_state,
               abuse_risk_tier, product_name, product_category
        FROM {cat}.refund_serving.refund_live_cases
        {where}
        ORDER BY risk_score DESC
        LIMIT {limit} OFFSET {offset}
    """)


def get_case_detail(refund_id: str) -> dict[str, Any] | None:
    """Full case detail including customer 360 and AI recommendation."""
    cat = _catalog()
    rows = execute_query(f"""
        SELECT *
        FROM {cat}.refund_serving.refund_live_cases
        WHERE refund_id = '{refund_id}'
    """)
    if not rows:
        return None

    case = rows[0]

    # Get customer 360 data
    customer_id = case.get("customer_id")
    c360_rows = execute_query(f"""
        SELECT *
        FROM {cat}.refund_gold.refund_customer_360
        WHERE customer_id = {customer_id}
    """)

    case["customer_360"] = c360_rows[0] if c360_rows else {}
    return case


def update_case_action(refund_id: str, action: str, reason: str) -> bool:
    """Update case with CSR action."""
    cat = _catalog()
    state_map = {
        "approved": "approved",
        "rejected": "rejected",
        "escalated": "escalated",
    }
    new_state = state_map.get(action, "pending_review")

    try:
        execute_query(f"""
            UPDATE {cat}.refund_serving.refund_live_cases
            SET csr_action = '{action}',
                csr_reason = '{reason}',
                csr_acted_at = CURRENT_TIMESTAMP(),
                workflow_state = '{new_state}'
            WHERE refund_id = '{refund_id}'
        """)
        # Invalidate cache
        _cache.clear()
        return True
    except Exception as e:
        print(f"Update failed: {e}")
        return False
