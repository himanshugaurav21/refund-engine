"""Foundation Model client using OpenAI-compatible API."""

import os
import time
from openai import OpenAI
from server.config import get_workspace_host, get_oauth_token


def _new_client() -> OpenAI:
    """Create a fresh OpenAI client with a current token.

    SP OAuth tokens expire (~1h), so we must NOT cache the client.
    """
    host = get_workspace_host()
    token = get_oauth_token()
    return OpenAI(
        api_key=token,
        base_url=f"{host}/serving-endpoints",
        timeout=45.0,  # Cap at 45s to stay under Databricks App gateway timeout
    )


def chat_completion(
    messages: list[dict],
    model: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> str:
    """Call the LLM and return the text response.

    Returns just the text content. LLM metrics (latency, tokens) are
    captured by the caller via _last_llm_metrics.
    """
    global _last_llm_metrics
    _last_llm_metrics = {}

    client = _new_client()
    endpoint = model or os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4")

    t0 = time.time()
    response = client.chat.completions.create(
        model=endpoint,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    latency_ms = int((time.time() - t0) * 1000)

    usage = response.usage
    _last_llm_metrics = {
        "model": endpoint,
        "latency_ms": latency_ms,
        "input_tokens": usage.prompt_tokens if usage else 0,
        "output_tokens": usage.completion_tokens if usage else 0,
        "total_tokens": usage.total_tokens if usage else 0,
    }

    return response.choices[0].message.content


# Module-level store for last LLM call metrics
_last_llm_metrics: dict = {}


def get_last_llm_metrics() -> dict:
    """Return metrics from the most recent chat_completion call."""
    return _last_llm_metrics.copy()
