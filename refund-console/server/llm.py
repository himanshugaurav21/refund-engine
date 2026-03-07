"""Foundation Model client using OpenAI-compatible API."""

import os
from openai import OpenAI
from server.config import get_workspace_host, get_oauth_token

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        host = get_workspace_host()
        token = get_oauth_token()
        _client = OpenAI(
            api_key=token,
            base_url=f"{host}/serving-endpoints",
        )
    return _client


def chat_completion(
    messages: list[dict],
    model: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> str:
    client = _get_client()
    endpoint = model or os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4")

    response = client.chat.completions.create(
        model=endpoint,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.choices[0].message.content
