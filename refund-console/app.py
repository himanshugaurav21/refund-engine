"""Refund Console - FastAPI application entry point."""

import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

from server.routes.dashboard import router as dashboard_router
from server.routes.cases import router as cases_router
from server.routes.actions import router as actions_router
from server.routes.agent import router as agent_router
from server.routes.feedback import router as feedback_router
from server.routes.genie import router as genie_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "")
    if not experiment_name:
        print("MLFLOW_EXPERIMENT_NAME not set — tracing disabled")
    elif HAS_MLFLOW:
        try:
            from server.config import get_workspace_host, get_oauth_token, IS_DATABRICKS_APP
            host = get_workspace_host()
            # Set Databricks auth env vars for MLflow
            os.environ["DATABRICKS_HOST"] = host
            if IS_DATABRICKS_APP:
                # On Databricks Apps, use the SDK-managed token
                os.environ["DATABRICKS_TOKEN"] = get_oauth_token()
            mlflow.set_tracking_uri("databricks")
            mlflow.set_experiment(experiment_name)
            mlflow.tracing.enable()
            try:
                mlflow.openai.autolog()
            except Exception:
                pass
            print(f"MLflow tracing enabled — experiment: {experiment_name}")
            print(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
            print(f"DATABRICKS_HOST: {host}")
        except Exception as e:
            import traceback
            print(f"MLflow setup error (non-fatal): {e}")
            traceback.print_exc()
    print("Refund Console starting up...")
    yield
    print("Refund Console shutting down.")


app = FastAPI(title="Refund Console", version="1.0.0", lifespan=lifespan)

# Register API routes
app.include_router(dashboard_router)
app.include_router(cases_router)
app.include_router(actions_router)
app.include_router(agent_router)
app.include_router(feedback_router)
app.include_router(genie_router)


@app.get("/api/health")
def health():
    return {"status": "healthy", "app": "refund-console"}


# Serve React frontend build
FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"

if FRONTEND_DIST.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = FRONTEND_DIST / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(FRONTEND_DIST / "index.html")
