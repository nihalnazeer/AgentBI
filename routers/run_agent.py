from fastapi import APIRouter, Query, HTTPException, Response, UploadFile, File
from agent.mcp_runner import run_mcp_task  # Updated import
from services.utils import load_sales_data
from services.pricing_engine import optimize_prices
from services.cashflow_engine import analyze_cash_flow
from engine.trigger_engine import evaluate_triggers
import json
import os

router = APIRouter()

@router.post("/api/segment-customers/")
async def segment_customers(
    n_clusters: int = Query(3, ge=2, le=10),
    max_graph_customers: int = Query(50, ge=10, le=100),
    include_reports: bool = Query(True),
    include_summaries: bool = Query(False),  # Added parameter
    format: str = Query("all", enum=["all", "graph", "stats", "reports"])
):
    try:
        payload = {
            "sales_data": load_sales_data().to_dict(orient='records'),
            "n_clusters": n_clusters,
            "max_graph_customers": max_graph_customers,
            "include_reports": include_reports,
            "include_summaries": include_summaries  # Added to payload
        }
        result = run_mcp_task("segmentation", payload)
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["details"])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Segmentation failed: {str(e)}")

@router.post("/api/upload-emails/")
async def upload_emails(file: UploadFile = File(...)):
    try:
        output_dir = "/Users/mohammednihal/Desktop/Business Intelligence/AgentBI/Backend/mock_data"
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/emails.csv", "wb") as f:
            content = await file.read()
            f.write(content)
        return {"message": "Emails uploaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@router.post("/api/optimize-prices/")
async def optimize_prices_endpoint():
    try:
        recommendations = optimize_prices()
        return recommendations
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Price optimization failed: {str(e)}")

@router.post("/api/analyze-cash-flow/")
async def analyze_cash_flow_endpoint():
    try:
        analysis = analyze_cash_flow()
        return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cash flow analysis failed: {str(e)}")

@router.post("/api/evaluate-triggers/")
async def evaluate_triggers_endpoint():
    try:
        result = evaluate_triggers()
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["details"])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trigger evaluation failed: {str(e)}")