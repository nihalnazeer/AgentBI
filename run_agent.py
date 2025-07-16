
import sys
import os
import json
import logging
from fastapi import APIRouter, Request, HTTPException, UploadFile, File
from pymongo import MongoClient
from bson import ObjectId
from agent.mcp_runner import run_mcp_task
from services.utils import load_sales_data
from services.cashflow_engine import analyze_cash_flow
from services.cluster_engine import run_clustering
from services.price_optimization_engine import optimize_prices
from services.threshold_engine import check_thresholds
from services.notification_engine import generate_notifications
from services.validate import validate_output_files
from services.email_templates import send_emails
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017", wTimeoutMS=1000)
db = client["AgentBI-Demo"]

# Task-to-collection mapping
TASK_COLLECTIONS = {
    1: "task_results",
    2: "cash_flow_results",
    3: "segmentation_results",
    5: "price_optimization_results",
    7: "trigger_results",
    8: "validation_results",
    9: "notifications",
    10: "email_templates"
}

def load_latest_schema(pipeline_id: str = "AgentBI-Demo", schema_dir: str = "schemas") -> str:
    """Load the latest schema version for the given pipeline_id."""
    try:
        schema_files = [f for f in os.listdir(schema_dir) if f.startswith(pipeline_id) and f.endswith(".json")]
        if not schema_files:
            logger.warning(f"No schema files found for pipeline_id: {pipeline_id}, defaulting to v0.6.2")
            return "v0.6.2"
        
        latest_version = "v0.0.0"
        for schema_file in schema_files:
            with open(os.path.join(schema_dir, schema_file), "r") as f:
                schema = json.load(f)
                version = schema.get("schema_version", "v0.0.0")
                if version > latest_version:
                    latest_version = version
        
        logger.info(f"Latest schema version for {pipeline_id}: {latest_version}")
        return latest_version
    except Exception as e:
        logger.error(f"Failed to load schema for {pipeline_id}: {str(e)}, defaulting to v0.6.2")
        return "v0.6.2"

def convert_to_json_serializable(obj):
    """Convert MongoDB ObjectId and other non-serializable objects to JSON-serializable formats."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    return obj

@router.post("/api/run-task/{task_id}")
async def run_task(task_id: int, request: Request):
    try:
        schema_version = load_latest_schema()
        output_collection = TASK_COLLECTIONS.get(task_id, "task_results")
        params = await request.json() if request.headers.get("content-type") == "application/json" else {}
        logger.info(f"Executing task {task_id} with params: {params}, schema_version: {schema_version}")
        
        # Clear collection if rerun: true
        if task_id in [1, 2, 3, 5, 7, 8, 9, 10]:
            db[output_collection].delete_many({"task_id": task_id, "pipeline_id": "AgentBI-Demo"})
            logger.info(f"Cleared collection {output_collection} for task {task_id}")
        
        result = {}
        if task_id == 1:
            result = run_mcp_task("LLM", params, db, pipeline_id="AgentBI-Demo", schema_version=schema_version)
        elif task_id == 2:
            granularity = params.get("granularity", "all")
            logger.info(f"Calling analyze_cash_flow with granularity={granularity}")
            result_dict = analyze_cash_flow(granularity=granularity, db=db)
            
            if not isinstance(result_dict, dict):
                logger.error(f"Task 2: analyze_cash_flow returned unexpected result format: {result_dict}")
                result = {
                    "status": "error",
                    "message": f"Unexpected result format from analyze_cash_flow: {result_dict}",
                    "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
                    "pipeline_id": "AgentBI-Demo",
                    "schema_version": schema_version,
                    "task_id": task_id,
                    "summary": True
                }
                try:
                    db[output_collection].insert_one(result)
                    logger.info(f"Task {task_id} error result saved to {output_collection}")
                except Exception as e:
                    logger.error(f"Failed to insert error result: {str(e)}")
                    raise
            else:
                saved_results = []
                granularities = ["weekly", "monthly", "quarterly", "yearly"] if granularity == "all" else [granularity]
                for gran in granularities:
                    gran_key = "week" if gran == "weekly" else gran
                    if gran_key not in result_dict:
                        logger.warning(f"Granularity {gran} not found in analyze_cash_flow output")
                        continue
                    
                    gran_result = {
                        "granularity": gran,
                        gran_key: result_dict[gran_key],
                        "status": result_dict.get("status", "success"),
                        "totalSales": result_dict.get("totalSales", 0.0),
                        "totalProfit": result_dict.get("totalProfit", 0.0),
                        "profitMargin": result_dict.get("profitMargin", 0.0),
                        "trend_summary": result_dict.get("trend_summary", {}),
                        "message": f"Cash flow analyzed for {gran} granularity",
                        "pipeline_id": "AgentBI-Demo",
                        "schema_version": schema_version,
                        "task_id": task_id,
                        "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M")
                    }
                    
                    try:
                        insert_result = db[output_collection].insert_one(gran_result)
                        logger.info(f"Task {task_id} result for granularity {gran} saved to {output_collection} with ID {insert_result.inserted_id}")
                    except Exception as e:
                        logger.error(f"Failed to insert granularity {gran} result: {str(e)}")
                        raise
                    
                    saved_results.append({
                        "granularity": gran,
                        "status": gran_result["status"],
                        "data_points": len(gran_result.get(gran_key, [])),
                        "total_sales": gran_result.get("totalSales", 0.0),
                        "total_profit": gran_result.get("totalProfit", 0.0),
                        "profit_margin": gran_result.get("profitMargin", 0.0),
                        "_id": str(insert_result.inserted_id)
                    })
                
                # Create summary document
                result = {
                    "status": "success",
                    "granularities_processed": [res["granularity"] for res in saved_results],
                    "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
                    "pipeline_id": "AgentBI-Demo",
                    "schema_version": schema_version,
                    "task_id": task_id,
                    "detailed_results": saved_results,
                    "total_records_saved": len(saved_results),
                    "summary": True
                }
                try:
                    insert_result = db[output_collection].insert_one(result)
                    logger.info(f"Task {task_id} summary saved to {output_collection} with ID {insert_result.inserted_id}")
                except Exception as e:
                    logger.error(f"Failed to insert summary result: {str(e)}")
                    raise
        elif task_id == 3:
            payload = {
                "sales_data": params.get("sales_data", load_sales_data().to_dict(orient='records')),
                "n_clusters": params.get("n_clusters", 3),
                "max_graph_customers": params.get("max_graph_customers", 50),
                "include_reports": params.get("include_reports", True),
                "historical_stats": params.get("historical_stats", [])
            }
            logger.info(f"Calling run_clustering with payload: {payload}")
            result = run_clustering(**payload, db=db)
            result["pipeline_id"] = "AgentBI-Demo"
            result["schema_version"] = schema_version
            result["task_id"] = task_id
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H:%M")
            try:
                db[output_collection].insert_one(result)
                logger.info(f"Task {task_id} result saved to {output_collection}")
            except Exception as e:
                logger.error(f"Failed to insert result: {str(e)}")
                raise
        elif task_id == 5:
            latest_segmentation = db.segmentation_results.find_one(
                {"task_id": 3, "schema_version": schema_version},
                sort=[("timestamp", -1)]
            )
            logger.info(f"Task 5: Latest segmentation document found: {latest_segmentation is not None}")
            segmentation_stats = latest_segmentation.get("output", {}).get("result", {}).get("stats") if latest_segmentation else None
            result = optimize_prices(segmentation_stats=segmentation_stats, db=db)
            result["pipeline_id"] = "AgentBI-Demo"
            result["schema_version"] = schema_version
            result["task_id"] = task_id
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H:%M")
            try:
                db[output_collection].insert_one(result)
                logger.info(f"Task {task_id} result saved to {output_collection}")
            except Exception as e:
                logger.error(f"Failed to insert result: {str(e)}")
                raise
        elif task_id == 7:
            trigger_inputs = db.trigger_inputs.find_one({"schema_version": schema_version}, sort=[("timestamp", -1)]) or {}
            segmentation_stats = trigger_inputs.get("segmentation_stats", db.segmentation_results.find_one({"task_id": 3, "schema_version": schema_version}, sort=[("timestamp", -1)]) or {}).get("output", {}).get("result", {}).get("stats", [])
            # Try any available granularity
            cash_flow_data = trigger_inputs.get("cash_flow_data", [])
            if not cash_flow_data:
                for gran in ["monthly", "weekly", "quarterly", "yearly"]:
                    gran_key = "week" if gran == "weekly" else gran
                    cash_flow_doc = db.cash_flow_results.find_one({"task_id": 2, "granularity": gran, "schema_version": schema_version}, sort=[("timestamp", -1)])
                    if cash_flow_doc and gran_key in cash_flow_doc:
                        cash_flow_data = cash_flow_doc[gran_key]
                        logger.info(f"Task 7: Using cash flow data from granularity: {gran}")
                        break
                else:
                    logger.warning("No cash flow data found for any granularity, using empty list")
                    cash_flow_data = []
            logger.info(f"Task 7: segmentation_stats length: {len(segmentation_stats) if segmentation_stats else 0}, cash_flow_data: {cash_flow_data}")
            db.trigger_inputs.insert_one({
                "pipeline_id": "AgentBI-Demo",
                "schema_version": schema_version,
                "task_id": 7,
                "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
                "segmentation_stats": segmentation_stats,
                "cash_flow_data": cash_flow_data
            })
            result = check_thresholds(segmentation_stats=segmentation_stats, cash_flow_data=cash_flow_data, db=db)
            result["pipeline_id"] = "AgentBI-Demo"
            result["schema_version"] = schema_version
            result["task_id"] = task_id
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H:%M")
            try:
                db[output_collection].insert_one(result)
                logger.info(f"Task {task_id} result saved to {output_collection}")
            except Exception as e:
                logger.error(f"Failed to insert result: {str(e)}")
                raise
        elif task_id == 8:
            timestamp = params.get("timestamp", datetime.now().strftime("%Y-%m-%d_%H:%M"))
            logger.info(f"Calling validate_output_files with timestamp: {timestamp}")
            result = validate_output_files(timestamp, db=db)
            result["pipeline_id"] = "AgentBI-Demo"
            result["schema_version"] = schema_version
            result["task_id"] = task_id
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H:%M")
            try:
                db[output_collection].insert_one(result)
                logger.info(f"Task {task_id} result saved to {output_collection}")
            except Exception as e:
                logger.error(f"Failed to insert result: {str(e)}")
                raise
        elif task_id == 9:
            trigger_results = params.get("trigger_results", db.trigger_results.find_one({"schema_version": schema_version}, sort=[("timestamp", -1)]) or {})
            logger.info(f"Task 9: Using trigger_results timestamp: {trigger_results.get('timestamp') if trigger_results else None}")
            result = generate_notifications(trigger_results, db=db)
            result["pipeline_id"] = "AgentBI-Demo"
            result["schema_version"] = schema_version
            result["task_id"] = task_id
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H:%M")
            try:
                db[output_collection].insert_one(result)
                logger.info(f"Task {task_id} result saved to {output_collection}")
            except Exception as e:
                logger.error(f"Failed to insert result: {str(e)}")
                raise
        elif task_id == 10:
            email_inputs = params.get("email_inputs", db.email_inputs.find_one({"schema_version": schema_version}, sort=[("timestamp", -1)]) or {})
            clusters = email_inputs.get("clusters", db.segmentation_results.find_one({"task_id": 3, "schema_version": schema_version}, sort=[("timestamp", -1)]) or {}).get("output", {}).get("result", {}).get("stats", [])
            reports = email_inputs.get("reports", db.segmentation_results.find_one({"task_id": 3, "schema_version": schema_version}, sort=[("timestamp", -1)]) or {}).get("output", {}).get("result", {}).get("reports", [])
            price_optimization_data = email_inputs.get("price_optimization_data", db.price_optimization_results.find_one({"task_id": 5, "schema_version": schema_version}, sort=[("timestamp", -1)]) or {}).get("output", {}).get("result", {})
            segmentation_stats = email_inputs.get("segmentation_stats", db.segmentation_results.find_one({"task_id": 3, "schema_version": schema_version}, sort=[("timestamp", -1)]) or {}).get("output", {}).get("result", {}).get("stats", [])
            db.email_inputs.insert_one({
                "pipeline_id": "AgentBI-Demo",
                "schema_version": schema_version,
                "task_id": 10,
                "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
                "clusters": clusters,
                "reports": reports,
                "price_optimization_data": price_optimization_data,
                "segmentation_stats": segmentation_stats
            })
            logger.info(f"Task 10: Using clusters length: {len(clusters)}, reports length: {len(reports)}")
            result = send_emails(clusters, reports, price_optimization_data, segmentation_stats, db=db)
            result["pipeline_id"] = "AgentBI-Demo"
            result["schema_version"] = schema_version
            result["task_id"] = task_id
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H:%M")
            try:
                db[output_collection].insert_one(result)
                logger.info(f"Task {task_id} result saved to {output_collection}")
            except Exception as e:
                logger.error(f"Failed to insert result: {str(e)}")
                raise
        else:
            raise HTTPException(status_code=400, detail="Invalid task ID")
        
        return {"status": "success", "result": convert_to_json_serializable(result)}
    except Exception as e:
        logger.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Task {task_id} failed: {str(e)}")

@router.get("/api/task-results/{task_id}")
async def get_task_results(task_id: int, timestamp: str = None):
    try:
        schema_version = load_latest_schema()
        output_collection = TASK_COLLECTIONS.get(task_id, "task_results")
        query = {"task_id": task_id, "pipeline_id": "AgentBI-Demo", "schema_version": schema_version}
        if timestamp:
            query["timestamp"] = timestamp
        results = list(db[output_collection].find(query).sort("timestamp", -1))
        if not results:
            raise HTTPException(status_code=404, detail="No results found")
        return convert_to_json_serializable(results)
    except Exception as e:
        logger.error(f"Failed to fetch task results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch task results: {str(e)}")

@router.get("/api/notifications")
async def get_notifications(timestamp: str = None, read: bool = None):
    try:
        schema_version = load_latest_schema()
        query = {"pipeline_id": "AgentBI-Demo", "schema_version": schema_version}
        if timestamp:
            query["timestamp"] = timestamp
        if read is not None:
            query["read"] = read
        notifications = list(db.notifications.find(query).sort("timestamp", -1))
        return convert_to_json_serializable(notifications)
    except Exception as e:
        logger.error(f"Failed to fetch notifications: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch notifications: {str(e)}")

@router.put("/api/notifications/{notification_id}")
async def mark_notification_read(notification_id: str):
    try:
        schema_version = load_latest_schema()
        result = db.notifications.update_one(
            {"id": notification_id, "pipeline_id": "AgentBI-Demo", "schema_version": schema_version},
            {"$set": {"read": True}}
        )
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Notification not found")
        return {"status": "updated"}
    except Exception as e:
        logger.error(f"Failed to update notification: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update notification: {str(e)}")

@router.get("/api/latest-pipeline")
async def get_latest_pipeline():
    try:
        schema_version = load_latest_schema()
        result = db.task_results.find_one({"pipeline_id": "AgentBI-Demo"}, sort=[("timestamp", -1)])
        return convert_to_json_serializable({"timestamp": result["timestamp"], "schema_version": schema_version} if result else {"schema_version": schema_version})
    except Exception as e:
        logger.error(f"Failed to fetch latest pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch latest pipeline: {str(e)}")

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
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")