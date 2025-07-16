
from typing import List, Dict, Any
from datetime import datetime
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_thresholds(segmentation_stats: List[Dict[str, Any]] = None, cash_flow_data: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client["AgentBI-Demo"]
        logger.info(f"Checking thresholds with segmentation_stats: {len(segmentation_stats) if segmentation_stats else 0}, cash_flow_data: {len(cash_flow_data) if cash_flow_data else 0}")
        
        # Handle None inputs
        if segmentation_stats is None:
            logger.warning("segmentation_stats is None, using empty list")
            segmentation_stats = []
        if cash_flow_data is None:
            logger.warning("cash_flow_data is None, using empty list")
            cash_flow_data = []
        
        triggers = []
        # Threshold logic for segmentation_stats
        for segment in segmentation_stats:
            segment_id = segment.get("id", "unknown")
            avg_order_value = segment.get("avgOrderValue", 0.0)
            if avg_order_value > 1000:
                triggers.append({
                    "segment_id": segment_id,
                    "trigger_type": "high_value_segment",
                    "value": avg_order_value,
                    "threshold": 1000
                })
        
        # Threshold logic for cash_flow_data
        for cash_flow in cash_flow_data:
            revenue = cash_flow.get("sales", 0.0)  # Updated to match cashflow_engine.py output
            if revenue < 5000:
                triggers.append({
                    "period": cash_flow.get("period", "unknown"),
                    "trigger_type": "low_cash_flow",
                    "value": revenue,
                    "threshold": 5000
                })
        
        result = {
            "task_id": 7,
            "pipeline_id": "AgentBI-Demo",
            "schema_version": "v0.6.2",
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "triggers": triggers,
            "status": "success",
            "message": f"Processed {len(triggers)} triggers"
        }
        db.trigger_results.insert_one(result)
        logger.info(f"Threshold check completed: {len(triggers)} triggers found")
        return result
    except Exception as e:
        logger.error(f"Threshold check failed: {str(e)}", exc_info=True)
        return {
            "task_id": 7,
            "pipeline_id": "AgentBI-Demo",
            "schema_version": "v0.6.2",
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "triggers": [],
            "status": "error",
            "message": f"Threshold check failed: {str(e)}"
        }