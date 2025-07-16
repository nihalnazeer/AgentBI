import os
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_output_files(timestamp: str):
    try:
        output_dir = os.path.join(os.path.dirname(__file__), f"output_{timestamp}")
        expected_files = {
            "Cash Flow Analysis": [
                f"{output_dir}/cash_flow_analysis_{g}.json" for g in ['yearly', 'monthly', 'weekly', 'seasonal', 'overall', 'day']
            ],
            "Segmentation": [
                f"{output_dir}/segmentation_results.json"
            ],
            "Cluster LLM Report": [
                f"{output_dir}/cluster_report.json"
            ],
            "Price Optimization": [
                f"{output_dir}/price_optimization_stats.json",
                f"{output_dir}/price_optimization_report.json"
            ],
            "Trigger Engine": [
                f"{output_dir}/queued_emails.json"
            ],
            "Notifications": [
                f"{output_dir}/notifications.json"
            ]
        }

        validation_results = {}
        for task, files in expected_files.items():
            validation_results[task] = []
            for file_path in files:
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            json.load(f)
                        validation_results[task].append({"file": file_path, "status": "success"})
                    except Exception as e:
                        validation_results[task].append({"file": file_path, "status": "failed", "error": str(e)})
                else:
                    validation_results[task].append({"file": file_path, "status": "missing"})

        output_dir = os.path.join(os.path.dirname(__file__), f"output_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/validation_results.json", "w") as f:
            json.dump(validation_results, f, indent=2)
        logger.info("Validation results saved to %s", output_dir)

        return validation_results
    except Exception as e:
        logger.error("Validation failed: %s", str(e))
        return {"error": str(e)}