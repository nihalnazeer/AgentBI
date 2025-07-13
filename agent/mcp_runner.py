from services.cluster_engine import run_clustering
from pydantic import BaseModel
import logging
import json
import os
import requests
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
api_key = os.getenv("OPENROUTER_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SegmentationInput(BaseModel):
    sales_data: list
    n_clusters: int
    max_graph_customers: int
    include_reports: bool
    include_summaries: bool = False  # Added to match context

SUMMARY_PROMPT = """
Summarize customer segment in 50 words:
Cluster ID: {cluster_id}
Average Spend: ${avg_sales:.2f}
Average Orders: {avg_frequency:.2f}
Top Category: {top_category}
Provide actionable insights for targeting this segment.
"""

def run_mcp_task(task_type: str, input_data: dict):
    input_data = SegmentationInput(**input_data)
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "engine", f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
    os.makedirs(output_dir, exist_ok=True)

    if task_type == "segmentation":
        result = run_clustering(
            input_data.sales_data,
            input_data.n_clusters,
            input_data.max_graph_customers,
            input_data.include_reports
        )
        if input_data.include_summaries:
            summaries = []
            for stat in result.get("stats", []):
                prompt = SUMMARY_PROMPT.format(
                    cluster_id=stat.get("cluster_label", "Unknown"),
                    avg_sales=stat.get("avg_sales", 0),
                    avg_frequency=stat.get("avg_frequency", 0),
                    top_category=stat.get("top_category", "Unknown")
                )
                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "HTTP-Referer": "http://your-site-url.com",
                        "X-Title": "Your Site Name"
                    },
                    json={
                        "model": "mistralai/mistral-small-3.2-24b-instruct:free",
                        "messages": [{"role": "user", "content": prompt}]
                    }
                )
                summary = response.json().get("choices", [{}])[0].get("message", {}).get("content", "Summary generation failed")
                summaries.append({"cluster_id": stat.get("cluster_label", "Unknown"), "summary": summary})
            result["summaries"] = summaries
            with open(f"{output_dir}/segmentation_summaries.json", "w") as f:
                json.dump({"timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"), "summaries": summaries}, f, indent=2)
            logger.info("Segmentation summaries saved to %s", output_dir)
        return result
    elif task_type == "summarize":
        raise NotImplementedError("Summarization task not implemented yet")
    else:
        raise ValueError(f"Unknown task type: {task_type}")