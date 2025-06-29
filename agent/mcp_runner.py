from services.cluster_engine import run_clustering, generate_cluster_summaries
from agent.tools.report import generate_summary
from agent.task_schemas import SegmentationInput

def run_mcp_task(task_type: str, payload: dict):
    if task_type == "segmentation":
        # Validate input
        input_data = SegmentationInput(**payload)
        # Run clustering
        result = run_clustering(input_data.sales_data, input_data.n_clusters)
        # Generate summaries if requested
        if input_data.include_summaries:
            summary_result = generate_cluster_summaries(result["clusters"], input_data.n_clusters)
            result["stats"] = summary_result["stats"]
            result["summaries"] = [generate_summary(stat) for stat in summary_result["summaries"]]
        else:
            result["stats"] = generate_cluster_summaries(result["clusters"], input_data.n_clusters)["stats"]
            result["summaries"] = []
        return result
    else:
        raise ValueError(f"Unknown task type: {task_type}")