from fastapi import APIRouter
from agent.mcp_runner import run_mcp_task
from services.utils import load_sales_data
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")

@router.post("/segment-customers/")
async def segment_customers(n_clusters: int = 4, include_summaries: bool = True):
    try:
        logger.info(f"Received request: n_clusters={n_clusters}, include_summaries={include_summaries}")
        payload = {
            "sales_data": load_sales_data().to_dict(orient='records'),
            "n_clusters": n_clusters,
            "include_summaries": include_summaries
        }
        result = run_mcp_task("segmentation", payload)
        logger.info("Clustering completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error in segment_customers: {str(e)}")
        raise