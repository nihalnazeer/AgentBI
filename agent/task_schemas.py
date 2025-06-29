from pydantic import BaseModel
from typing import List, Any

class SegmentationInput(BaseModel):
    sales_data: List[Any]
    n_clusters: int = 4
    include_summaries: bool = True