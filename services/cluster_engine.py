
import logging
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from datetime import datetime
from services.utils import load_sales_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_clustering(
    sales_data: List[Dict[str, Any]] = None,
    n_clusters: int = 3,
    max_graph_customers: int = 50,
    include_reports: bool = True,
    historical_stats: List[Dict[str, Any]] = None,
    db=None
) -> Dict[str, Any]:
    try:
        # Load sales data if not provided
        if sales_data is None:
            logger.info("No sales data provided, loading from utils")
            sales_data = load_sales_data().to_dict(orient='records')
        
        if not sales_data:
            logger.warning("No sales data available")
            return {
                "task_id": 3,
                "pipeline_id": "AgentBI-Demo",
                "schema_version": "v0.6.2",
                "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
                "status": "no_data",
                "graph_data": [],
                "stats": [],
                "reports": [],
                "message": "No sales data available for clustering"
            }
        
        # Convert to DataFrame
        df = pd.DataFrame(sales_data)
        
        # Calculate RFM metrics (simplified for demo)
        if 'CustomerID' not in df.columns or 'OrderDate' not in df.columns or 'Sales' not in df.columns:
            logger.error("Required columns missing in sales data")
            return {
                "task_id": 3,
                "pipeline_id": "AgentBI-Demo",
                "schema_version": "v0.6.2",
                "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
                "status": "error",
                "graph_data": [],
                "stats": [],
                "reports": [],
                "message": "Required columns missing in sales data"
            }
        
        # Calculate recency and monetary value
        current_date = pd.to_datetime(datetime.now())
        rfm = df.groupby('CustomerID').agg({
            'OrderDate': lambda x: (current_date - pd.to_datetime(x).max()).days,
            'Sales': ['sum', 'count']
        }).reset_index()
        rfm.columns = ['CustomerID', 'recency', 'monetary', 'frequency']
        
        # Apply KMeans clustering
        X = rfm[['recency', 'monetary']].values
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        rfm['cluster'] = kmeans.fit_predict(X)
        
        # Map cluster numbers to labels (simplified logic)
        cluster_means = rfm.groupby('cluster')['monetary'].mean().sort_values()
        cluster_labels = {cluster: label for cluster, label in zip(cluster_means.index, ['Low', 'Mid', 'High'][:n_clusters])}
        rfm['cluster'] = rfm['cluster'].map(cluster_labels)
        
        # Prepare graph_data (limited to max_graph_customers)
        graph_data = rfm[['recency', 'monetary', 'cluster']].head(max_graph_customers).to_dict(orient='records')
        
        # Prepare stats
        stats = []
        for cluster_label in ['High', 'Mid', 'Low'][:n_clusters]:
            cluster_data = rfm[rfm['cluster'] == cluster_label]
            if not cluster_data.empty:
                stats.append({
                    "id": cluster_label.lower(),
                    "name": f"{cluster_label} Customers",
                    "count": int(cluster_data.shape[0]),
                    "value": float(cluster_data['monetary'].sum()),
                    "totalRevenue": float(cluster_data['monetary'].sum()),
                    "avgOrderValue": float(cluster_data['monetary'].mean()),
                    "color": {"High": "#10B981", "Mid": "#14B8A6", "Low": "#06B6D4"}[cluster_label],
                    "characteristics": ["Technology focused", "Office Supplies focused", "Furniture focused"],
                    "growth": 0.0
                })
        
        # Prepare reports
        reports = []
        if include_reports:
            for stat in stats:
                reports.append(
                    f"{stat['name']}: {stat['count']} customers, ${stat['totalRevenue']:.2f} revenue, "
                    f"top categories: {', '.join(stat['characteristics'])}."
                )
        
        result = {
            "task_id": 3,
            "pipeline_id": "AgentBI-Demo",
            "schema_version": "v0.6.2",
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "status": "success",
            "graph_data": graph_data,
            "stats": stats,
            "reports": reports,
            "message": f"Clustered {len(rfm)} customers into {n_clusters} segments"
        }
        
        logger.info(f"Clustering completed: {len(stats)} clusters generated")
        return result
    except Exception as e:
        logger.error(f"Clustering failed: {str(e)}", exc_info=True)
        return {
            "task_id": 3,
            "pipeline_id": "AgentBI-Demo",
            "schema_version": "v0.6.2",
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "status": "error",
            "graph_data": [],
            "stats": [],
            "reports": [],
            "message": f"Clustering failed: {str(e)}"
        }