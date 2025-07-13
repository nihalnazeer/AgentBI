import logging
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime  # Added import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_clustering(sales_data, n_clusters=3, max_graph_customers=50, include_reports=True):
    try:
        logger.info("Starting clustering with %d records", len(sales_data))
        df = pd.DataFrame(sales_data)
        
        required_columns = ['Customer ID', 'Order Date', 'Sales', 'Category']
        if not all(col in df.columns for col in required_columns):
            missing = set(required_columns) - set(df.columns)
            logger.error("Missing columns: %s", missing)
            raise ValueError(f"Missing required columns: {missing}")

        df['Category'] = df['Category'].fillna('Unknown').str.strip()
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')

        df_rfm = df.groupby('Customer ID').agg({
            'Order Date': lambda x: (df['Order Date'].max() - x.max()).days,
            'Sales': 'sum'
        }).rename(columns={'Order Date': 'recency', 'Sales': 'monetary'})

        df_category = df.groupby(['Customer ID', 'Category'])['Sales'].sum().unstack(fill_value=0)
        df_category.columns = [f"{cat}_spend" for cat in df_category.columns]
        df = df_rfm.join(df_category, how='left').fillna(0)

        feature_columns = ['recency', 'monetary'] + list(df_category.columns)
        scaler = StandardScaler()
        features = scaler.fit_transform(df[feature_columns])

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df['cluster'] = kmeans.fit_predict(features)
        cluster_means = df.groupby('cluster')['monetary'].mean().sort_values(ascending=False)
        cluster_labels = {cluster_means.index[i]: label for i, label in enumerate(['High', 'Mid', 'Low'])}
        df['cluster_label'] = df['cluster'].map(cluster_labels)

        graph_df = df.sample(n=min(max_graph_customers, len(df)), random_state=42)
        graph_data = [
            {'recency': row['recency'], 'monetary': row['monetary'], 'cluster': row['cluster_label']}
            for _, row in graph_df.iterrows()
        ]

        stats = []
        reports = []
        for cluster_id in range(n_clusters):
            cluster_label = cluster_labels.get(cluster_id, f"Cluster {cluster_id}")
            cluster_data = df[df['cluster'] == cluster_id]
            if not cluster_data.empty:
                customer_count = len(cluster_data)
                total_revenue = cluster_data['monetary'].sum()
                category_spend = {col: cluster_data[col].sum() for col in df_category.columns}
                total_cluster_spend = sum(category_spend.values())
                spend_proportion = {cat: spend / total_cluster_spend for cat, spend in category_spend.items()} if total_cluster_spend > 0 else {}
                top_categories = sorted(spend_proportion, key=spend_proportion.get, reverse=True)[:3]
                top_categories = [cat.replace('_spend', '') for cat in top_categories]
                stats.append({
                    'cluster_label': cluster_label,
                    'customer_count': customer_count,
                    'total_revenue': total_revenue,
                    'top_categories': top_categories
                })
                if include_reports:
                    reports.append(f"{cluster_label}-value cluster: {customer_count} customers, ${total_revenue:.2f} revenue, top categories: {', '.join(top_categories)}.")
            else:
                stats.append({
                    'cluster_label': cluster_label,
                    'customer_count': 0,
                    'total_revenue': 0.0,
                    'top_categories': []
                })

        output_dir = os.path.join(os.path.dirname(__file__), f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/graph_data.json", "w") as f:
            json.dump({"graph_data": graph_data}, f, indent=2)
        with open(f"{output_dir}/stats.json", "w") as f:
            json.dump({"stats": stats}, f, indent=2)
        if include_reports:
            with open(f"{output_dir}/reports.json", "w") as f:
                json.dump({"reports": reports}, f, indent=2)

        logger.info("Clustering completed with %d clusters", n_clusters)
        return {
            'graph_data': graph_data,
            'stats': stats,
            'reports': reports if include_reports else []
        }
    except Exception as e:
        logger.error("Clustering failed: %s", str(e))
        raise ValueError(f"Clustering failed: {str(e)}")