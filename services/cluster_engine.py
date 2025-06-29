from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd

def run_clustering(sales_data, n_clusters=4):
    try:
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(sales_data)
        
        # Validate required columns
        required_columns = ['Customer ID', 'Order Date', 'Sales', 'Category']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing required columns: {set(required_columns) - set(df.columns)}")

        # Convert Order Date to datetime
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        if df['Order Date'].isna().any():
            raise ValueError("Invalid Order Date values in sales data")

        # Compute RFM features
        reference_date = df['Order Date'].max()
        df_rfm = df.groupby('Customer ID').agg({
            'Order Date': lambda x: (reference_date - x.max()).days,
            'Order ID': 'count',
            'Sales': 'sum'
        }).rename(columns={'Order Date': 'recency_days', 'Order ID': 'frequency', 'Sales': 'monetary'})

        # Compute category spend
        df_category = df.groupby(['Customer ID', 'Category'])['Sales'].sum().unstack(fill_value=0)
        df_category.columns = [f"{cat}_spend" for cat in df_category.columns]
        df = df_rfm.join(df_category, how='left').fillna(0)

        # Prepare features for clustering
        feature_columns = ['recency_days', 'frequency', 'monetary'] + list(df_category.columns)
        scaler = StandardScaler()
        features = scaler.fit_transform(df[feature_columns])

        # Apply KMeans
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df['cluster'] = kmeans.fit_predict(features)

        # Format output
        clusters = [
            {
                'customer_id': cid,
                'recency': row['recency_days'],
                'monetary': row['monetary'],
                'cluster': int(row['cluster']),
                'category_spend': {col: row[col] for col in df_category.columns}
            }
            for cid, row in df.iterrows()
        ]

        return {'clusters': clusters}
    except Exception as e:
        raise ValueError(f"Clustering failed: {str(e)}")

def generate_cluster_summaries(clusters, n_clusters=4):
    try:
        df_clusters = pd.DataFrame(clusters)
        category_columns = [k for k in clusters[0]['category_spend'].keys()] if clusters else []
        stats = []
        for i in range(n_clusters):
            cluster_data = df_clusters[df_clusters['cluster'] == i]
            if not cluster_data.empty:
                avg_sales = cluster_data['monetary'].mean()
                avg_frequency = cluster_data['frequency'].mean() if 'frequency' in cluster_data else 0
                category_sums = {col: cluster_data.apply(lambda x: x['category_spend'].get(col, 0), axis=1).mean() for col in category_columns}
                top_category = max(category_sums, key=category_sums.get, default='N/A') if category_sums else 'N/A'
                stats.append({
                    'cluster_id': i,
                    'avg_sales': avg_sales,
                    'avg_frequency': avg_frequency,
                    'top_category': top_category
                })
        summaries = [{'cluster_id': stat['cluster_id'], 'avg_sales': stat['avg_sales'], 'avg_frequency': stat['avg_frequency'], 'top_category': stat['top_category']} for stat in stats]
        return {'stats': stats, 'summaries': []}
    except Exception as e:
        raise ValueError(f"Summary generation failed: {str(e)}")