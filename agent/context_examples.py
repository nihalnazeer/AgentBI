SUMMARY_PROMPT = """
Summarize customer segment in 50 words:
Cluster ID: {cluster_id}
Average Spend: ${avg_sales:.2f}
Average Orders: {avg_frequency:.2f}
Top Category: {top_category}
Provide actionable insights for targeting this segment.
"""