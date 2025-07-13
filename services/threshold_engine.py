import logging
import json
import os
from datetime import datetime, timedelta
from services.utils import load_sales_data
from services.cluster_engine import run_clustering
from services.price_optimization_engine import optimize_prices
from services.cash_flow_analysis_engine import analyze_cash_flow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_thresholds():
    try:
        # Load and process data from all engines
        sales_data = load_sales_data().to_dict(orient='records')
        segmentation_result = run_clustering(sales_data, n_clusters=3, max_graph_customers=50, include_reports=True)
        price_result = optimize_prices()
        cash_flow_result = analyze_cash_flow()

        # Load historical data for comparison (simulated)
        historical_stats = {}
        if os.path.exists("Backend/output/stats.json"):
            with open("Backend/output/stats.json", "r") as f:
                historical_stats = json.load(f)["stats"]
        historical_cash_flow = {}
        if os.path.exists("/data/strategies/cash_flow_analysis.json"):
            with open("/data/strategies/cash_flow_analysis.json", "r") as f:
                historical_cash_flow = json.load(f)

        # Apply threshold rules
        emails = []
        current_stats = segmentation_result["stats"]
        current_cash_flow = cash_flow_result["monthly_data"]

        # Rule 1: High-value customers decrease by >10%
        high_cluster = next((s for s in current_stats if s["cluster_label"] == "High"), {})
        high_historical = next((s for s in historical_stats if s["cluster_label"] == "High"), {})
        if high_cluster and high_historical and high_cluster["customer_count"] > 0:
            decrease_pct = ((high_historical["customer_count"] - high_cluster["customer_count"]) / high_historical["customer_count"] * 100)
            if decrease_pct > 10:
                emails.append({
                    "to": "admin@example.com",
                    "subject": "Alert: High-Value Customer Loss",
                    "body": f"High-value customers decreased by {decrease_pct:.2f}% ({high_historical['customer_count']} to {high_cluster['customer_count']}). Action required."
                })

        # Rule 2: Net cash flow negative 2 weeks in a row
        if len(current_cash_flow) >= 2:
            last_two_weeks = current_cash_flow[-2:]
            if all(row["Net_Cash_Flow"] < 0 for row in last_two_weeks):
                emails.append({
                    "to": "admin@example.com",
                    "subject": "Escalation: Negative Cash Flow",
                    "body": f"Net cash flow negative for 2 weeks: {last_two_weeks[0]['Net_Cash_Flow']:.2f} and {last_two_weeks[1]['Net_Cash_Flow']:.2f}. Immediate review needed."
                })

        # Rule 3: Price optimization available
        if "llm_report" in price_result:
            emails.append({
                "to": "marketing@example.com",
                "subject": "New Price Optimization Available",
                "body": price_result["llm_report"]
            })

        # Save to queued emails
        output_dir = "/data/emails"
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/queued_emails.json", "w") as f:
            json.dump({"emails": emails, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
        logger.info("Queued emails saved to %s", f"{output_dir}/queued_emails.json")

        return {"status": "checked", "queued_emails": len(emails)}
    except Exception as e:
        logger.error("Threshold check failed: %s", str(e))
        return {"error": str(e)}