import logging
import json
import os
import csv
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from services.utils import load_sales_data
from services.cluster_engine import run_clustering
from services.cashflow_engine import analyze_cash_flow
from services.pricing_engine import optimize_prices

# Load environment variables
load_dotenv()
api_key = os.getenv("OPENROUTER_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def evaluate_triggers():
    try:
        sales_data = load_sales_data().to_dict(orient='records')
        segmentation_result = run_clustering(sales_data, n_clusters=3, max_graph_customers=50, include_reports=True)
        cash_flow_result = analyze_cash_flow()
        price_result = optimize_prices()

        historical_stats = {}
        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "engine", f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
        if os.path.exists(f"{output_dir}/stats.json"):
            with open(f"{output_dir}/stats.json", "r") as f:
                historical_stats = json.load(f)["stats"]
        historical_cash_flow = {}
        if os.path.exists(f"{output_dir}/cash_flow_analysis.json"):
            with open(f"{output_dir}/cash_flow_analysis.json", "r") as f:
                historical_cash_flow = json.load(f)

        graph_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "high_customers": next((s["customer_count"] for s in segmentation_result["stats"] if s["cluster_label"] == "High"), 0),
            "net_cash_flow": cash_flow_result["net_cash_flow"],
            "current_revenue": price_result.get("stats", {}).get("total_products_analyzed", 0) * price_result.get("stats", {}).get("optimized_products", [{}])[0].get("current_price", 0)
        }

        alerts = []
        llm_inputs = ["No alerts triggered based on current data."]
        llm_responses = ["No responses generated based on current data."]
        emails = []

        # Customer behavior (high-value customer change)
        high_cluster = next((s for s in segmentation_result["stats"] if s["cluster_label"] == "High"), {})
        high_historical = next((s for s in historical_stats if s["cluster_label"] == "High"), {})
        if high_cluster and high_historical and high_cluster["customer_count"] > 0:
            decrease_pct = ((high_historical["customer_count"] - high_cluster["customer_count"]) / high_historical["customer_count"] * 100)
            if abs(decrease_pct) > 10:  # Trigger on significant increase or decrease
                alert_type = "customer_behavior_increase" if decrease_pct < 0 else "customer_behavior_decrease"
                context = f"High-value customers {'increased' if decrease_pct < 0 else 'decreased'} by {abs(decrease_pct):.2f}% ({high_historical['customer_count']} to {high_cluster['customer_count']})."
                alert = {"type": alert_type, "context": context}
                alerts.append(alert)
                llm_inputs = [f"Generate {alert_type.replace('_', ' ')} email for admin: {context} Action required."]
                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "HTTP-Referer": "http://your-site-url.com",
                        "X-Title": "Your Site Name"
                    },
                    json={
                        "model": "mistralai/mistral-small-3.2-24b-instruct:free",
                        "messages": [{"role": "user", "content": llm_inputs[0]}]
                    }
                )
                llm_responses = [response.json().get("choices", [{}])[0].get("message", {}).get("content", "API error")]
                emails.append({
                    "type": alert_type,
                    "subject": f"Alert - {alert_type.replace('_', ' ').title()}",
                    "body": llm_responses[0] or f"Dear Admin,\n{context} Please take action immediately.\nBest,\nAgentBI",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        # Cash flow analysis (sale spike or loss last week)
        if len(cash_flow_result["monthly_data"]) >= 2:
            last_week = cash_flow_result["monthly_data"][-1]
            prev_week = cash_flow_result["monthly_data"][-2]
            change_pct = ((last_week["Net_Cash_Flow"] - prev_week["Net_Cash_Flow"]) / prev_week["Net_Cash_Flow"] * 100)
            if abs(change_pct) > 10:
                alert_type = "sale_spike" if change_pct > 0 else "sale_loss"
                context = f"{'Sale spike' if change_pct > 0 else 'Sale loss'} of {abs(change_pct):.2f}% last week ({prev_week['Net_Cash_Flow']:.2f} to {last_week['Net_Cash_Flow']:.2f})."
                alert = {"type": alert_type, "context": context}
                alerts.append(alert)
                llm_inputs = [f"Generate {alert_type.replace('_', ' ')} email for admin: {context} Action required."]
                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "HTTP-Referer": "http://your-site-url.com",
                        "X-Title": "Your Site Name"
                    },
                    json={
                        "model": "mistralai/mistral-small-3.2-24b-instruct:free",
                        "messages": [{"role": "user", "content": llm_inputs[0]}]
                    }
                )
                llm_responses = [response.json().get("choices", [{}])[0].get("message", {}).get("content", "API error")]
                emails.append({
                    "type": alert_type,
                    "subject": f"Alert - {alert_type.replace('_', ' ').title()}",
                    "body": llm_responses[0] or f"Dear Admin,\n{context} Please take action immediately.\nBest,\nAgentBI",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        # Price optimization suggestion
        if "llm_report" in price_result:
            alert = {"type": "price_optimization", "context": price_result["llm_report"]}
            alerts.append(alert)
            llm_inputs = [f"Generate price optimization notification email for marketing: {alert['context']} Share with team."]
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "HTTP-Referer": "http://your-site-url.com",
                    "X-Title": "Your Site Name"
                },
                json={
                    "model": "mistralai/mistral-small-3.2-24b-instruct:free",
                    "messages": [{"role": "user", "content": llm_inputs[0]}]
                }
            )
            llm_responses = [response.json().get("choices", [{}])[0].get("message", {}).get("content", "API error")]
            emails.append({
                "type": "price_optimization",
                "subject": "New Price Optimization",
                "body": llm_responses[0] or f"Dear Marketing Team,\n{alert['context']} Please share with the team.\nBest,\nAgentBI",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        os.makedirs(output_dir, exist_ok=True)
        try:
            with open(f"{output_dir}/graph_data.json", "w") as f:
                json.dump(graph_data, f, indent=2)
            with open(f"{output_dir}/llm_inputs.json", "w") as f:
                json.dump({"timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"), "inputs": llm_inputs}, f, indent=2)
            with open(f"{output_dir}/llm_responses.json", "w") as f:
                json.dump({"timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"), "responses": llm_responses}, f, indent=2)
            with open(f"{output_dir}/emails.csv", "w", newline='') as f:
                fieldnames = ["type", "subject", "body", "timestamp"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(emails)
            logger.info("Trigger evaluation files saved to %s", output_dir)
        except Exception as e:
            logger.error("Failed to save trigger files to %s: %s", output_dir, str(e))
            raise

        logger.info("Trigger evaluation completed, %d alerts generated", len(alerts))
        return {"status": "evaluated", "alert_count": len(alerts)}
    except Exception as e:
        logger.error("Trigger evaluation failed: %s", str(e))
        return {"error": str(e), "details": str(e)}
