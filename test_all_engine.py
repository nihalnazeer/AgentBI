import requests
import json
import os
import time
from datetime import datetime

BASE_URL = "http://127.0.0.1:8080"

def test_all_engines():
    try:
        print("Starting tests. Ensure the server is running at http://127.0.0.1:8080...")
        
        # Load sample sales data with explicit path
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        sales_file_path = os.path.join(project_root, "Backend", "mock_data", "sales_BI.csv")
        with open(sales_file_path, "r") as f:
            sales_data = [line.strip() for line in f]  # Adjust parsing as needed based on CSV format

        print("Testing Segmentation Engine...")
        segment_response = requests.post(
            f"{BASE_URL}/api/segment-customers/?n_clusters=3&max_graph_customers=50&include_reports=true&include_summaries=true",
            json={"sales_data": sales_data},  # Pass pre-loaded data
            headers={"Content-Type": "application/json"}
        )
        segment_data = segment_response.json()
        if segment_response.status_code == 200:
            print("Segmentation successful. Data saved to Backend/engine/output_<timestamp>/.")
        else:
            print(f"Segmentation failed: {segment_data}")

        print("Testing Price Optimization Engine...")
        price_response = requests.post(
            f"{BASE_URL}/api/optimize-prices/",
            headers={"Content-Type": "application/json"}
        )
        price_data = price_response.json()
        if price_response.status_code == 200:
            print("Price optimization successful. Data saved to Backend/engine/output_<timestamp>/.")
        else:
            print(f"Price optimization failed: {price_data}")

        print("Testing Cash Flow Analysis Engine...")
        cash_flow_response = requests.post(
            f"{BASE_URL}/api/analyze-cash-flow/",
            headers={"Content-Type": "application/json"}
        )
        cash_flow_data = cash_flow_response.json()
        if cash_flow_response.status_code == 200:
            print("Cash flow analysis successful. Data saved to Backend/engine/output_<timestamp>/.")
        else:
            print(f"Cash flow analysis failed: {cash_flow_data}")

        print("Testing Trigger Engine...")
        time.sleep(2)  # Wait for previous operations
        trigger_response = requests.post(
            f"{BASE_URL}/api/evaluate-triggers/",
            headers={"Content-Type": "application/json"}
        )
        trigger_data = trigger_response.json()
        if trigger_response.status_code == 200:
            print("Trigger evaluation successful. Data saved to Backend/engine/output_<timestamp>/.")
        else:
            print(f"Trigger evaluation failed: {trigger_data}")

        # Wait and find the latest output folder in engine
        time.sleep(1)  # Additional delay to ensure folder creation
        base_dir = "/Users/mohammednihal/Desktop/Business Intelligence/AgentBI/Backend/engine"
        output_folders = [f for f in os.listdir(base_dir) if f.startswith("output_")]
        if not output_folders:
            print("No output folder found. Check server logs for errors.")
            return
        latest_folder = max(output_folders, key=lambda x: datetime.strptime(x, "output_%Y-%m-%d_%H:%M"), default=None)
        output_dir = os.path.join(base_dir, latest_folder)

        output_files = {
            "Segmentation": [
                f"{output_dir}/graph_data.json",
                f"{output_dir}/stats.json",
                f"{output_dir}/reports.json",
                f"{output_dir}/segmentation_summaries.json"
            ],
            "Price Optimization": [
                f"{output_dir}/price_optimization_stats.json",
                f"{output_dir}/price_optimization_report.json"
            ],
            "Cash Flow Analysis": [
                f"{output_dir}/cash_flow_analysis.json"
            ],
            "Trigger Engine": [
                f"{output_dir}/graph_data.json",
                f"{output_dir}/llm_inputs.json",
                f"{output_dir}/llm_responses.json",
                f"{output_dir}/emails.csv"
            ]
        }
        
        for engine, files in output_files.items():
            for file_path in files:
                if os.path.exists(file_path):
                    print(f"{engine} - {file_path} generated successfully.")
                else:
                    print(f"{engine} - {file_path} not found.")

        print("All tests completed.")

    except Exception as e:
        print(f"Test failed: {str(e)}")

if __name__ == "__main__":
    test_all_engines()