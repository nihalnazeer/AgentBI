# AgentBI

AgentBI: AI-Driven Business Intelligence Dashboard
Overview
AgentBI is a powerful business intelligence platform designed to deliver actionable insights for e-commerce and retail clients. Built for an agency demo, it leverages a rich sales dataset to provide a dynamic cash flow dashboard, customer segmentation visualization, personalized email generation, price optimization, and promotion-sales correlation analysis. Powered by a FastAPI backend, React frontend, and Model Context Protocol (MCP) for task orchestration, AgentBI showcases modular, scalable AI workflows, with KMeans clustering as the core asset for customer segmentation.
Features

Cash Flow Dashboard: Interactive line chart displaying net cash flow (sales minus estimated costs) over weekly or monthly periods, enabling financial health monitoring.
Customer Segmentation: KMeans-based clustering visualized as a scatter plot (Recency vs. Monetary), identifying high-value, low-engagement, or dormant customers with LLM-generated summaries.
Email Generator: Produces personalized email campaigns for customer segments, enhancing engagement (e.g., targeted discounts for high-value customers).
Price Optimization: Recommends optimal prices per product or segment based on sales, discounts, and profitability, maximizing revenue.
Promotion-Sales Correlation: Analyzes the impact of discounts on sales by category, visualized as a heatmap or bar chart to guide marketing strategies.

Data
The platform uses a sales CSV (mock_data/sales.csv) with columns: Row ID, Order ID, Order Date, Customer ID, Customer Name, Segment, Country/Region, City, State, Postal Code, Region, Product ID, Category, Sub-Category, Product Name, Sales, Quantity, Discount, Profit. Additional mock data includes expenses.json (for cash flow) and promotions.json (for correlations).
Workflow

Data Input: Sales CSV is loaded by backend services (services/cashflow_engine.py, cluster_engine.py) for processing.
Backend Processing: Services compute cash flow metrics (from Sales, Order Date, Profit) and RFM features for KMeans clustering (Customer ID, Order Date, Sales).
MCP Orchestration: agent/mcp_runner.py routes tasks (e.g., cashflow, segmentation) using validated inputs (task_schemas.py) and LLM prompts (context_examples.py).
LLM Integration: Sarvam API (report.py, emailer.py) generates cluster summaries and emails, enhancing client-friendly insights.
API Endpoints: FastAPI (routers/mock_api.py, run_agent.py) serves JSON data for graphs (/cashflow-data/, /segment-customers/).
Frontend Visualization: React dashboard (frontend/src/pages/Dashboard.jsx) renders interactive Chart.js graphs (CashFlowCharts.jsx, SegmentCharts.jsx) with a polished Ant Design UI.

Technical Stack

Backend: FastAPI, Python, pandas, scikit-learn (KMeans), Sarvam API (LLM).
Frontend: React, Chart.js, Ant Design, axios.
Framework: MCP for task orchestration, Pydantic for input validation.
Deployment: Docker (deploy/Dockerfile), Nginx (nginx.conf).

File Structure
agentbi/
├── frontend/                    # React dashboard
│   ├── src/
│   │   ├── components/         # CashFlowCharts.jsx, SegmentCharts.jsx
│   │   ├── api/                # cashflowApi.js, segmentApi.js
│   │   ├── pages/              # Dashboard.jsx
├── backend/
│   ├── mock_data/              # sales.csv, expenses.json, promotions.json
│   ├── services/               # cashflow_engine.py, cluster_engine.py
│   ├── agent/                  # mcp_runner.py, task_schemas.py, tools/
│   ├── routers/                # mock_api.py, run_agent.py
│   ├── main.py                 # FastAPI startup
│   ├── config.py, .env         # Configuration
├── deploy/                     # Dockerfile, start.sh

Setup

Install backend dependencies: pip install -r backend/requirements.txt.
Install frontend dependencies: cd frontend && npm install.
Set Sarvam API key in backend/.env.
Run: deploy/start.sh (starts FastAPI and React via Docker).
Access: http://localhost:3000 for the dashboard, http://localhost:8000/docs for API docs.

Future Enhancements

Real-time data integration via API webhooks or ETL pipelines.
Additional clustering algorithms (e.g., k-prototypes for categorical data).
Expanded visualizations for pricing and promotion insights.

AgentBI is designed for scalability, ready to integrate with client CRMs or e-commerce platforms, delivering data-driven insights to boost revenue and engagement.
