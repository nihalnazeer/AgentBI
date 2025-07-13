import logging
import pandas as pd
import json
import os
from datetime import datetime
from services.utils import load_sales_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_cash_flow():
    try:
        df = load_sales_data()
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        df = df.dropna(subset=['Sales', 'Order Date'])
        df['Month'] = df['Order Date'].dt.to_period('M').astype(str)  # Convert Period to string for JSON
        df['Year'] = df['Order Date'].dt.year

        monthly_data = df.groupby(['Year', 'Month']).agg({'Sales': 'sum'}).reset_index()
        monthly_data['Inflows'] = monthly_data['Sales']
        monthly_data['Outflows'] = monthly_data['Sales'] * 0.3  # 30% cost assumption
        monthly_data['Net_Cash_Flow'] = monthly_data['Inflows'] - monthly_data['Outflows']
        monthly_data['Profit_Margin_%'] = ((monthly_data['Net_Cash_Flow'] / monthly_data['Inflows']) * 100).round(2)

        monthly_data.loc[0, 'Trend'] = 'N/A'  # Use loc to avoid SettingWithCopyWarning
        monthly_data['Trend'] = monthly_data['Net_Cash_Flow'].pct_change().apply(
            lambda x: 'Positive' if x > 0 else 'Negative' if x < 0 else 'Stable'
        ).fillna('N/A')

        total_inflows = monthly_data['Inflows'].sum()
        total_outflows = monthly_data['Outflows'].sum()
        net_cash_flow = monthly_data['Net_Cash_Flow'].sum()
        avg_profit_margin = monthly_data['Profit_Margin_%'].mean()
        trend_summary = monthly_data['Trend'].value_counts().to_dict()

        analysis = {
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "monthly_data": monthly_data[['Year', 'Month', 'Inflows', 'Outflows', 'Net_Cash_Flow', 'Profit_Margin_%', 'Trend']].to_dict(orient='records'),
            "total_inflows": total_inflows,
            "total_outflows": total_outflows,
            "net_cash_flow": net_cash_flow,
            "avg_profit_margin_%": avg_profit_margin,
            "trend_summary": trend_summary
        }

        output_dir = os.path.join(os.path.dirname(__file__), f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/cash_flow_analysis.json", "w") as f:
            json.dump(analysis, f, indent=2)
        logger.info("Cash flow analysis saved to %s", output_dir)

        return analysis
    except Exception as e:
        logger.error("Cash flow analysis failed: %s", str(e))
        return {"error": str(e), "details": str(e)}