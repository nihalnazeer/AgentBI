import logging
import pandas as pd
import xgboost as xgb
import json
import os
import numpy as np
from datetime import datetime
from services.utils import load_sales_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def optimize_prices():
    try:
        # Load and validate data
        df = load_sales_data()
        if df.empty:
            raise ValueError("No data loaded from sales_BI.csv")
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        df = df.dropna(subset=['Sales', 'Discount', 'Quantity', 'Category', 'Sub-Category', 'Product Name', 'Order Date'])
        if df.empty:
            raise ValueError("No data available after dropping NA values")
        df['Price'] = df['Sales'] / (df['Quantity'] * (1 - df['Discount']))
        if df['Price'].isnull().all():
            raise ValueError("Price calculation resulted in all NA values; check Sales, Quantity, or Discount")
        df['Month'] = df['Order Date'].dt.month
        df['Year'] = df['Order Date'].dt.year

        # Add sale frequency and seasonality features
        df = df.sort_values('Order Date')
        df['days_between_sales'] = df.groupby(['Product Name'])['Order Date'].diff().dt.days.fillna(0)
        df['number_of_orders_last_month'] = df.groupby(['Product Name', 'Year', 'Month'])['Order Date'].transform('count')
        df['product_seasonality'] = df.groupby(['Product Name', 'Month'])['Sales'].transform('mean') / df['Sales'].mean()

        # Aggregate product data
        product_data = df.groupby(['Category', 'Sub-Category', 'Product Name']).agg({
            'Sales': 'sum',
            'Quantity': 'sum',
            'Price': 'mean',
            'Discount': 'mean',
            'days_between_sales': 'mean',
            'number_of_orders_last_month': 'mean',
            'product_seasonality': 'mean'
        }).reset_index()
        if product_data.empty:
            raise ValueError("No product data available after aggregation")

        # Prepare features for model training
        features = ['Discount', 'Quantity', 'Month', 'Year', 'Price', 'days_between_sales', 'number_of_orders_last_month', 'product_seasonality']
        categorical_cols = ['Category', 'Sub-Category']
        all_features = pd.get_dummies(df[features + categorical_cols], columns=categorical_cols)
        y_quantity = df['Quantity']  # Predict quantity for elasticity
        if len(all_features) != len(y_quantity):
            raise ValueError(f"Mismatch in feature and target lengths: {len(all_features)} vs {len(y_quantity)}")
        model_quantity = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=100, random_state=42)
        model_quantity.fit(all_features, y_quantity)
        logger.info("Quantity prediction model trained successfully with %d samples", len(y_quantity))

        # Optimize prices with multi-price simulation
        profit_threshold = 0.05
        optimizations = {}
        price_revenue_data = {}  # Data for frontend visualization
        for _, product in product_data.iterrows():
            product_df = df[df['Product Name'] == product['Product Name']]
            X_product = pd.get_dummies(product_df[features + categorical_cols], columns=categorical_cols)
            missing_cols = set(all_features.columns) - set(X_product.columns)
            for col in missing_cols:
                X_product[col] = 0
            X_product = X_product[all_features.columns]
            if X_product.empty:
                continue
            current_price = product['Price']
            current_revenue = product['Sales']

            # Multi-price simulation
            price_range = np.linspace(current_price * 0.8, current_price * 1.2, 10)
            revenues = []
            for new_price in price_range:
                X_sim = X_product.copy()
                X_sim['Price'] = new_price
                try:
                    predicted_quantity = model_quantity.predict(X_sim).mean()
                    revenue = predicted_quantity * new_price
                    revenues.append(revenue)
                except Exception as e:
                    logger.warning("Prediction failed for price %f: %s", new_price, str(e))
                    revenues.append(0)

            # Find optimal price
            optimal_index = np.argmax(revenues)
            optimal_price = price_range[optimal_index]
            optimal_revenue = revenues[optimal_index]
            revenue_impact = (optimal_revenue - current_revenue) / current_revenue

            if revenue_impact > profit_threshold:
                optimizations[product['Product Name']] = {
                    "category": product['Category'],
                    "sub_category": product['Sub-Category'],
                    "current_price": current_price,
                    "suggested_price": optimal_price,
                    "revenue_impact_%": revenue_impact * 100
                }
            # Store data for frontend
            price_revenue_data[product['Product Name']] = {
                "price_range": price_range.tolist(),
                "revenues": revenues
            }

        # Prepare output data with confidence estimation
        stats = {
            "timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"),
            "total_products_analyzed": len(product_data),
            "total_products_optimized": len(optimizations),
            "optimized_products": [
                {
                    "product_name": name,
                    "current_price": float(data["current_price"]),
                    "suggested_price": float(data["suggested_price"]),
                    "revenue_impact_%": float(data["revenue_impact_%"]),
                    "confidence_band": "±10%"  # Placeholder
                } for name, data in optimizations.items()
            ],
            "price_revenue_data": price_revenue_data  # Data for JS frontend
        }

        # Build llm_report without syntax errors
        date_str = datetime.now().strftime('%Y-%m-%d')
        base_report = f"Price Optimization - {date_str}\nOptimized {len(optimizations)} of {len(product_data)} products. "
        if not optimizations:
            llm_report = base_report + "No changes suggested."
        else:
            example = list(optimizations.keys())[0]
            example_data = list(optimizations.values())[0]
            llm_report = base_report + f"Example: {example} from ${example_data['current_price']:.2f} to ${example_data['suggested_price']:.2f} (+{example_data['revenue_impact_%']:.2f}%)."

        # Save to output directory
        output_dir = os.path.join(os.path.dirname(__file__), f"output_{datetime.now().strftime('%Y-%m-%d_%H:%M')}")
        os.makedirs(output_dir, exist_ok=True)
        try:
            with open(f"{output_dir}/price_optimization_stats.json", "w") as f:
                json.dump(stats, f, indent=2)
            with open(f"{output_dir}/price_optimization_report.json", "w") as f:
                json.dump({"timestamp": datetime.now().strftime("%Y-%m-%d_%H:%M"), "report": llm_report}, f, indent=2)
            logger.info("Price optimization saved to %s", output_dir)
        except PermissionError as e:
            logger.error("Permission denied when saving to %s: %s", output_dir, str(e))
            raise
        except Exception as e:
            logger.error("Failed to save files to %s: %s", output_dir, str(e))
            raise

        return {
            "stats": stats,
            "llm_report": llm_report
        }
    except Exception as e:
        logger.error("Price optimization failed: %s", str(e))
        return {"error": str(e), "details": str(e)}

if __name__ == "__main__":
    result = optimize_prices()
    print(result)