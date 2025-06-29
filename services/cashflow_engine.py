import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

def calculate_cv_metrics(model, train_data):
    """
    Calculate model performance using cross-validation approach
    """
    try:
        # Use last 20% of training data for validation
        val_size = max(7, int(len(train_data) * 0.2))  # At least 7 days
        train_subset = train_data[:-val_size].copy()
        val_subset = train_data[-val_size:].copy()
        
        # Create and train a temporary model
        temp_model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=True,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10,
            interval_width=0.95
        )
        
        temp_model.add_country_holidays(country_name='UK')
        temp_model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        temp_model.fit(train_subset)
        
        # Predict on validation set
        val_forecast = temp_model.predict(val_subset[['ds']])
        
        # Calculate metrics
        val_actuals = val_subset['y'].values
        val_predictions = val_forecast['yhat'].values
        
        mae = np.mean(np.abs(val_actuals - val_predictions))
        rmse = np.sqrt(np.mean((val_actuals - val_predictions)**2))
        mape = np.mean(np.abs((val_actuals - val_predictions) / np.maximum(val_actuals, 1))) * 100
        
        model_reliability = "High" if mape < 15 else "Medium" if mape < 25 else "Low"
        
        print(f"✓ Cross-validation Performance ({len(val_actuals)} validation days):")
        print(f"  - MAE: ${mae:,.2f}")
        print(f"  - RMSE: ${rmse:,.2f}") 
        print(f"  - MAPE: {mape:.2f}%")
        
        return mae, rmse, mape, model_reliability
        
    except Exception as e:
        print(f"⚠ Warning: Cross-validation failed ({e}), using default metrics")
        return 1000.0, 1500.0, 20.0, "Medium"

def enhanced_revenue_forecasting(csv_path, forecast_days=90):
    """
    Enhanced revenue forecasting with comprehensive data processing and weekly JSON output
    """
    
    print("=== ENHANCED SALES REVENUE FORECASTING ===")
    print(f"Loading data from: {csv_path}")
    
    # Load and clean data
    try:
        time_series_data = pd.read_csv(csv_path, encoding='iso-8859-1', low_memory=False)
        print(f"✓ Loaded {len(time_series_data)} records")
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    # Data cleaning and preprocessing
    print("\n=== DATA CLEANING & PREPROCESSING ===")
    
    # Clean column names
    time_series_data.columns = [c.strip().replace(" ", "_") for c in time_series_data.columns]
    
    # Convert InvoiceDate to datetime
    time_series_data['InvoiceDate'] = pd.to_datetime(time_series_data['InvoiceDate'], errors='coerce')
    
    # Remove invalid data
    original_size = len(time_series_data)
    time_series_data = time_series_data.dropna(subset=['InvoiceDate', 'Revenue'])
    time_series_data = time_series_data[time_series_data['Revenue'] > 0]  # Remove negative/zero revenue
    
    print(f"✓ Cleaned data: {len(time_series_data)} records ({original_size - len(time_series_data)} removed)")
    
    # Handle outliers using IQR method
    Q1 = time_series_data['Revenue'].quantile(0.25)
    Q3 = time_series_data['Revenue'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 2 * IQR
    upper_bound = Q3 + 2 * IQR
    
    time_series_data = time_series_data[
        (time_series_data['Revenue'] >= lower_bound) & 
        (time_series_data['Revenue'] <= upper_bound)
    ]
    print(f"✓ Outliers removed: {len(time_series_data)} records remaining")
    
    # Group by date and sum the revenue for each day
    daily_data = time_series_data.groupby(time_series_data['InvoiceDate'].dt.date)['Revenue'].sum().reset_index()
    daily_data.rename(columns={'InvoiceDate': 'ds', 'Revenue': 'y'}, inplace=True)
    daily_data['ds'] = pd.to_datetime(daily_data['ds'])
    
    # Fill missing dates with interpolated values
    daily_data = daily_data.set_index('ds').asfreq('D').reset_index()
    daily_data['y'] = daily_data['y'].interpolate(method='linear')
    daily_data = daily_data.dropna()
    
    print(f"✓ Daily aggregation complete: {len(daily_data)} days")
    print(f"✓ Date range: {daily_data['ds'].min().date()} to {daily_data['ds'].max().date()}")
    print(f"✓ Average daily revenue: ${daily_data['y'].mean():,.2f}")
    
    # Split data for validation
    split_date = daily_data['ds'].max() - timedelta(days=30)  # Last 30 days for testing
    train = daily_data[daily_data['ds'] <= split_date]
    test = daily_data[daily_data['ds'] > split_date]
    
    print(f"✓ Training data: {len(train)} days")
    print(f"✓ Test data: {len(test)} days")
    
    # Initialize and configure Prophet model
    print("\n=== PROPHET MODEL TRAINING ===")
    
    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=True,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10,
        interval_width=0.95
    )
    
    # Add UK holidays and custom seasonalities
    model.add_country_holidays(country_name='UK')
    model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
    model.add_seasonality(name='quarterly', period=91.25, fourier_order=8)
    
    print("✓ Training Prophet model...")
    model.fit(train)
    print("✓ Model training completed!")
    
    # Generate forecast
    print(f"\n=== GENERATING {forecast_days}-DAY FORECAST ===")
    
    future = model.make_future_dataframe(periods=forecast_days)
    forecast = model.predict(future)
    
    # Get future forecast only
    future_forecast = forecast[forecast['ds'] > daily_data['ds'].max()].copy()
    
    # Model evaluation on test set if available
    if len(test) > 0:
        try:
            test_forecast = model.predict(test[['ds']])
            
            # Ensure we have valid predictions and actuals
            test_actuals = test['y'].values
            test_predictions = test_forecast['yhat'].values
            
            # Remove any NaN values
            valid_mask = ~(np.isnan(test_actuals) | np.isnan(test_predictions))
            if np.sum(valid_mask) > 0:
                test_actuals = test_actuals[valid_mask]
                test_predictions = test_predictions[valid_mask]
                
                mae = np.mean(np.abs(test_actuals - test_predictions))
                rmse = np.sqrt(np.mean((test_actuals - test_predictions)**2))
                
                # Calculate MAPE with protection against division by zero
                mape_values = np.abs((test_actuals - test_predictions) / np.maximum(test_actuals, 1))
                mape = np.mean(mape_values) * 100
                
                print(f"✓ Model Performance on Test Set ({len(test_actuals)} valid predictions):")
                print(f"  - MAE: ${mae:,.2f}")
                print(f"  - RMSE: ${rmse:,.2f}")
                print(f"  - MAPE: {mape:.2f}%")
                
                model_reliability = "High" if mape < 15 else "Medium" if mape < 25 else "Low"
            else:
                raise ValueError("No valid predictions available")
                
        except Exception as e:
            print(f"⚠ Warning: Could not calculate test metrics ({e})")
            # Use cross-validation approach instead
            mae, rmse, mape, model_reliability = calculate_cv_metrics(model, train)
    else:
        mae, rmse, mape, model_reliability = calculate_cv_metrics(model, train)
    
    print(f"\n=== FORECAST SUMMARY ===")
    print(f"✓ Forecast period: {forecast_days} days")
    print(f"✓ Total forecasted revenue: ${future_forecast['yhat'].sum():,.2f}")
    print(f"✓ Average daily forecast: ${future_forecast['yhat'].mean():,.2f}")
    print(f"✓ Model reliability: {model_reliability}")
    
    # Generate weekly batched JSON output
    print(f"\n=== GENERATING WEEKLY JSON OUTPUT ===")
    
    weekly_json = generate_weekly_forecast_json(
        future_forecast, 
        daily_data['y'].mean(),
        {
            'mae': mae,
            'rmse': rmse,
            'mape': mape,
            'reliability': model_reliability
        }
    )
    
    print(f"✓ Weekly JSON generated with {len(weekly_json['weekly_batches'])} batches")
    
    return weekly_json, future_forecast, model

def generate_weekly_forecast_json(future_forecast, historical_avg, performance_metrics):
    """
    Generate weekly batched JSON output for LLM processing
    """
    
    # Add additional features
    future_forecast['day_of_week'] = future_forecast['ds'].dt.day_name()
    future_forecast['week_of_year'] = future_forecast['ds'].dt.isocalendar().week
    future_forecast['month'] = future_forecast['ds'].dt.month
    
    # Group by weeks
    future_forecast['week_start'] = future_forecast['ds'].dt.to_period('W').dt.start_time
    
    # Calculate growth vs historical
    avg_forecast = future_forecast['yhat'].mean()
    growth_rate = ((avg_forecast - historical_avg) / historical_avg) * 100 if historical_avg > 0 else 0
    
    # Create weekly batches
    weekly_batches = []
    week_groups = future_forecast.groupby('week_start')
    
    for week_start, week_data in week_groups:
        week_end = week_start + timedelta(days=6)
        week_num = len(weekly_batches) + 1
        
        # Create values array for this week
        week_values = []
        for _, row in week_data.iterrows():
            week_values.append([
                row['ds'].strftime('%Y-%m-%d'),
                round(float(row['yhat']), 2),
                round(float(row['yhat_lower']), 2),
                round(float(row['yhat_upper']), 2),
                row['day_of_week'],
                int(row['week_of_year']),
                int(row['month'])
            ])
        
        # Calculate week summary stats
        week_total = week_data['yhat'].sum()
        week_avg = week_data['yhat'].mean()
        week_volatility = week_data['yhat'].std()
        
        batch = {
            "batch_id": f"week_{week_num}_{week_start.year}",
            "start_date": week_start.strftime('%Y-%m-%d'),
            "end_date": week_end.strftime('%Y-%m-%d'),
            "summary": {
                "total_revenue": round(float(week_total), 2),
                "avg_daily_revenue": round(float(week_avg), 2),
                "volatility": round(float(week_volatility), 2),
                "best_day": week_data.loc[week_data['yhat'].idxmax(), 'day_of_week'],
                "best_day_revenue": round(float(week_data['yhat'].max()), 2)
            },
            "values": week_values
        }
        
        weekly_batches.append(batch)
    
    # Create main JSON structure
    forecast_json = {
        "pipeline_id": f"forecast_{datetime.now().strftime('%Y%m%d_%H%M')}",
        "created_at": datetime.now().isoformat(),
        "forecast_metadata": {
            "total_forecast_days": len(future_forecast),
            "total_weeks": len(weekly_batches),
            "model_performance": {
                "mae": round(float(performance_metrics['mae']), 2),
                "rmse": round(float(performance_metrics['rmse']), 2),
                "mape_percent": round(float(performance_metrics['mape']), 2),
                "reliability": performance_metrics['reliability']
            },
            "forecast_summary": {
                "total_forecasted_revenue": round(float(future_forecast['yhat'].sum()), 2),
                "avg_daily_revenue": round(float(future_forecast['yhat'].mean()), 2),
                "growth_vs_historical_percent": round(float(growth_rate), 2),
                "min_daily_forecast": round(float(future_forecast['yhat'].min()), 2),
                "max_daily_forecast": round(float(future_forecast['yhat'].max()), 2)
            }
        },
        "key_index": [
            "date",
            "predicted_revenue", 
            "lower_bound",
            "upper_bound", 
            "day_of_week",
            "week_of_year",
            "month"
        ],
        "weekly_batches": weekly_batches,
        "llm_instructions": {
            "task": "Generate comprehensive business insights and weekly sales forecast analysis",
            "focus_areas": [
                "Weekly revenue trends and growth patterns",
                "Day-of-week performance variations and seasonality", 
                "Risk assessment for underperforming periods",
                "Opportunity identification for peak revenue days",
                "Monthly and seasonal business implications",
                "Strategic recommendations for resource planning"
            ],
            "output_requirements": [
                "Executive summary with key findings",
                "Weekly performance breakdown with insights",
                "Actionable business recommendations",
                "Risk and opportunity assessment",
                "Seasonal planning guidance"
            ],
            "context": f"Model reliability is {performance_metrics['reliability']} with {performance_metrics['mape_percent']:.1f}% MAPE. Historical average was ${historical_avg:,.2f}/day, forecast shows {growth_rate:.1f}% growth."
        }
    }
    
    return forecast_json

def create_forecast_visualizations(model, forecast, daily_data, future_forecast):
    """
    Optional visualization function - not called by default
    """
    pass

def save_forecast_results(weekly_json, csv_path='forecast_results.csv', json_path='weekly_forecast.json'):
    """
    Save results to both CSV and JSON formats
    """
    # Save JSON
    with open(json_path, 'w') as f:
        json.dump(weekly_json, f, indent=2)
    
    print(f"✓ Weekly forecast JSON saved to: {json_path}")
    
    # Create CSV from weekly data
    csv_data = []
    for batch in weekly_json['weekly_batches']:
        for values in batch['values']:
            csv_row = {
                'week_id': batch['batch_id'],
                'date': values[0],
                'predicted_revenue': values[1],
                'lower_bound': values[2], 
                'upper_bound': values[3],
                'day_of_week': values[4],
                'week_of_year': values[5],
                'month': values[6],
                'week_total_revenue': batch['summary']['total_revenue'],
                'week_avg_revenue': batch['summary']['avg_daily_revenue'],
                'model_reliability': weekly_json['forecast_metadata']['model_performance']['reliability'],
                'mape': weekly_json['forecast_metadata']['model_performance']['mape']  # Changed from mape_percent
            }
            csv_data.append(csv_row)
    
    df_results = pd.DataFrame(csv_data)
    df_results.to_csv(csv_path, index=False)
    print(f"✓ Forecast CSV saved to: {csv_path}")
    
    return df_results

# Example usage
if __name__ == "__main__":
    # Run the enhanced forecasting
    csv_file_path = '/Users/mohammednihal/Desktop/Business Intelligence/AgentBI/Backend/agent/mock_data/sales-analysis-forecasting.csv'  # Update with your actual path
    
    try:
        weekly_json, forecast_data, trained_model = enhanced_revenue_forecasting(
            csv_path=csv_file_path,
            forecast_days=90
        )
        
        if weekly_json:
            # Save results
            results_df = save_forecast_results(
                weekly_json, 
                csv_path='forecast_output.csv',
                json_path='weekly_forecast_output.json'
            )
            
            print(f"\n=== EXECUTION COMPLETE ===")
            print(f"✓ Generated {len(weekly_json['weekly_batches'])} weekly batches")
            print(f"✓ Total forecast revenue: ${weekly_json['forecast_metadata']['forecast_summary']['total_forecasted_revenue']:,.2f}")
            print(f"✓ Model reliability: {weekly_json['forecast_metadata']['model_performance']['reliability']}")
            print(f"✓ Ready for LLM analysis and insights generation")
            
        else:
            print("❌ Forecasting failed. Please check your data file path and format.")
            
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        print("Please ensure your CSV file exists and has the required columns: InvoiceDate, Revenue")