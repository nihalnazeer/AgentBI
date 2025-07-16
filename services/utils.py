
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_sales_data():
    try:
        file_path = "/Users/mohammednihal/Desktop/Business Intelligence/AgentBI/Backend/mock_data/sales_BI.csv"
        logger.info(f"Loading sales data from {file_path}")
        df = pd.read_csv(file_path)
        
        # Ensure correct column names
        if 'Customer ID' in df.columns:
            df = df.rename(columns={'Customer ID': 'CustomerID'})
        if 'Order Date' in df.columns:
            df = df.rename(columns={'Order Date': 'OrderDate'})
        
        # Verify required columns
        required_columns = ['CustomerID', 'OrderDate', 'Sales']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns in sales data: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert OrderDate to datetime
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
        logger.info(f"Loaded {len(df)} rows of sales data")
        return df
    except Exception as e:
        logger.error(f"Failed to load sales data: {str(e)}", exc_info=True)
        raise