import pandas as pd
import os

def load_sales_data():
    data_path = os.path.join(os.path.dirname(__file__), '../mock_data/sales_BI.csv')
    return pd.read_csv(data_path)