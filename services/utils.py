import pandas as pd
import os

def load_sales_data():
    # Use absolute path from project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    file_path = os.path.join(project_root, "Backend", "mock_data", "sales_BI.csv")
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError as e:
        raise FileNotFoundError(f"No such file or directory: {file_path}")
    except Exception as e:
        raise Exception(f"Error loading sales data: {str(e)}")