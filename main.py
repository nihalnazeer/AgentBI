
from fastapi import FastAPI
from run_agent import router
from dotenv import load_dotenv
import warnings

# Load environment variables
load_dotenv()

# Suppress urllib3 warnings
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

app = FastAPI()

# Include the router from run_agent.py
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)