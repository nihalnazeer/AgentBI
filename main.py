from fastapi import FastAPI
from routers.run_agent import router as agent_router
from dotenv import load_dotenv
import warnings

# Load environment variables
load_dotenv()

# Suppress urllib3 warning
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

app = FastAPI()

app.include_router(agent_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)