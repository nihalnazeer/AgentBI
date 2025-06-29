from fastapi import FastAPI
from routers.run_agent import router as agent_router

app = FastAPI(
    title="AgentBI API",
    description="Simple FastAPI server for testing",
    version="0.1.0"
)

# Include the agent router
app.include_router(agent_router)

@app.get("/")
async def root():
    return {"message": "Welcome to the AgentBI API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}