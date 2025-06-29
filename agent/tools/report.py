import json
import os
from dotenv import load_dotenv
import requests
from agent.context_examples import SUMMARY_PROMPT

load_dotenv()
SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")

def generate_summary(payload: dict) -> str:
    if not SARVAM_API_KEY:
        raise ValueError("SARVAM_API_KEY not found in .env")
    
    cache_file = os.path.join(os.path.dirname(__file__), '../../cache/llm_outputs.json')
    cache = {}
    
    # Load cache
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    
    # Check cache
    cache_key = f"cluster_{payload['cluster_id']}_{payload['avg_sales']}_{payload['avg_frequency']}_{payload['top_category']}"
    if cache_key in cache:
        return cache[cache_key]
    
    # Generate prompt
    prompt = SUMMARY_PROMPT.format(**payload)
    
    # Sarvam API call (placeholder, replace with actual endpoint)
    try:
        headers = {"Authorization": f"Bearer {SARVAM_API_KEY}"}
        response = requests.post(
            "https://api.sarvam.ai/generate",  # Replace with actual Sarvam endpoint
            headers=headers,
            json={"prompt": prompt, "max_tokens": 100}
        )
        response.raise_for_status()
        summary = response.json().get("text", "").strip()
    except requests.RequestException as e:
        summary = f"Segment {payload['cluster_id']}: ${payload['avg_sales']:.2f} avg. spend, {payload['avg_frequency']:.2f} orders, prefers {payload['top_category']}. Target with {payload['top_category']} offers."
        print(f"Sarvam API error: {e}. Using fallback summary.")
    
    # Cache result
    cache[cache_key] = summary
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    with open(cache_file, 'w') as f:
        json.dump(cache, f)
    
    return summary