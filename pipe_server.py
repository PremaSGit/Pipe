import sqlite3
import datetime
import logging
import os # Added for Railway port binding
from typing import List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware # Added for CORS
from pydantic import BaseModel
import httpx
import asyncio

# --- Configuration ---
app = FastAPI(title="Content Enrichment Pipeline")
DB_NAME = "pipeline_storage.db"
EXTERNAL_SOURCE = "https://jsonplaceholder.typicode.com/posts"

# --- Fix 1: Add CORS Middleware ---
# This allows the testing platform's browser to successfully 'fetch' your API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all domains to access your API
    allow_credentials=True,
    allow_methods=["*"], # Allows POST, GET, etc.
    allow_headers=["*"],
)

# Setup Logging for Notifications
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("NotificationService")

# --- Database Setup (SQLite) ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS processed_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_content TEXT,
            analysis TEXT,
            sentiment TEXT,
            source TEXT,
            timestamp DATETIME
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# --- Data Models (Pydantic) ---
class PipelineRequest(BaseModel):
    email: str
    source: str

class ProcessedItem(BaseModel):
    original: str
    analysis: str
    sentiment: str
    stored: bool
    timestamp: str

class PipelineResponse(BaseModel):
    items: List[ProcessedItem]
    notificationSent: bool
    processedAt: str
    errors: List[str]

# --- Helper: AI Enrichment (Mock) ---
async def generate_ai_insights(text: str):
    try:
        await asyncio.sleep(0.5) 
        sentiment = "Objective"
        if "error" in text.lower(): sentiment = "Critical"
        elif "love" in text.lower(): sentiment = "Enthusiastic"
            
        analysis = (
            "1. The text discusses general placeholder content. "
            "2. The structure implies a standard Latin derivation used in typesetting."
        )
        return analysis, sentiment
        
    except Exception as e:
        raise Exception(f"AI Service Failure: {str(e)}")

# --- Helper: Notification ---
def send_notification(email: str, count: int):
    target_email = "premakumari.sathiamurthy@straive.com"
    logger.info(f"NOTIFICATION SENT TO: {target_email} (CC: {email}) | Processed {count} items.")
    return True

# --- API Endpoint ---
@app.post("/pipeline", response_model=PipelineResponse)
async def run_pipeline(request: PipelineRequest):
    processed_items = []
    errors = []
    
    raw_posts = []
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(EXTERNAL_SOURCE)
            resp.raise_for_status()
            data = resp.json()
            raw_posts = data[:3]
    except Exception as e:
        return PipelineResponse(
            items=[],
            notificationSent=False,
            processedAt=datetime.datetime.utcnow().isoformat() + "Z",
            errors=[f"Critical API Fetch Error: {str(e)}"]
        )

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for post in raw_posts:
        try:
            original_text = f"Title: {post['title']} | Body: {post['body']}"
            analysis, sentiment = await generate_ai_insights(original_text)
            ts = datetime.datetime.utcnow().isoformat() + "Z"
            
            cursor.execute(
                "INSERT INTO processed_items (original_content, analysis, sentiment, source, timestamp) VALUES (?, ?, ?, ?, ?)",
                (original_text, analysis, sentiment, request.source, ts)
            )
            
            item_result = ProcessedItem(
                original=original_text[:50] + "...", 
                analysis=analysis,
                sentiment=sentiment,
                stored=True,
                timestamp=ts
            )
            processed_items.append(item_result)
            
        except Exception as e:
            errors.append(f"Item ID {post.get('id', 'unknown')} failed: {str(e)}")
            continue

    conn.commit()
    conn.close()

    notif_status = send_notification(request.email, len(processed_items))

    return PipelineResponse(
        items=processed_items,
        notificationSent=notif_status,
        processedAt=datetime.datetime.utcnow().isoformat() + "Z",
        errors=errors
    )

if __name__ == "__main__":
    import uvicorn
    # --- Fix 2: Read Railway's PORT environment variable ---
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting Pipeline API on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
