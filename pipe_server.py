import sqlite3
import requests
import time
import json
import random
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- CONFIGURATION ---
DATA_SOURCE_URL = "https://jsonplaceholder.typicode.com/posts"
DB_NAME = "pipeline_data.db"

# --- DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS processed_posts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  original_id INTEGER,
                  title TEXT,
                  body TEXT,
                  insights TEXT,
                  sentiment TEXT,
                  processed_at TIMESTAMP)''')
    conn.commit()
    conn.close()

# Initialize DB on start
init_db()

# --- MOCK LLM (Runs locally without API Key) ---
def mock_llm_analysis(text):
    """
    Simulates an LLM analysis to ensure this code runs immediately for you.
    Replace this function with actual OpenAI/Anthropic calls if needed.
    """
    # Simple keyword-based sentiment
    positive_words = ['good', 'great', 'happy', 'sun', 'qui', 'est'] # 'est' is common in latin filler
    sentiment = "objective"
    if any(w in text.lower() for w in positive_words):
        sentiment = "enthusiastic"
    elif "error" in text.lower() or "dolor" in text.lower():
        sentiment = "critical"
        
    insights = [
        f"The text focuses on key themes regarding '{text.split()[0]}'.",
        f"This post contains {len(text.split())} words, indicating high density.",
        "The tone suggests a formal communication style."
    ]
    
    return {
        "insights": " ".join(insights),
        "sentiment": sentiment
    }

# --- PIPELINE STAGES ---

def stage_fetch_data(limit=3):
    try:
        # Disable SSL verification
        response = requests.get(DATA_SOURCE_URL, timeout=5, verify=False)
        response.raise_for_status()
        posts = response.json()
        return posts[:limit] # Return first 3
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def stage_process_item(item):
    # Combine title and body for analysis
    content = f"{item['title']}\n{item['body']}"
    
    # Call AI (Mocked for stability)
    analysis_result = mock_llm_analysis(content)
    
    return {
        "original_id": item['id'],
        "original_content": content,
        "analysis": analysis_result['insights'],
        "sentiment": analysis_result['sentiment'],
        "timestamp": datetime.utcnow().isoformat()
    }

def stage_store_item(processed_item):
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("INSERT INTO processed_posts (original_id, title, body, insights, sentiment, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
                  (processed_item['original_id'], 
                   processed_item['original_content'].split('\n')[0], # Title
                   processed_item['original_content'], 
                   processed_item['analysis'], 
                   processed_item['sentiment'], 
                   processed_item['timestamp']))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Storage Error: {e}")
        return False

def stage_notify(email, count):
    # Simulate sending email
    print(f"--- NOTIFICATION SENT TO {email} ---")
    print(f"Subject: Pipeline Complete. Processed {count} items.")
    return True

# --- API ENDPOINT ---

@app.route('/pipeline', methods=['POST'])
def run_pipeline():
    data = request.json or {}
    notification_email = data.get('email', 'admin@example.com')
    
    results = []
    errors = []
    
    # 1. Fetch
    raw_items = stage_fetch_data(limit=3)
    if not raw_items:
        return jsonify({"error": "Failed to fetch data from source"}), 502

    # 2. Process & Store Loop
    for item in raw_items:
        try:
            # AI Enrichment
            processed = stage_process_item(item)
            
            # Storage
            stored = stage_store_item(processed)
            
            results.append({
                "original": processed['original_content'][:50] + "...", # Truncated for display
                "analysis": processed['analysis'],
                "sentiment": processed['sentiment'],
                "stored": stored,
                "timestamp": processed['timestamp']
            })
            
        except Exception as e:
            errors.append(f"Item {item.get('id', 'unknown')} failed: {str(e)}")

    # 3. Notify
    notified = stage_notify(notification_email, len(results))

    # 4. Return Response
    response_payload = {
        "items": results,
        "notificationSent": notified,
        "processedAt": datetime.utcnow().isoformat(),
        "errors": errors,
        "recordCount": len(results)
    }
    
    return jsonify(response_payload), 200

if __name__ == '__main__':
    # Running on port 8000
    app.run(host='0.0.0.0', port=8000)
