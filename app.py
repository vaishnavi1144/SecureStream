"""
SecureStream - Real-Time Application Log Monitoring & Incident Alerting Platform
Web Dashboard & Live Streaming Server
"""

import asyncio
import json
import os
import random
import subprocess
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

# Ensure repository paths are on sys.path
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

# Import the existing incident detector
try:
    from consumer.detector import detect_incident
except ImportError:
    # Fallback to local import if needed
    try:
        from detector import detect_incident
    except ImportError:
        def detect_incident(log: Dict[str, Any]) -> Dict[str, Any]:
            reasons = []
            if log.get("status_code", 0) >= 500:
                reasons.append("HTTP server error")
            if log.get("response_time_ms", 0) > 3000:
                reasons.append("Slow API response")
            if log.get("level") == "CRITICAL":
                reasons.append("Critical application error")
            return {
                "incident": len(reasons) > 0,
                "reasons": reasons
            }

app = FastAPI(
    title="SecureStream Live Dashboard",
    description="Real-Time Application Log Monitoring & Incident Alerting Platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Global State & In-Memory Store
# ---------------------------------------------------------------------------
SERVICES = [
    "checkout-api",
    "payment-api",
    "authentication-service",
    "inventory-api"
]

SUCCESS_MESSAGES = [
    "Request completed successfully",
    "Database connection established",
    "User authentication completed",
    "Inventory lookup completed",
    "Payment request processed"
]

class StreamState:
    def __init__(self):
        self.is_running: bool = True
        self.interval: float = 1.5
        self.total_logs: int = 0
        self.total_incidents: int = 0
        self.total_s3_stored: int = 0
        self.total_sqs_sent: int = 0
        self.total_sns_published: int = 0
        self.response_times: deque = deque(maxlen=60)
        self.logs_buffer: deque = deque(maxlen=250)
        self.incidents_buffer: deque = deque(maxlen=100)
        self.sns_alerts_buffer: deque = deque(maxlen=50)
        self.s3_storage: Dict[str, Dict[str, Any]] = {}  # key -> object
        self.sqs_queue: List[Dict[str, Any]] = []
        self.subscribers: List[asyncio.Queue] = []
        self.lock = asyncio.Lock()

state = StreamState()

# ---------------------------------------------------------------------------
# Core Generation & Pipeline Processing
# ---------------------------------------------------------------------------
def generate_sample_log(custom_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if custom_params:
        service = custom_params.get("service") or random.choice(SERVICES)
        status_code = int(custom_params.get("status_code", 200))
        response_time = int(custom_params.get("response_time_ms", random.randint(80, 600)))
        level = custom_params.get("level")
        message = custom_params.get("message")
        if not level:
            if status_code >= 500:
                level = "ERROR"
            elif response_time > 3000:
                level = "WARNING"
            else:
                level = "INFO"
        if not message:
            if status_code >= 500:
                message = "Service request failed with internal error"
            else:
                message = random.choice(SUCCESS_MESSAGES)
    else:
        service = random.choice(SERVICES)
        status_code = random.choice([200, 200, 200, 200, 201, 200, 500, 503])
        response_time = random.randint(50, 4800)
        if status_code >= 500:
            level = "ERROR"
            message = "Service request failed"
        elif response_time > 3500 and random.random() < 0.2:
            level = "CRITICAL"
            message = "Service health check timeout failure"
        else:
            level = "INFO"
            message = random.choice(SUCCESS_MESSAGES)

    event_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "service": service,
        "environment": "production",
        "level": level,
        "status_code": status_code,
        "response_time_ms": response_time,
        "message": message,
        # Kafka & mTLS Transport metadata
        "kafka_meta": {
            "topic": "application.logs",
            "partition": random.randint(0, 2),
            "offset": state.total_logs + 1,
            "security_protocol": "mTLS (SSL:9093)",
            "client_cert": "producer.crt",
            "verified": True
        }
    }

async def process_log_pipeline(log: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates the end-to-end SecureStream pipeline."""
    state.total_logs += 1
    state.response_times.append(log["response_time_ms"])
    
    # 1. Evaluate with incident detector
    detection = detect_incident(log)
    is_incident = detection.get("incident", False)
    reasons = detection.get("reasons", [])
    
    # 2. Store in S3 (Application Logs)
    s3_key = f"logs/{log['service']}/{log['event_id']}.json"
    state.s3_storage[s3_key] = {
        "key": s3_key,
        "bucket": "securestream-app-logs-prod",
        "stored_at": datetime.now(timezone.utc).isoformat(),
        "type": "log",
        "data": log
    }
    state.total_s3_stored += 1

    incident_payload = None
    sns_alert = None

    # 3. If incident, process through SQS -> Lambda -> S3 & SNS
    if is_incident:
        state.total_incidents += 1
        state.total_sqs_sent += 1
        
        incident_payload = {
            **log,
            "incident_reasons": reasons,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "sqs_meta": {
                "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/securestream-incidents",
                "message_id": f"msg-{uuid.uuid4().hex[:12]}",
                "status": "PROCESSED_BY_LAMBDA"
            }
        }
        
        # Store in S3 incidents/
        incident_s3_key = f"incidents/{log['service']}/{log['event_id']}.json"
        state.s3_storage[incident_s3_key] = {
            "key": incident_s3_key,
            "bucket": "securestream-incidents-prod",
            "stored_at": datetime.now(timezone.utc).isoformat(),
            "type": "incident",
            "data": incident_payload
        }

        # Lambda executes and publishes SNS email alert
        state.total_sns_published += 1
        alert_body = f"""SecureStream Critical Incident

Service: {log['service']}
Environment: {log.get('environment', 'production')}
Level: {log.get('level')}
HTTP Status: {log.get('status_code')}
Response Time: {log.get('response_time_ms')} ms
Message: {log.get('message')}
Reasons: {', '.join(reasons)}
Event ID: {log['event_id']}
Timestamp: {log['timestamp']}"""

        sns_alert = {
            "alert_id": f"sns-{uuid.uuid4().hex[:8]}",
            "topic_arn": "arn:aws:sns:us-east-1:123456789012:securestream-critical-alerts",
            "subject": f"SecureStream Alert - {log['service']}",
            "message": alert_body,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "event_id": log["event_id"],
            "service": log["service"],
            "reasons": reasons,
            "recipients": ["ops-team@securestream.local", "oncall@company.com"]
        }
        state.sns_alerts_buffer.appendleft(sns_alert)
        state.incidents_buffer.appendleft(incident_payload)

    # Attach pipeline details to log for visualization
    pipeline_result = {
        "log": log,
        "is_incident": is_incident,
        "reasons": reasons,
        "s3_key": s3_key,
        "incident": incident_payload,
        "sns_alert": sns_alert,
        "pipeline_step": "SNS_DISPATCHED" if is_incident else "S3_STORED"
    }

    state.logs_buffer.appendleft(pipeline_result)

    # Broadcast event to SSE subscribers
    await broadcast_event("pipeline_event", pipeline_result)
    return pipeline_result

async def broadcast_event(event_type: str, data: Any):
    payload = json.dumps({"type": event_type, "data": data})
    disconnected = []
    for queue in state.subscribers:
        try:
            queue.put_nowait(payload)
        except Exception:
            disconnected.append(queue)
    for q in disconnected:
        if q in state.subscribers:
            state.subscribers.remove(q)

# ---------------------------------------------------------------------------
# Background Generator Task
# ---------------------------------------------------------------------------
background_task: Optional[asyncio.Task] = None

async def generator_loop():
    while True:
        try:
            if state.is_running:
                log = generate_sample_log()
                await process_log_pipeline(log)
            await asyncio.sleep(state.interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in generator loop: {e}", file=sys.stderr)
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    global background_task
    # Pre-generate a few initial logs so dashboard looks rich immediately
    for _ in range(8):
        sample = generate_sample_log()
        await process_log_pipeline(sample)
    background_task = asyncio.create_task(generator_loop())

@app.on_event("shutdown")
async def shutdown_event():
    global background_task
    if background_task:
        background_task.cancel()

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/status")
async def get_status():
    avg_latency = round(sum(state.response_times) / max(len(state.response_times), 1), 1)
    incident_pct = round((state.total_incidents / max(state.total_logs, 1)) * 100, 1)
    return {
        "is_running": state.is_running,
        "interval_seconds": state.interval,
        "stats": {
            "total_logs": state.total_logs,
            "total_incidents": state.total_incidents,
            "incident_percentage": incident_pct,
            "avg_latency_ms": avg_latency,
            "total_s3_stored": state.total_s3_stored,
            "total_sqs_sent": state.total_sqs_sent,
            "total_sns_published": state.total_sns_published,
            "connected_subscribers": len(state.subscribers)
        },
        "services": SERVICES,
        "architecture_status": {
            "kafka_mtls": "HEALTHY",
            "ecs_consumer": "RUNNING",
            "s3_bucket": "HEALTHY",
            "sqs_queue": "HEALTHY",
            "lambda_processor": "READY",
            "sns_topic": "ONLINE"
        }
    }

@app.post("/api/stream/toggle")
async def toggle_stream(action: Optional[str] = Query(None)):
    if action == "start":
        state.is_running = True
    elif action == "pause":
        state.is_running = False
    else:
        state.is_running = not state.is_running
    await broadcast_event("state_change", {"is_running": state.is_running})
    return {"is_running": state.is_running, "interval": state.interval}

@app.post("/api/stream/rate")
async def set_stream_rate(interval: float = Query(..., ge=0.2, le=10.0)):
    state.interval = interval
    await broadcast_event("state_change", {"interval": state.interval})
    return {"status": "ok", "interval": state.interval}

class CustomLogRequest(BaseModel):
    service: Optional[str] = None
    status_code: Optional[int] = None
    response_time_ms: Optional[int] = None
    level: Optional[str] = None
    message: Optional[str] = None

@app.post("/api/stream/inject")
async def inject_log(req: CustomLogRequest):
    custom_params = {k: v for k, v in req.dict().items() if v is not None}
    log = generate_sample_log(custom_params if custom_params else None)
    result = await process_log_pipeline(log)
    return {"status": "injected", "event_id": log["event_id"], "result": result}

@app.post("/api/stream/clear")
async def clear_buffer():
    state.logs_buffer.clear()
    state.incidents_buffer.clear()
    state.sns_alerts_buffer.clear()
    state.response_times.clear()
    await broadcast_event("buffer_cleared", {})
    return {"status": "cleared"}

@app.get("/api/logs")
async def get_logs(limit: int = 50, service: Optional[str] = None, level: Optional[str] = None):
    results = list(state.logs_buffer)
    if service:
        results = [r for r in results if r["log"]["service"] == service]
    if level:
        results = [r for r in results if r["log"]["level"] == level]
    return results[:limit]

@app.get("/api/incidents")
async def get_incidents(limit: int = 50):
    return list(state.incidents_buffer)[:limit]

@app.get("/api/sns/alerts")
async def get_sns_alerts(limit: int = 50):
    return list(state.sns_alerts_buffer)[:limit]

@app.get("/api/s3/files")
async def get_s3_files(prefix: Optional[str] = None, limit: int = 40):
    files = list(state.s3_storage.values())
    if prefix:
        files = [f for f in files if f["key"].startswith(prefix)]
    # Sort newest first
    files.sort(key=lambda x: x["stored_at"], reverse=True)
    return files[:limit]

@app.post("/api/tests/run")
async def run_tests():
    """Runs pytest on the test_detector.py file and returns result."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_detector.py", "-v", "--no-header"],
            cwd=str(CURRENT_DIR),
            capture_output=True,
            text=True,
            timeout=15
        )
        passed = proc.returncode == 0
        return {
            "success": passed,
            "return_code": proc.returncode,
            "output": proc.stdout or proc.stderr,
            "executed_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "success": False,
            "return_code": -1,
            "output": f"Execution error: {str(e)}",
            "executed_at": datetime.now(timezone.utc).isoformat()
        }

@app.get("/api/stream/events")
async def sse_events(request: Request):
    """Server-Sent Events endpoint for real-time frontend streaming."""
    queue: asyncio.Queue = asyncio.Queue()
    state.subscribers.append(queue)

    async def event_generator():
        # First send immediate current state
        initial_data = {
            "is_running": state.is_running,
            "interval": state.interval,
            "total_logs": state.total_logs,
            "total_incidents": state.total_incidents
        }
        yield f"event: init\ndata: {json.dumps(initial_data)}\n\n"

        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield f"event: message\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    # Keep-alive heartbeat
                    yield ": ping\n\n"
        finally:
            if queue in state.subscribers:
                state.subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ---------------------------------------------------------------------------
# Embedded Rich Single-Page Dashboard UI
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return HTML_CONTENT

HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SecureStream | Real-Time Log Monitoring & Incident Alerting</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg-base: #090d16;
      --bg-surface: #0f172a;
      --bg-card: #151e34;
      --bg-card-hover: #1b2642;
      --border: rgba(255, 255, 255, 0.08);
      --border-accent: rgba(6, 182, 212, 0.3);
      --text-main: #f1f5f9;
      --text-muted: #94a3b8;
      --text-dim: #64748b;
      --cyan: #06b6d4;
      --cyan-glow: rgba(6, 182, 212, 0.25);
      --purple: #8b5cf6;
      --purple-glow: rgba(139, 92, 246, 0.25);
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.2);
      --red: #ef4444;
      --red-glow: rgba(239, 68, 68, 0.25);
      --amber: #f59e0b;
      --amber-glow: rgba(245, 158, 11, 0.2);
      --radius: 12px;
      --shadow-sm: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
      --shadow-lg: 0 10px 25px -5px rgba(0, 0, 0, 0.5), 0 0 15px rgba(6, 182, 212, 0.08);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      background-color: var(--bg-base);
      color: var(--text-main);
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      line-height: 1.5;
      min-height: 100vh;
      overflow-x: hidden;
    }

    /* Ambient background gradient glow */
    body::before {
      content: "";
      position: fixed;
      top: -200px;
      left: 20%;
      width: 600px;
      height: 600px;
      background: radial-gradient(circle, rgba(6, 182, 212, 0.12) 0%, rgba(139, 92, 246, 0.05) 50%, transparent 70%);
      pointer-events: none;
      z-index: 0;
    }

    .app-container {
      position: relative;
      z-index: 1;
      max-width: 1720px;
      margin: 0 auto;
      padding: 20px 28px 40px;
    }

    /* ---------------- HEADER ---------------- */
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 16px 24px;
      background: rgba(15, 23, 42, 0.75);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow-sm);
      margin-bottom: 22px;
    }

    .brand-section {
      display: flex;
      align-items: center;
      gap: 16px;
    }

    .brand-logo {
      width: 44px;
      height: 44px;
      border-radius: 10px;
      background: linear-gradient(135deg, #06b6d4, #8b5cf6);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 0 20px rgba(6, 182, 212, 0.4);
    }

    .brand-logo svg {
      width: 26px;
      height: 26px;
      fill: #ffffff;
    }

    .brand-text h1 {
      font-size: 1.35rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      background: linear-gradient(90deg, #f8fafc, #93c5fd, #38bdf8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .brand-subtitle {
      font-size: 0.8rem;
      color: var(--text-muted);
      font-weight: 400;
    }

    .header-controls {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .localhost-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: rgba(6, 182, 212, 0.12);
      border: 1px solid rgba(6, 182, 212, 0.35);
      padding: 6px 14px;
      border-radius: 20px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.82rem;
      color: #38bdf8;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    .localhost-badge:hover {
      background: rgba(6, 182, 212, 0.2);
      border-color: #38bdf8;
      transform: translateY(-1px);
    }

    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 10px var(--green);
      animation: pulse 2s infinite;
    }

    @keyframes pulse {
      0% { transform: scale(0.95); opacity: 0.8; }
      50% { transform: scale(1.3); opacity: 1; }
      100% { transform: scale(0.95); opacity: 0.8; }
    }

    /* ---------------- ARCHITECTURE STATUS BAR ---------------- */
    .arch-bar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      background: rgba(21, 30, 52, 0.6);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 12px 20px;
      margin-bottom: 22px;
      font-size: 0.82rem;
    }

    .arch-chip {
      display: flex;
      align-items: center;
      gap: 8px;
      background: rgba(15, 23, 42, 0.7);
      padding: 6px 12px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.05);
      color: var(--text-muted);
    }

    .arch-chip strong {
      color: var(--text-main);
      font-weight: 600;
    }

    .chip-led {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 8px var(--green-glow);
    }

    .chip-led.cyan {
      background: var(--cyan);
      box-shadow: 0 0 8px var(--cyan-glow);
    }

    /* ---------------- METRICS ROW ---------------- */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 16px;
      margin-bottom: 22px;
    }

    .metric-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px 20px;
      position: relative;
      overflow: hidden;
      transition: all 0.25s ease;
    }

    .metric-card:hover {
      border-color: rgba(255, 255, 255, 0.16);
      transform: translateY(-2px);
      box-shadow: var(--shadow-sm);
    }

    .metric-card::before {
      content: "";
      position: absolute;
      top: 0;
      left: 0;
      width: 4px;
      height: 100%;
      background: var(--cyan);
    }

    .metric-card.alert-card::before {
      background: var(--red);
    }
    .metric-card.warning-card::before {
      background: var(--amber);
    }
    .metric-card.purple-card::before {
      background: var(--purple);
    }
    .metric-card.green-card::before {
      background: var(--green);
    }

    .metric-label {
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      font-weight: 600;
      margin-bottom: 6px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .metric-value {
      font-size: 1.85rem;
      font-weight: 800;
      color: var(--text-main);
      font-family: 'JetBrains Mono', monospace;
      letter-spacing: -0.03em;
    }

    .metric-sub {
      font-size: 0.75rem;
      color: var(--text-dim);
      margin-top: 4px;
    }

    /* ---------------- INTERACTIVE VISUAL PIPELINE ---------------- */
    .pipeline-section {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px 22px;
      margin-bottom: 22px;
    }

    .pipeline-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
    }

    .pipeline-title {
      font-size: 0.9rem;
      font-weight: 700;
      letter-spacing: -0.01em;
      color: var(--text-main);
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .pipeline-diagram {
      display: flex;
      align-items: center;
      justify-content: space-between;
      overflow-x: auto;
      padding: 10px 0;
      gap: 12px;
    }

    .pipe-node {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 14px;
      text-align: center;
      min-width: 140px;
      position: relative;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .pipe-node.active-pulse {
      border-color: var(--cyan);
      box-shadow: 0 0 16px var(--cyan-glow);
      transform: translateY(-2px);
    }

    .pipe-node.incident-pulse {
      border-color: var(--red);
      box-shadow: 0 0 20px var(--red-glow);
      transform: translateY(-2px);
    }

    .pipe-node-title {
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--text-main);
    }

    .pipe-node-sub {
      font-size: 0.7rem;
      color: var(--cyan);
      font-family: 'JetBrains Mono', monospace;
      margin-top: 2px;
    }

    .pipe-node-badge {
      font-size: 0.65rem;
      background: rgba(255, 255, 255, 0.06);
      color: var(--text-muted);
      border-radius: 4px;
      padding: 2px 6px;
      margin-top: 4px;
      display: inline-block;
    }

    .pipe-connector {
      flex: 1;
      height: 2px;
      background: rgba(255, 255, 255, 0.12);
      position: relative;
      min-width: 32px;
    }

    .pipe-connector::after {
      content: "";
      position: absolute;
      top: -3px;
      right: 0;
      width: 8px;
      height: 8px;
      border-top: 2px solid rgba(255, 255, 255, 0.3);
      border-right: 2px solid rgba(255, 255, 255, 0.3);
      transform: rotate(45deg);
    }

    .pipe-connector.pulse-flow::before {
      content: "";
      position: absolute;
      top: -3px;
      left: 0;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--cyan);
      box-shadow: 0 0 8px var(--cyan);
      animation: flowRight 1s infinite linear;
    }

    @keyframes flowRight {
      0% { left: 0%; opacity: 0; }
      20% { opacity: 1; }
      80% { opacity: 1; }
      100% { left: 95%; opacity: 0; }
    }

    /* ---------------- CONTROL TOOLBAR ---------------- */
    .controls-deck {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 14px 20px;
      margin-bottom: 22px;
    }

    .control-group {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 0.82rem;
      font-weight: 600;
      padding: 8px 16px;
      border-radius: 8px;
      border: 1px solid transparent;
      cursor: pointer;
      transition: all 0.2s ease;
      font-family: inherit;
    }

    .btn:active {
      transform: scale(0.97);
    }

    .btn-primary {
      background: linear-gradient(135deg, #0284c7, #06b6d4);
      color: #ffffff;
      box-shadow: 0 2px 10px rgba(6, 182, 212, 0.3);
    }

    .btn-primary:hover {
      background: linear-gradient(135deg, #0369a1, #0891b2);
      box-shadow: 0 4px 14px rgba(6, 182, 212, 0.45);
    }

    .btn-pause {
      background: rgba(245, 158, 11, 0.15);
      border-color: rgba(245, 158, 11, 0.4);
      color: #fbbf24;
    }

    .btn-pause:hover {
      background: rgba(245, 158, 11, 0.25);
    }

    .btn-outline {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border);
      color: var(--text-muted);
    }

    .btn-outline:hover {
      background: rgba(255, 255, 255, 0.09);
      color: var(--text-main);
      border-color: rgba(255, 255, 255, 0.2);
    }

    .btn-danger-outline {
      background: rgba(239, 68, 68, 0.12);
      border: 1px solid rgba(239, 68, 68, 0.35);
      color: #f87171;
    }

    .btn-danger-outline:hover {
      background: rgba(239, 68, 68, 0.22);
      border-color: #ef4444;
    }

    .btn-warning-outline {
      background: rgba(245, 158, 11, 0.12);
      border: 1px solid rgba(245, 158, 11, 0.35);
      color: #fbbf24;
    }

    .btn-warning-outline:hover {
      background: rgba(245, 158, 11, 0.22);
    }

    .rate-selector {
      display: flex;
      align-items: center;
      gap: 6px;
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 4px;
    }

    .rate-btn {
      background: transparent;
      border: none;
      color: var(--text-dim);
      font-size: 0.75rem;
      font-weight: 600;
      padding: 4px 10px;
      border-radius: 6px;
      cursor: pointer;
      transition: all 0.15s ease;
    }

    .rate-btn.active {
      background: var(--cyan);
      color: #0b0f19;
      font-weight: 700;
    }

    /* ---------------- MAIN SPLIT VIEW ---------------- */
    .main-grid {
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 20px;
    }

    @media (max-width: 1200px) {
      .main-grid {
        grid-template-columns: 1fr;
      }
    }

    .panel {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      display: flex;
      flex-direction: column;
      height: 720px;
      overflow: hidden;
      box-shadow: var(--shadow-sm);
    }

    .panel-header {
      padding: 16px 20px;
      background: rgba(21, 30, 52, 0.7);
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-shrink: 0;
    }

    .panel-title {
      font-size: 0.92rem;
      font-weight: 700;
      color: var(--text-main);
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .panel-filters {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    select.filter-select {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 6px;
      color: var(--text-main);
      font-size: 0.75rem;
      padding: 4px 10px;
      outline: none;
      font-family: inherit;
    }

    /* Tabs on Right Panel */
    .tab-nav {
      display: flex;
      gap: 4px;
      border-bottom: 1px solid var(--border);
      background: rgba(15, 23, 42, 0.9);
      padding: 0 16px;
      flex-shrink: 0;
    }

    .tab-btn {
      background: transparent;
      border: none;
      color: var(--text-muted);
      font-size: 0.8rem;
      font-weight: 600;
      padding: 12px 14px;
      cursor: pointer;
      position: relative;
      transition: all 0.2s ease;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .tab-btn:hover {
      color: var(--text-main);
    }

    .tab-btn.active {
      color: #38bdf8;
    }

    .tab-btn.active::after {
      content: "";
      position: absolute;
      bottom: 0;
      left: 0;
      width: 100%;
      height: 2px;
      background: #38bdf8;
      box-shadow: 0 -2px 8px rgba(56, 189, 248, 0.5);
    }

    .tab-badge {
      background: rgba(239, 68, 68, 0.2);
      color: #f87171;
      border-radius: 10px;
      font-size: 0.68rem;
      padding: 2px 6px;
      font-weight: 700;
    }

    .panel-content {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
    }

    /* Custom Scrollbar */
    .panel-content::-webkit-scrollbar {
      width: 6px;
    }
    .panel-content::-webkit-scrollbar-track {
      background: rgba(0, 0, 0, 0.1);
    }
    .panel-content::-webkit-scrollbar-thumb {
      background: rgba(255, 255, 255, 0.15);
      border-radius: 3px;
    }

    /* ---------------- LOG CARDS ---------------- */
    .log-item {
      background: var(--bg-card);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 8px;
      padding: 12px 16px;
      margin-bottom: 10px;
      transition: all 0.2s ease;
      cursor: pointer;
      display: flex;
      flex-direction: column;
      gap: 6px;
      position: relative;
    }

    .log-item:hover {
      background: var(--bg-card-hover);
      border-color: rgba(6, 182, 212, 0.3);
      transform: translateX(2px);
    }

    .log-item.is-incident {
      border-left: 3px solid var(--red);
      background: rgba(239, 68, 68, 0.04);
    }

    .log-top-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }

    .log-badge-group {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .badge-status {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.72rem;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 4px;
    }

    .status-200 { background: rgba(16, 185, 129, 0.2); color: #34d399; }
    .status-201 { background: rgba(16, 185, 129, 0.2); color: #34d399; }
    .status-500 { background: rgba(239, 68, 68, 0.25); color: #f87171; }
    .status-503 { background: rgba(239, 68, 68, 0.25); color: #f87171; }

    .badge-service {
      font-size: 0.75rem;
      font-weight: 600;
      color: #93c5fd;
    }

    .badge-level {
      font-size: 0.65rem;
      font-weight: 700;
      padding: 2px 6px;
      border-radius: 4px;
      text-transform: uppercase;
    }

    .level-info { background: rgba(6, 182, 212, 0.15); color: #38bdf8; }
    .level-error { background: rgba(239, 68, 68, 0.2); color: #f87171; }
    .level-critical { background: rgba(139, 92, 246, 0.25); color: #c084fc; }

    .badge-latency {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.72rem;
      color: var(--text-dim);
    }

    .badge-latency.slow {
      color: #fbbf24;
      font-weight: 700;
    }

    .log-message {
      font-size: 0.8rem;
      color: var(--text-muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .log-meta-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 0.7rem;
      color: var(--text-dim);
      font-family: 'JetBrains Mono', monospace;
    }

    .incident-tag-list {
      display: flex;
      gap: 6px;
      margin-top: 4px;
      flex-wrap: wrap;
    }

    .tag-reason {
      font-size: 0.68rem;
      font-weight: 600;
      background: rgba(239, 68, 68, 0.2);
      border: 1px solid rgba(239, 68, 68, 0.35);
      color: #fca5a5;
      padding: 2px 8px;
      border-radius: 4px;
    }

    /* ---------------- INCIDENT CARDS ---------------- */
    .incident-item {
      background: rgba(239, 68, 68, 0.07);
      border: 1px solid rgba(239, 68, 68, 0.3);
      border-radius: 8px;
      padding: 14px 18px;
      margin-bottom: 12px;
      position: relative;
    }

    .incident-item-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
    }

    .incident-item-service {
      font-size: 0.88rem;
      font-weight: 700;
      color: #f87171;
    }

    .incident-item-body {
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-bottom: 8px;
    }

    .incident-dispatch-chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      font-size: 0.7rem;
    }

    .dispatch-chip {
      background: rgba(15, 23, 42, 0.7);
      border: 1px solid rgba(255, 255, 255, 0.08);
      padding: 2px 8px;
      border-radius: 4px;
      color: var(--text-muted);
    }

    /* ---------------- SNS EMAIL CARDS ---------------- */
    .sns-card {
      background: var(--bg-card);
      border: 1px solid rgba(139, 92, 246, 0.3);
      border-radius: 8px;
      padding: 16px;
      margin-bottom: 12px;
      position: relative;
    }

    .sns-card-subject {
      font-size: 0.85rem;
      font-weight: 700;
      color: #c084fc;
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .sns-card-recipients {
      font-size: 0.72rem;
      color: var(--text-dim);
      margin-bottom: 10px;
    }

    .sns-email-box {
      background: rgba(10, 14, 26, 0.7);
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 6px;
      padding: 12px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.75rem;
      color: #e2e8f0;
      white-space: pre-wrap;
      line-height: 1.4;
    }

    /* ---------------- S3 BUCKET EXPLORER ---------------- */
    .s3-tree-view {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.78rem;
    }

    .s3-file-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px 12px;
      border-radius: 6px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      cursor: pointer;
      transition: background 0.15s ease;
    }

    .s3-file-row:hover {
      background: var(--bg-card-hover);
      color: #38bdf8;
    }

    .s3-key {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    /* ---------------- PYTEST RUNNER ---------------- */
    .test-box {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px;
    }

    .test-output-console {
      background: #060912;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 6px;
      padding: 14px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.76rem;
      color: #a7f3d0;
      white-space: pre-wrap;
      max-height: 380px;
      overflow-y: auto;
      margin-top: 14px;
    }

    /* ---------------- MODAL / DETAIL DRAWER ---------------- */
    .modal-overlay {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.7);
      backdrop-filter: blur(8px);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }

    .modal-overlay.open {
      display: flex;
    }

    .modal-card {
      background: #111827;
      border: 1px solid var(--border);
      border-radius: 14px;
      width: 100%;
      max-width: 680px;
      max-height: 85vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.7);
    }

    .modal-header {
      padding: 16px 22px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .modal-title {
      font-size: 1rem;
      font-weight: 700;
      color: var(--text-main);
    }

    .modal-close {
      background: transparent;
      border: none;
      color: var(--text-muted);
      font-size: 1.2rem;
      cursor: pointer;
    }

    .modal-body {
      padding: 20px;
      overflow-y: auto;
      font-size: 0.85rem;
    }

    pre.json-viewer {
      background: #090d16;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 8px;
      padding: 14px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.76rem;
      color: #7dd3fc;
      overflow-x: auto;
    }

    .form-group {
      margin-bottom: 14px;
    }

    .form-label {
      display: block;
      font-size: 0.78rem;
      font-weight: 600;
      color: var(--text-muted);
      margin-bottom: 6px;
    }

    .form-input, .form-select {
      width: 100%;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 8px 12px;
      color: var(--text-main);
      font-family: inherit;
      font-size: 0.82rem;
    }

    /* Toast notification */
    .toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: #1e293b;
      border: 1px solid var(--cyan);
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.5);
      border-radius: 8px;
      padding: 12px 20px;
      color: #ffffff;
      font-size: 0.84rem;
      display: none;
      z-index: 2000;
      animation: fadeIn 0.3s ease;
    }

    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
  </style>
</head>
<body>

<div class="app-container">

  <!-- HEADER -->
  <header>
    <div class="brand-section">
      <div class="brand-logo">
        <svg viewBox="0 0 24 24">
          <path d="M12 2L4 5v6.09c0 5.05 3.41 9.76 8 10.91 4.59-1.15 8-5.86 8-10.91V5l-8-3zm1 14h-2v-2h2v2zm0-4h-2V7h2v5z"/>
        </svg>
      </div>
      <div class="brand-text">
        <h1>SecureStream <span>Console</span></h1>
        <div class="brand-subtitle">Real-Time Application Log Monitoring & Incident Alerting Platform</div>
      </div>
    </div>

    <div class="header-controls">
      <div class="localhost-badge" id="hostBadge" title="Click to copy localhost URL" onclick="copyHostUrl()">
        <span class="pulse-dot" id="liveDot"></span>
        <span id="hostLabel">http://localhost:8000</span>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
        </svg>
      </div>
    </div>
  </header>

  <!-- ARCHITECTURE STATUS BAR -->
  <div class="arch-bar">
    <div class="arch-chip">
      <span class="chip-led cyan"></span>
      <span>Kafka mTLS: <strong>Port 9093 (Active)</strong></span>
    </div>
    <div class="arch-chip">
      <span class="chip-led"></span>
      <span>ECS Fargate Consumer: <strong>Port 19093 (Private VPC)</strong></span>
    </div>
    <div class="arch-chip">
      <span class="chip-led"></span>
      <span>Amazon S3: <strong>Bucket Connected</strong></span>
    </div>
    <div class="arch-chip">
      <span class="chip-led"></span>
      <span>Amazon SQS: <strong>Incident Queue Active</strong></span>
    </div>
    <div class="arch-chip">
      <span class="chip-led"></span>
      <span>AWS Lambda: <strong>Processor Ready</strong></span>
    </div>
    <div class="arch-chip">
      <span class="chip-led"></span>
      <span>Amazon SNS: <strong>Alerts Enabled</strong></span>
    </div>
  </div>

  <!-- KPI METRICS ROW -->
  <div class="metrics-grid">
    <div class="metric-card">
      <div class="metric-label">Ingested Logs (Kafka)</div>
      <div class="metric-value" id="kpiTotalLogs">0</div>
      <div class="metric-sub">Topic: application.logs</div>
    </div>

    <div class="metric-card alert-card">
      <div class="metric-label">Detected Incidents</div>
      <div class="metric-value" id="kpiTotalIncidents" style="color: #f87171;">0</div>
      <div class="metric-sub">Dispatched to SQS & SNS</div>
    </div>

    <div class="metric-card warning-card">
      <div class="metric-label">Incident Rate</div>
      <div class="metric-value" id="kpiIncidentRate">0.0%</div>
      <div class="metric-sub">Failures / Slow APIs</div>
    </div>

    <div class="metric-card green-card">
      <div class="metric-label">Avg Response Time</div>
      <div class="metric-value" id="kpiAvgLatency">0 ms</div>
      <div class="metric-sub">Threshold: 3,000 ms</div>
    </div>

    <div class="metric-card purple-card">
      <div class="metric-label">S3 Log Objects</div>
      <div class="metric-value" id="kpiTotalS3">0</div>
      <div class="metric-sub">Stored JSON records</div>
    </div>

    <div class="metric-card">
      <div class="metric-label">SNS Email Alerts</div>
      <div class="metric-value" id="kpiTotalSns">0</div>
      <div class="metric-sub">On-call notifications</div>
    </div>
  </div>

  <!-- INTERACTIVE ARCHITECTURE PIPELINE VISUALIZER -->
  <div class="pipeline-section">
    <div class="pipeline-header">
      <div class="pipeline-title">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#06b6d4" stroke-width="2">
          <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
        </svg>
        Live End-to-End Pipeline Visualization
      </div>
      <div style="font-size: 0.75rem; color: var(--text-dim);">Real-time message packet flow</div>
    </div>

    <div class="pipeline-diagram">
      <div class="pipe-node" id="nodeProducer">
        <div class="pipe-node-title">Python Producer</div>
        <div class="pipe-node-sub">producer.py</div>
        <div class="pipe-node-badge">Local Host</div>
      </div>

      <div class="pipe-connector pulse-flow" id="connProducerKafka"></div>

      <div class="pipe-node" id="nodeKafka">
        <div class="pipe-node-title">Apache Kafka</div>
        <div class="pipe-node-sub">mTLS :9093</div>
        <div class="pipe-node-badge">KRaft Mode</div>
      </div>

      <div class="pipe-connector pulse-flow" id="connKafkaConsumer"></div>

      <div class="pipe-node" id="nodeConsumer">
        <div class="pipe-node-title">Python Consumer</div>
        <div class="pipe-node-sub">detector.py</div>
        <div class="pipe-node-badge">AWS ECS/Fargate</div>
      </div>

      <div class="pipe-connector pulse-flow" id="connConsumerS3"></div>

      <div class="pipe-node" id="nodeS3">
        <div class="pipe-node-title">Amazon S3</div>
        <div class="pipe-node-sub">logs/ & incidents/</div>
        <div class="pipe-node-badge">Historical Storage</div>
      </div>

      <div class="pipe-connector" id="connConsumerSqs"></div>

      <div class="pipe-node" id="nodeSqs">
        <div class="pipe-node-title">Amazon SQS</div>
        <div class="pipe-node-sub">Incidents Queue</div>
        <div class="pipe-node-badge">Async Decoupling</div>
      </div>

      <div class="pipe-connector" id="connSqsLambda"></div>

      <div class="pipe-node" id="nodeLambda">
        <div class="pipe-node-title">AWS Lambda</div>
        <div class="pipe-node-sub">lambda_function.py</div>
        <div class="pipe-node-badge">Serverless Processor</div>
      </div>

      <div class="pipe-connector" id="connLambdaSns"></div>

      <div class="pipe-node" id="nodeSns">
        <div class="pipe-node-title">Amazon SNS</div>
        <div class="pipe-node-sub">Critical Alerts</div>
        <div class="pipe-node-badge">Email Notification</div>
      </div>
    </div>
  </div>

  <!-- CONTROL TOOLBAR -->
  <div class="controls-deck">
    <div class="control-group">
      <button class="btn btn-primary" id="btnToggleStream" onclick="toggleStream()">
        <span id="btnToggleIcon">⏸</span>
        <span id="btnToggleText">Pause Streaming</span>
      </button>

      <div class="rate-selector">
        <span style="font-size: 0.72rem; color: var(--text-dim); padding-left: 6px;">Rate:</span>
        <button class="rate-btn" onclick="setRate(0.5, this)">0.5s</button>
        <button class="rate-btn" onclick="setRate(1.0, this)">1.0s</button>
        <button class="rate-btn active" onclick="setRate(1.5, this)">1.5s</button>
        <button class="rate-btn" onclick="setRate(3.0, this)">3.0s</button>
      </div>
    </div>

    <div class="control-group">
      <button class="btn btn-danger-outline" onclick="injectIncident('500')">
        ⚡ Inject HTTP 500 Error
      </button>
      <button class="btn btn-warning-outline" onclick="injectIncident('slow')">
        ⏱️ Inject Slow API (4500ms)
      </button>
      <button class="btn btn-outline" onclick="injectIncident('critical')">
        🚨 Inject Critical Alert
      </button>
      <button class="btn btn-outline" onclick="openCustomLogModal()">
        🛠️ Custom Log...
      </button>
      <button class="btn btn-outline" onclick="openTestModal()">
        🧪 Run Pytest
      </button>
      <button class="btn btn-outline" onclick="clearLogs()" title="Clear local buffer">
        🗑️ Clear
      </button>
    </div>
  </div>

  <!-- MAIN SPLIT WORKSPACE -->
  <div class="main-grid">

    <!-- LEFT PANEL: KAFKA APPLICATION LOGS STREAM -->
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2">
            <polyline points="4 17 10 11 4 5"></polyline>
            <line x1="12" y1="19" x2="20" y2="19"></line>
          </svg>
          Kafka Live Log Stream
          <span style="font-size: 0.7rem; color: var(--text-dim); font-family: 'JetBrains Mono', monospace;">[topic: application.logs]</span>
        </div>

        <div class="panel-filters">
          <select class="filter-select" id="filterService" onchange="renderLogs()">
            <option value="">All Services</option>
            <option value="checkout-api">checkout-api</option>
            <option value="payment-api">payment-api</option>
            <option value="authentication-service">authentication-service</option>
            <option value="inventory-api">inventory-api</option>
          </select>

          <select class="filter-select" id="filterLevel" onchange="renderLogs()">
            <option value="">All Levels</option>
            <option value="INFO">INFO</option>
            <option value="ERROR">ERROR</option>
            <option value="CRITICAL">CRITICAL</option>
          </select>
        </div>
      </div>

      <div class="panel-content" id="logsFeed">
        <!-- Log rows rendered dynamically -->
      </div>
    </div>

    <!-- RIGHT PANEL: TABBED INTELLIGENCE (INCIDENTS, SNS, S3, CHARTS) -->
    <div class="panel">
      <div class="tab-nav">
        <button class="tab-btn active" id="tabIncidents" onclick="switchTab('incidents')">
          🚨 Incidents
          <span class="tab-badge" id="incidentCountBadge">0</span>
        </button>
        <button class="tab-btn" id="tabSns" onclick="switchTab('sns')">
          📧 SNS Alerts
        </button>
        <button class="tab-btn" id="tabS3" onclick="switchTab('s3')">
          🪣 S3 Explorer
        </button>
        <button class="tab-btn" id="tabAnalytics" onclick="switchTab('analytics')">
          📊 Charts
        </button>
        <button class="tab-btn" id="tabTests" onclick="switchTab('tests')">
          🧪 Unit Tests
        </button>
      </div>

      <!-- TAB 1: INCIDENTS -->
      <div class="panel-content" id="tabContentIncidents">
        <div id="incidentsFeed">
          <!-- Incident rows rendered dynamically -->
        </div>
      </div>

      <!-- TAB 2: SNS ALERTS -->
      <div class="panel-content" id="tabContentSns" style="display: none;">
        <div id="snsAlertsFeed">
          <!-- SNS emails rendered dynamically -->
        </div>
      </div>

      <!-- TAB 3: S3 EXPLORER -->
      <div class="panel-content" id="tabContentS3" style="display: none;">
        <div style="font-size: 0.8rem; color: var(--text-muted); margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center;">
          <span>S3 Bucket: <strong>securestream-app-logs-prod</strong></span>
          <button class="btn btn-outline" style="padding: 4px 10px; font-size: 0.72rem;" onclick="refreshS3Files()">🔄 Refresh</button>
        </div>
        <div class="s3-tree-view" id="s3FilesList">
          <!-- S3 files rendered dynamically -->
        </div>
      </div>

      <!-- TAB 4: REAL-TIME CHARTS -->
      <div class="panel-content" id="tabContentAnalytics" style="display: none;">
        <div style="margin-bottom: 20px;">
          <div style="font-size: 0.85rem; font-weight: 700; margin-bottom: 10px;">Response Time Trend (ms)</div>
          <div style="height: 220px; background: rgba(15, 23, 42, 0.5); padding: 10px; border-radius: 8px;">
            <canvas id="latencyChart"></canvas>
          </div>
        </div>

        <div>
          <div style="font-size: 0.85rem; font-weight: 700; margin-bottom: 10px;">Traffic Breakdown by Service</div>
          <div style="height: 200px; display: flex; justify-content: center; background: rgba(15, 23, 42, 0.5); padding: 10px; border-radius: 8px;">
            <canvas id="serviceChart"></canvas>
          </div>
        </div>
      </div>

      <!-- TAB 5: PYTEST SUITE -->
      <div class="panel-content" id="tabContentTests" style="display: none;">
        <div class="test-box">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            <div>
              <div style="font-weight: 700; font-size: 0.95rem;">Detector Test Suite</div>
              <div style="font-size: 0.75rem; color: var(--text-muted);">Validates HTTP 500, Slow Response & CRITICAL alerts</div>
            </div>
            <button class="btn btn-primary" id="btnRunTests" onclick="executePytest()">
              ▶ Run Pytest Now
            </button>
          </div>

          <div style="font-size: 0.78rem; color: var(--text-muted); line-height: 1.6; margin-bottom: 10px;">
            File: <code>tests/test_detector.py</code>
            <ul style="padding-left: 20px; margin-top: 6px;">
              <li><code>test_normal_request</code>: HTTP 200 / 500ms -> incident is False</li>
              <li><code>test_http_server_error</code>: HTTP 500 -> "HTTP server error"</li>
              <li><code>test_slow_response</code>: 4500ms -> "Slow API response"</li>
              <li><code>test_multiple_reasons</code>: HTTP 503 + 5000ms + CRITICAL -> 3 reasons</li>
            </ul>
          </div>

          <div class="test-output-console" id="testConsole">
Click "Run Pytest Now" to execute the test suite against the local Python environment.
          </div>
        </div>
      </div>

    </div>

  </div>

</div>

<!-- DETAILS MODAL -->
<div class="modal-overlay" id="detailModal">
  <div class="modal-card">
    <div class="modal-header">
      <div class="modal-title" id="modalTitle">Log Event Inspector</div>
      <button class="modal-close" onclick="closeModal('detailModal')">&times;</button>
    </div>
    <div class="modal-body">
      <div style="display: flex; gap: 8px; margin-bottom: 14px;" id="modalBadges"></div>
      <pre class="json-viewer" id="modalJsonContent"></pre>
    </div>
  </div>
</div>

<!-- CUSTOM LOG INJECTION MODAL -->
<div class="modal-overlay" id="customLogModal">
  <div class="modal-card">
    <div class="modal-header">
      <div class="modal-title">Manual Log Event Generator</div>
      <button class="modal-close" onclick="closeModal('customLogModal')">&times;</button>
    </div>
    <div class="modal-body">
      <div class="form-group">
        <label class="form-label">Service Name</label>
        <select class="form-select" id="customService">
          <option value="checkout-api">checkout-api</option>
          <option value="payment-api">payment-api</option>
          <option value="authentication-service">authentication-service</option>
          <option value="inventory-api">inventory-api</option>
        </select>
      </div>

      <div class="form-group">
        <label class="form-label">HTTP Status Code</label>
        <input type="number" class="form-input" id="customStatus" value="200" placeholder="200, 500, 503, 404">
      </div>

      <div class="form-group">
        <label class="form-label">Response Time (ms)</label>
        <input type="number" class="form-input" id="customLatency" value="250" placeholder="e.g. 250 or 4200">
      </div>

      <div class="form-group">
        <label class="form-label">Log Level</label>
        <select class="form-select" id="customLevel">
          <option value="INFO">INFO</option>
          <option value="ERROR">ERROR</option>
          <option value="CRITICAL">CRITICAL</option>
          <option value="WARNING">WARNING</option>
        </select>
      </div>

      <div class="form-group">
        <label class="form-label">Custom Message</label>
        <input type="text" class="form-input" id="customMessage" value="Manual test event" placeholder="Enter log message">
      </div>

      <button class="btn btn-primary" style="width: 100%; margin-top: 10px;" onclick="submitCustomLog()">
        🚀 Publish to Kafka Stream
      </button>
    </div>
  </div>
</div>

<!-- TOAST -->
<div class="toast" id="toast"></div>

<script>
  // Global State in Browser
  let logsStore = [];
  let incidentsStore = [];
  let snsAlertsStore = [];
  let isStreamRunning = true;
  let latencyChartInstance = null;
  let serviceChartInstance = null;
  let serviceCounts = {
    "checkout-api": 0,
    "payment-api": 0,
    "authentication-service": 0,
    "inventory-api": 0
  };

  // Connect SSE Stream
  function connectSSE() {
    const evtSource = new EventSource("/api/stream/events");

    evtSource.addEventListener("init", (e) => {
      const data = JSON.parse(e.data);
      updateStreamState(data.is_running);
    });

    evtSource.addEventListener("message", (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === "pipeline_event") {
          handlePipelineEvent(msg.data);
        } else if (msg.type === "state_change") {
          if (msg.data.is_running !== undefined) {
            updateStreamState(msg.data.is_running);
          }
        } else if (msg.type === "buffer_cleared") {
          logsStore = [];
          incidentsStore = [];
          snsAlertsStore = [];
          renderLogs();
          renderIncidents();
          renderSnsAlerts();
          updateKPIs();
        }
      } catch (err) {
        console.error("SSE parse error", err);
      }
    });

    evtSource.onerror = (err) => {
      console.warn("SSE connection interrupted, retrying in 3s...", err);
      evtSource.close();
      setTimeout(connectSSE, 3000);
    };
  }

  function handlePipelineEvent(item) {
    logsStore.unshift(item);
    if (logsStore.length > 200) logsStore.pop();

    if (item.is_incident && item.incident) {
      incidentsStore.unshift(item.incident);
      if (incidentsStore.length > 80) incidentsStore.pop();
    }

    if (item.sns_alert) {
      snsAlertsStore.unshift(item.sns_alert);
      if (snsAlertsStore.length > 50) snsAlertsStore.pop();
    }

    // Update service count
    const svc = item.log.service;
    if (serviceCounts[svc] !== undefined) {
      serviceCounts[svc]++;
    }

    // Trigger visual pulse in architecture diagram
    triggerArchitecturePulse(item.is_incident);

    // Render updates
    renderLogs();
    if (item.is_incident) {
      renderIncidents();
      renderSnsAlerts();
    }
    updateKPIs();
    updateCharts(item.log);
  }

  function triggerArchitecturePulse(isIncident) {
    const pNode = document.getElementById("nodeProducer");
    const kNode = document.getElementById("nodeKafka");
    const cNode = document.getElementById("nodeConsumer");
    const s3Node = document.getElementById("nodeS3");
    const sqsNode = document.getElementById("nodeSqs");
    const lNode = document.getElementById("nodeLambda");
    const snsNode = document.getElementById("nodeSns");

    const pulseClass = isIncident ? "incident-pulse" : "active-pulse";

    pNode.classList.add(pulseClass);
    setTimeout(() => {
      pNode.classList.remove(pulseClass);
      kNode.classList.add(pulseClass);
    }, 200);

    setTimeout(() => {
      kNode.classList.remove(pulseClass);
      cNode.classList.add(pulseClass);
    }, 450);

    setTimeout(() => {
      cNode.classList.remove(pulseClass);
      s3Node.classList.add(pulseClass);
      if (isIncident) {
        sqsNode.classList.add("incident-pulse");
      }
    }, 700);

    if (isIncident) {
      setTimeout(() => {
        sqsNode.classList.remove("incident-pulse");
        lNode.classList.add("incident-pulse");
      }, 950);

      setTimeout(() => {
        lNode.classList.remove("incident-pulse");
        snsNode.classList.add("incident-pulse");
      }, 1200);

      setTimeout(() => {
        snsNode.classList.remove("incident-pulse");
      }, 1600);
    } else {
      setTimeout(() => {
        s3Node.classList.remove(pulseClass);
      }, 1000);
    }
  }

  function renderLogs() {
    const feed = document.getElementById("logsFeed");
    const sFilter = document.getElementById("filterService").value;
    const lFilter = document.getElementById("filterLevel").value;

    let filtered = logsStore;
    if (sFilter) filtered = filtered.filter(x => x.log.service === sFilter);
    if (lFilter) filtered = filtered.filter(x => x.log.level === lFilter);

    if (filtered.length === 0) {
      feed.innerHTML = '<div style="text-align:center; padding: 40px; color: var(--text-dim); font-size: 0.85rem;">Waiting for incoming logs...</div>';
      return;
    }

    feed.innerHTML = filtered.slice(0, 50).map(item => {
      const log = item.log;
      const isInc = item.is_incident;
      const statusClass = `status-${log.status_code}`;
      const levelClass = `level-${log.level.toLowerCase()}`;
      const isSlow = log.response_time_ms > 3000;

      let reasonTags = "";
      if (isInc && item.reasons) {
        reasonTags = `<div class="incident-tag-list">` +
          item.reasons.map(r => `<span class="tag-reason">⚠️ ${r}</span>`).join("") +
          `</div>`;
      }

      return `
        <div class="log-item ${isInc ? 'is-incident' : ''}" onclick="showLogDetail('${log.event_id}')">
          <div class="log-top-row">
            <div class="log-badge-group">
              <span class="badge-status ${statusClass}">HTTP ${log.status_code}</span>
              <span class="badge-service">${log.service}</span>
              <span class="badge-level ${levelClass}">${log.level}</span>
            </div>
            <div class="badge-latency ${isSlow ? 'slow' : ''}">
              ${isSlow ? '⚡ ' : ''}${log.response_time_ms} ms
            </div>
          </div>
          <div class="log-message">${escapeHtml(log.message)}</div>
          ${reasonTags}
          <div class="log-meta-row">
            <span>id: ${log.event_id.substring(0, 8)}...</span>
            <span>${formatTime(log.timestamp)}</span>
          </div>
        </div>
      `;
    }).join("");
  }

  function renderIncidents() {
    const feed = document.getElementById("incidentsFeed");
    const countBadge = document.getElementById("incidentCountBadge");
    countBadge.innerText = incidentsStore.length;

    if (incidentsStore.length === 0) {
      feed.innerHTML = '<div style="text-align:center; padding: 40px; color: var(--text-dim); font-size: 0.85rem;">No incidents detected yet. All services normal.</div>';
      return;
    }

    feed.innerHTML = incidentsStore.map(inc => {
      const reasonsHtml = inc.incident_reasons ? inc.incident_reasons.map(r =>
        `<span class="tag-reason">🚨 ${r}</span>`
      ).join(" ") : "";

      return `
        <div class="incident-item" onclick="showLogDetail('${inc.event_id}')">
          <div class="incident-item-header">
            <span class="incident-item-service">${inc.service}</span>
            <span style="font-family:'JetBrains Mono'; font-size:0.75rem; color:#f87171; font-weight:700;">
              HTTP ${inc.status_code} (${inc.response_time_ms} ms)
            </span>
          </div>
          <div class="incident-item-body">${escapeHtml(inc.message)}</div>
          <div style="margin-bottom: 8px;">${reasonsHtml}</div>
          <div class="incident-dispatch-chips">
            <span class="dispatch-chip">SQS: Queued</span>
            <span class="dispatch-chip">Lambda: Stored S3</span>
            <span class="dispatch-chip">SNS: Dispatched</span>
            <span class="dispatch-chip" style="margin-left:auto;">${formatTime(inc.timestamp)}</span>
          </div>
        </div>
      `;
    }).join("");
  }

  function renderSnsAlerts() {
    const feed = document.getElementById("snsAlertsFeed");
    if (snsAlertsStore.length === 0) {
      feed.innerHTML = '<div style="text-align:center; padding: 40px; color: var(--text-dim); font-size: 0.85rem;">No SNS email alerts dispatched yet.</div>';
      return;
    }

    feed.innerHTML = snsAlertsStore.map(alert => `
      <div class="sns-card">
        <div class="sns-card-subject">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"></path>
            <polyline points="22,6 12,13 2,6"></polyline>
          </svg>
          ${escapeHtml(alert.subject)}
        </div>
        <div class="sns-card-recipients">To: ${alert.recipients.join(", ")} | Dispatched at ${formatTime(alert.published_at)}</div>
        <div class="sns-email-box">${escapeHtml(alert.message)}</div>
      </div>
    `).join("");
  }

  async function refreshS3Files() {
    try {
      const res = await fetch("/api/s3/files?limit=40");
      const files = await res.json();
      const list = document.getElementById("s3FilesList");
      if (files.length === 0) {
        list.innerHTML = '<div style="text-align:center; padding: 20px; color: var(--text-dim);">No files in S3 yet.</div>';
        return;
      }
      list.innerHTML = files.map(f => `
        <div class="s3-file-row" onclick="viewS3File('${f.key}')">
          <span class="s3-key">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
              <polyline points="14 2 14 8 20 8"></polyline>
            </svg>
            ${f.key}
          </span>
          <span style="color: var(--text-dim); font-size: 0.7rem;">${formatTime(f.stored_at)}</span>
        </div>
      `).join("");
    } catch (e) {
      console.error(e);
    }
  }

  function viewS3File(key) {
    const item = logsStore.find(x => x.s3_key === key || (x.incident && `incidents/${x.log.service}/${x.log.event_id}.json` === key));
    if (item) {
      showLogDetail(item.log.event_id);
    }
  }

  async function updateKPIs() {
    try {
      const res = await fetch("/api/status");
      const data = await res.json();
      const stats = data.stats;

      document.getElementById("kpiTotalLogs").innerText = stats.total_logs;
      document.getElementById("kpiTotalIncidents").innerText = stats.total_incidents;
      document.getElementById("kpiIncidentRate").innerText = stats.incident_percentage + "%";
      document.getElementById("kpiAvgLatency").innerText = stats.avg_latency_ms + " ms";
      document.getElementById("kpiTotalS3").innerText = stats.total_s3_stored;
      document.getElementById("kpiTotalSns").innerText = stats.total_sns_published;
    } catch (e) {
      console.warn(e);
    }
  }

  function updateStreamState(running) {
    isStreamRunning = running;
    const btn = document.getElementById("btnToggleStream");
    const icon = document.getElementById("btnToggleIcon");
    const text = document.getElementById("btnToggleText");
    const liveDot = document.getElementById("liveDot");

    if (running) {
      btn.className = "btn btn-pause";
      icon.innerText = "⏸";
      text.innerText = "Pause Streaming";
      liveDot.style.background = "var(--green)";
      liveDot.style.boxShadow = "0 0 10px var(--green)";
    } else {
      btn.className = "btn btn-primary";
      icon.innerText = "▶";
      text.innerText = "Resume Streaming";
      liveDot.style.background = "var(--amber)";
      liveDot.style.boxShadow = "0 0 10px var(--amber)";
    }
  }

  async function toggleStream() {
    const action = isStreamRunning ? "pause" : "start";
    await fetch(`/api/stream/toggle?action=${action}`, { method: "POST" });
    updateStreamState(!isStreamRunning);
    showToast(isStreamRunning ? "Stream resumed" : "Stream paused");
  }

  async function setRate(val, btnEl) {
    document.querySelectorAll(".rate-btn").forEach(b => b.classList.remove("active"));
    btnEl.classList.add("active");
    await fetch(`/api/stream/rate?interval=${val}`, { method: "POST" });
    showToast(`Emission interval set to ${val}s`);
  }

  async function injectIncident(type) {
    let payload = {};
    if (type === "500") {
      payload = { status_code: 500, response_time_ms: 650, level: "ERROR", message: "Database connection timeout (500)" };
    } else if (type === "slow") {
      payload = { status_code: 200, response_time_ms: 4300, level: "INFO", message: "Long database join query took 4300ms" };
    } else if (type === "critical") {
      payload = { status_code: 503, response_time_ms: 4800, level: "CRITICAL", message: "Payment gateway downstream outage" };
    }
    await fetch("/api/stream/inject", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    showToast(`Injected ${type} incident scenario!`);
  }

  async function clearLogs() {
    await fetch("/api/stream/clear", { method: "POST" });
    logsStore = [];
    incidentsStore = [];
    snsAlertsStore = [];
    renderLogs();
    renderIncidents();
    renderSnsAlerts();
    updateKPIs();
    showToast("Local buffer cleared");
  }

  function switchTab(tabName) {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.getElementById("tabContentIncidents").style.display = "none";
    document.getElementById("tabContentSns").style.display = "none";
    document.getElementById("tabContentS3").style.display = "none";
    document.getElementById("tabContentAnalytics").style.display = "none";
    document.getElementById("tabContentTests").style.display = "none";

    if (tabName === "incidents") {
      document.getElementById("tabIncidents").classList.add("active");
      document.getElementById("tabContentIncidents").style.display = "block";
    } else if (tabName === "sns") {
      document.getElementById("tabSns").classList.add("active");
      document.getElementById("tabContentSns").style.display = "block";
    } else if (tabName === "s3") {
      document.getElementById("tabS3").classList.add("active");
      document.getElementById("tabContentS3").style.display = "block";
      refreshS3Files();
    } else if (tabName === "analytics") {
      document.getElementById("tabAnalytics").classList.add("active");
      document.getElementById("tabContentAnalytics").style.display = "block";
      initOrUpdateCharts();
    } else if (tabName === "tests") {
      document.getElementById("tabTests").classList.add("active");
      document.getElementById("tabContentTests").style.display = "block";
    }
  }

  function showLogDetail(eventId) {
    const item = logsStore.find(x => x.log.event_id === eventId);
    if (!item) return;

    document.getElementById("modalTitle").innerText = `Event Inspector [${item.log.service}]`;
    const badgesEl = document.getElementById("modalBadges");
    badgesEl.innerHTML = `
      <span class="badge-status status-${item.log.status_code}">HTTP ${item.log.status_code}</span>
      <span class="badge-level level-${item.log.level.toLowerCase()}">${item.log.level}</span>
      <span class="dispatch-chip">${item.log.response_time_ms} ms</span>
      ${item.is_incident ? '<span class="tag-reason">⚠️ INCIDENT</span>' : '<span class="dispatch-chip" style="color:#34d399;">NORMAL</span>'}
    `;

    document.getElementById("modalJsonContent").innerText = JSON.stringify(item, null, 2);
    document.getElementById("detailModal").classList.add("open");
  }

  function closeModal(id) {
    document.getElementById(id).classList.remove("open");
  }

  function openCustomLogModal() {
    document.getElementById("customLogModal").classList.add("open");
  }

  function openTestModal() {
    switchTab("tests");
  }

  async function submitCustomLog() {
    const svc = document.getElementById("customService").value;
    const status = parseInt(document.getElementById("customStatus").value) || 200;
    const lat = parseInt(document.getElementById("customLatency").value) || 200;
    const lvl = document.getElementById("customLevel").value;
    const msg = document.getElementById("customMessage").value || "Custom test event";

    await fetch("/api/stream/inject", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        service: svc,
        status_code: status,
        response_time_ms: lat,
        level: lvl,
        message: msg
      })
    });

    closeModal("customLogModal");
    showToast("Custom log published to Kafka stream!");
  }

  async function executePytest() {
    const consoleEl = document.getElementById("testConsole");
    const btn = document.getElementById("btnRunTests");
    btn.disabled = true;
    btn.innerText = "⏳ Running...";
    consoleEl.innerText = "Executing pytest tests/test_detector.py in background...";

    try {
      const res = await fetch("/api/tests/run", { method: "POST" });
      const data = await res.json();
      consoleEl.innerText = data.output;
      if (data.success) {
        showToast("✅ All Pytest tests passed!");
      } else {
        showToast("⚠️ Pytest encountered errors.");
      }
    } catch (e) {
      consoleEl.innerText = "Test execution error: " + e;
    } finally {
      btn.disabled = false;
      btn.innerText = "▶ Run Pytest Now";
    }
  }

  // Charts
  function initOrUpdateCharts() {
    if (!latencyChartInstance) {
      const ctx1 = document.getElementById("latencyChart").getContext("2d");
      latencyChartInstance = new Chart(ctx1, {
        type: "line",
        data: {
          labels: logsStore.slice(0, 20).reverse().map(x => formatTime(x.log.timestamp)),
          datasets: [{
            label: "Latency (ms)",
            data: logsStore.slice(0, 20).reverse().map(x => x.log.response_time_ms),
            borderColor: "#06b6d4",
            backgroundColor: "rgba(6, 182, 212, 0.1)",
            fill: true,
            tension: 0.35,
            borderWidth: 2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#64748b" } },
            y: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#64748b" } }
          }
        }
      });
    }

    if (!serviceChartInstance) {
      const ctx2 = document.getElementById("serviceChart").getContext("2d");
      serviceChartInstance = new Chart(ctx2, {
        type: "doughnut",
        data: {
          labels: Object.keys(serviceCounts),
          datasets: [{
            data: Object.values(serviceCounts),
            backgroundColor: ["#06b6d4", "#8b5cf6", "#10b981", "#f59e0b"],
            borderWidth: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: {
              position: "bottom",
              labels: { color: "#94a3b8", font: { size: 10 } }
            }
          }
        }
      });
    }
  }

  function updateCharts(log) {
    if (latencyChartInstance) {
      latencyChartInstance.data.labels.push(formatTime(log.timestamp));
      latencyChartInstance.data.datasets[0].data.push(log.response_time_ms);
      if (latencyChartInstance.data.labels.length > 25) {
        latencyChartInstance.data.labels.shift();
        latencyChartInstance.data.datasets[0].data.shift();
      }
      latencyChartInstance.update("none");
    }

    if (serviceChartInstance) {
      serviceChartInstance.data.datasets[0].data = Object.values(serviceCounts);
      serviceChartInstance.update("none");
    }
  }

  function formatTime(isoStr) {
    if (!isoStr) return "";
    const d = new Date(isoStr);
    return d.toTimeString().split(" ")[0];
  }

  function escapeHtml(str) {
    if (!str) return "";
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function copyHostUrl() {
    navigator.clipboard.writeText("http://localhost:8000");
    showToast("Copied http://localhost:8000 to clipboard!");
  }

  function showToast(msg) {
    const t = document.getElementById("toast");
    t.innerText = msg;
    t.style.display = "block";
    setTimeout(() => { t.style.display = "none"; }, 2500);
  }

  // Initialize on load
  window.addEventListener("DOMContentLoaded", async () => {
    connectSSE();
    await updateKPIs();
    renderLogs();
    renderIncidents();
  });
</script>
</body>
</html>
"""

def main():
    print("=" * 65)
    print("  SecureStream - Real-Time Log Monitoring & Incident Alerting")
    print("=" * 65)
    print("  Localhost Web Dashboard:  http://localhost:8000")
    print("  API Docs (Swagger UI):    http://localhost:8000/docs")
    print("  Kafka mTLS Topic:         application.logs")
    print("  Incident Detector:        Active")
    print("=" * 65)
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")

if __name__ == "__main__":
    main()
