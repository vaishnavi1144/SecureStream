def detect_incident(log):
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