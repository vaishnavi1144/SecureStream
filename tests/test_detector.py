from consumer.detector import detect_incident


def test_normal_request():
    log = {
        "status_code": 200,
        "response_time_ms": 500,
        "level": "INFO"
    }

    result = detect_incident(log)

    assert result["incident"] is False


def test_http_server_error():
    log = {
        "status_code": 500,
        "response_time_ms": 800,
        "level": "ERROR"
    }

    result = detect_incident(log)

    assert result["incident"] is True
    assert "HTTP server error" in result["reasons"]


def test_slow_response():
    log = {
        "status_code": 200,
        "response_time_ms": 4500,
        "level": "INFO"
    }

    result = detect_incident(log)

    assert result["incident"] is True
    assert "Slow API response" in result["reasons"]


def test_multiple_reasons():
    log = {
        "status_code": 503,
        "response_time_ms": 5000,
        "level": "CRITICAL"
    }

    result = detect_incident(log)

    assert result["incident"] is True
    assert "HTTP server error" in result["reasons"]
    assert "Slow API response" in result["reasons"]
    assert "Critical application error" in result["reasons"]