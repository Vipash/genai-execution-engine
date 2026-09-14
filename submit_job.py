"""
Reliable Job Submission Script (Cross-Platform)
"""

import sys
import uuid

import httpx


def submit(key: str | None = None):
    idempotency_key = key or f"crash-test-{uuid.uuid4().hex[:6]}"
    url = "http://localhost:8000/v1/jobs/"
    payload = {
        "workflow_type": "document_ingestion",
        "payload": {"document_url": f"https://arxiv.org/pdf/{idempotency_key}"},
    }
    headers = {"Content-Type": "application/json", "X-Idempotency-Key": idempotency_key}

    try:
        response = httpx.post(url, json=payload, headers=headers, timeout=5.0)
        print(f"\n[Status {response.status_code}]")
        print(response.json())
    except Exception as e:
        print(f"Failed to submit: {e}")


if __name__ == "__main__":
    test_key = sys.argv[1] if len(sys.argv) > 1 else None
    submit(test_key)
