import redis, json

r = redis.Redis(host="genai_redis", port=6379)
r.xadd("jobs:stream", {
"job_id": "d1c03d51-d78d-4467-b3ec-dc8b9638f1a8",
"workflow_type": "document_indexing",
"payload": json.dumps({"document_url": "https://arxiv.org/doc/sample"})
})
print("Job enqueued successfully with payload")
