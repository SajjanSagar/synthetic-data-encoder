What the app does
    Reads CSVs from data/real/
    Uses a local SQLite database (artifacts/staging.db)
    Trains SDV on that local data
    Writes synthetic CSVs to data/synthetic/

No network usage in your code
    No imports of requests, httpx, urllib, socket, aiohttp, boto3, or openai
    No HTTP calls or URL fetching
    No API keys or remote endpoints

Safety controls
    safety.py blocks network-related modules when SAFE_MODE=true
    security_check.py scans for risky imports and flags them

SDV dependency
    SDV pulls in libraries like botocore that can do network I/O
    Your code never calls those parts
    With SAFE_MODE=true, those modules are blocked at import time

Summary: All processing is local. Input comes from local CSVs, and output is written to local files. No online data is used.