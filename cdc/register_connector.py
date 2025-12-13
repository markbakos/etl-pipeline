import json
import urllib.request

CONNECT_URL = "http://localhost:8083/connectors"
CONFIG_PATH = "cdc/debezium-connector.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    payload = f.read().encode("utf-8")

req = urllib.request.Request(
    CONNECT_URL,
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    with urllib.request.urlopen(req) as resp:
        print(resp.status, resp.read().decode("utf-8"))
except urllib.error.HTTPError as e:
    print("HTTPError:", e.code, e.read().decode("utf-8"))
