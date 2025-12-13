import urllib.request

CONNECT_URL = "http://localhost:8083/connectors/postgres-cdc/config"
CONFIG_PATH = "cdc/debezium-connector.json"

with open(CONFIG_PATH, "rb") as f:
    payload = f.read()

req = urllib.request.Request(
    CONNECT_URL,
    data=payload,
    headers={"Content-Type": "application/json"},
    method="PUT",
)

with urllib.request.urlopen(req) as resp:
    print(resp.status, resp.read().decode("utf-8"))
