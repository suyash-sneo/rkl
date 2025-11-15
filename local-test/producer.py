import json
import random
import time
import uuid
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Categorical choices (realistic values)
SERVICES = ["auth", "orders", "billing", "catalog", "search"]
ENVS = ["prod", "staging"]
REGIONS = ["us-east-1", "eu-west-1", "ap-south-1"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
EVENT_TYPES = ["login", "purchase", "logout", "password_reset", "view"]
USER_ROLES = ["admin", "customer", "service"]
COUNTRIES = ["US", "DE", "IN", "GB", "BR"]
STATUS_CODES = [200, 201, 204, 400, 401, 403, 404, 409, 429, 500, 502, 503]
PATHS = [
    "/api/v1/auth/login",
    "/api/v1/auth/logout",
    "/api/v1/orders",
    "/api/v1/orders/12345",
    "/api/v1/catalog/items",
    "/api/v1/catalog/items/42",
    "/api/v1/search",
    "/api/v1/profile",
]


def build_key(service: str, env: str, region: str, user_id: str) -> str:
    # Structured key derived from categorical fields; no random gibberish strings
    return f"{service}-{env}-{region}:{user_id[:8]}"


def generate_log_event() -> dict:
    """Generate a realistic log/event payload with categorical strings and GUIDs."""
    service = random.choice(SERVICES)
    env = random.choice(ENVS)
    region = random.choice(REGIONS)
    user_id = str(uuid.uuid4())

    req_method = random.choice(METHODS)
    path = random.choice(PATHS)
    status = random.choice(STATUS_CODES)
    duration_ms = random.randint(5, 1500)
    size_bytes = random.randint(100, 200_000)
    event_type = random.choice(EVENT_TYPES)
    role = random.choice(USER_ROLES)
    country = random.choice(COUNTRIES)

    # Optional response message; occasionally include the word "error" to support CONTAINS
    if status >= 500:
        resp_msg = "internal server error"
    elif status == 404:
        resp_msg = "resource not found"
    elif status >= 400:
        resp_msg = "client error"
    else:
        resp_msg = "ok"

    payload = {
        "meta": {
            "id": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            "service": service,
            "env": env,
            "region": region,
            "source": random.choice(["web", "mobile", "service"]),
        },
        "request": {
            "method": req_method,
            "path": path,
            "headers": {
                "contentType": random.choice(["application/json", "text/plain", "application/x-www-form-urlencoded"]),
                "accept": random.choice(["*/*", "application/json"]),
            },
        },
        "response": {
            "status": status,
            "duration_ms": duration_ms,
            "size_bytes": size_bytes,
            "msg": resp_msg,
        },
        "user": {
            "id": user_id,
            "role": role,
            "country": country,
        },
        "event": {
            "type": event_type,
            "success": status < 400,
        },
    }
    return payload

def wait_for_kafka(bootstrap_servers, max_retries=30):
    """Wait for Kafka to be ready"""
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            print("Kafka is ready!")
            return True
        except NoBrokersAvailable:
            print(f"Waiting for Kafka... (attempt {i+1}/{max_retries})")
            time.sleep(2)
    return False

def main():
    bootstrap_servers = ['kafka:29092']
    topic = 'random-data'
    num_messages = 10000
    
    print("Waiting for Kafka to be available...")
    if not wait_for_kafka(bootstrap_servers):
        print("Failed to connect to Kafka after maximum retries")
        return
    
    print("Creating Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print(f"Starting to produce seed messages then {num_messages} structured messages to topic '{topic}'...")

    # Seed messages crafted to exercise query features (AND/OR, CONTAINS, !=/<> and JSON paths)
    seed_payloads = []

    # 1) auth-prod key with PUT and an error in response msg
    p1 = generate_log_event()
    p1["meta"]["service"] = "auth"
    p1["meta"]["env"] = "prod"
    p1["meta"]["region"] = "us-east-1"
    p1["request"]["method"] = "PUT"
    p1["response"]["status"] = 500
    p1["response"]["msg"] = "hello error world"
    k1 = build_key(p1["meta"]["service"], p1["meta"]["env"], p1["meta"]["region"], p1["user"]["id"])
    seed_payloads.append((k1, p1))

    # 2) orders service, non-GET method
    p2 = generate_log_event()
    p2["meta"]["service"] = "orders"
    p2["request"]["method"] = "POST"
    p2["response"]["status"] = 201
    p2["response"]["msg"] = "ok"
    k2 = build_key(p2["meta"]["service"], p2["meta"]["env"], p2["meta"]["region"], p2["user"]["id"])
    seed_payloads.append((k2, p2))

    # 3) billing service with a not-found error
    p3 = generate_log_event()
    p3["meta"]["service"] = "billing"
    p3["request"]["method"] = "DELETE"
    p3["response"]["status"] = 404
    p3["response"]["msg"] = "not found error"
    k3 = build_key(p3["meta"]["service"], p3["meta"]["env"], p3["meta"]["region"], p3["user"]["id"])
    seed_payloads.append((k3, p3))

    # 4) catalog service, GET success false (e.g., 503)
    p4 = generate_log_event()
    p4["meta"]["service"] = "catalog"
    p4["request"]["method"] = "GET"
    p4["response"]["status"] = 503
    p4["response"]["msg"] = "service unavailable error"
    k4 = build_key(p4["meta"]["service"], p4["meta"]["env"], p4["meta"]["region"], p4["user"]["id"])
    seed_payloads.append((k4, p4))

    # 5) purchase event success=true for filtering
    p5 = generate_log_event()
    p5["event"]["type"] = "purchase"
    p5["response"]["status"] = 200
    p5["response"]["msg"] = "ok"
    k5 = build_key(p5["meta"]["service"], p5["meta"]["env"], p5["meta"]["region"], p5["user"]["id"])
    seed_payloads.append((k5, p5))

    for k, v in seed_payloads:
        producer.send(topic, key=k, value=v)

    print(f"Produced {len(seed_payloads)} seed messages...")

    for i in range(num_messages):
        payload = generate_log_event()
        key = build_key(payload["meta"]["service"], payload["meta"]["env"], payload["meta"]["region"], payload["user"]["id"])
        producer.send(topic, key=key, value=payload)
        if (i + 1) % 1000 == 0:
            print(f"Produced {i + 1} structured messages...")
    
    # Wait for all messages to be delivered
    producer.flush()
    print(f"Successfully produced {num_messages} messages!")
    
    producer.close()
    print("Producer finished and closed.")

if __name__ == "__main__":
    main()
