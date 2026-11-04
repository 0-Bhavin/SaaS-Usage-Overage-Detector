import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

ACCOUNTS = [
    {"account_id": f"acct_{i}", "plan_tier": random.choice(["basic", "pro", "enterprise"])}
    for i in range(1, 21)
]
PLAN_LIMITS = {"basic": 5000, "pro": 20000, "enterprise": 100000}

# A few accounts are "noisy neighbors" that spike randomly
SPIKE_ACCOUNTS = random.sample(ACCOUNTS, 3)

def generate_event(account):
    base_load = random.randint(10, 50)
    if account in SPIKE_ACCOUNTS and random.random() < 0.15:
        base_load *= random.randint(10, 30)  # simulate a usage spike

    # occasionally emit a late/out-of-order event (real streaming problem)
    ts = datetime.utcnow()
    if random.random() < 0.05:
        ts -= timedelta(minutes=random.randint(1, 10))

    return {
        "account_id": account["account_id"],
        "plan_tier": account["plan_tier"],
        "request_count": base_load,
        "event_time": ts.isoformat(),
    }

if __name__ == "__main__":
    print("Streaming simulated usage events... Ctrl+C to stop.")
    while True:
        account = random.choice(ACCOUNTS)
        event = generate_event(account)
        producer.send("usage-events", value=event)
        print(event)
        time.sleep(random.uniform(0.1, 0.5))
