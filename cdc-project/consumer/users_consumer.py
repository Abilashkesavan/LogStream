from kafka import KafkaConsumer
import json
from db import get_connection
import sys

# ---------- Robust JSON Deserializer ----------
def safe_deserialize(data):
    if data is None:
        return None
    return json.loads(data.decode("utf-8"))

# ---------- Kafka Consumer ----------
consumer = KafkaConsumer(
    "mysqlserver1.shop.users",
    bootstrap_servers="localhost:29092",  # Use localhost and 29092 for Mac
    value_deserializer=safe_deserialize,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="users-warehouse-sync"       # Required for consumer.commit()
)

# ---------- Postgres Connection ----------
conn = get_connection()
cur = conn.cursor()

print("✅ Users CDC Consumer started...")

try:
    for msg in consumer:
        # 1. Handle Tombstones (null messages from Debezium)
        if msg.value is None:
            consumer.commit()
            continue

        payload = msg.value.get("payload")
        if not payload:
            continue

        op = payload.get("op")

        # 2. Extract data (Debezium wraps everything in payload)
        if op in ("c", "u", "r"):
            after = payload.get("after")
            cur.execute("""
                INSERT INTO users (user_id, name, email, created_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    name=EXCLUDED.name,
                    email=EXCLUDED.email
            """, (
                after["user_id"],
                after["name"],
                after["email"],
                after["created_at"]
            ))

        elif op == "d":
            before = payload.get("before")
            if before:
                cur.execute("DELETE FROM users WHERE user_id=%s", (before["user_id"],))

        # 3. Transactional commit
        conn.commit()
        consumer.commit()
        print(f"✔ Processed User event: op={op}, offset={msg.offset}")

except KeyboardInterrupt:
    print("\nStopping Users consumer...")
finally:
    cur.close()
    conn.close()