from kafka import KafkaConsumer
import json
import base64
from decimal import Decimal
from db import get_connection
import sys
import traceback

# ---------- Debezium DECIMAL decoder ----------
def decode_decimal(value, scale=2):
    """
    Debezium encodes decimals as Base64 strings.
    This decodes them back into Python Decimal objects.
    """
    if value is None:
        return None
    try:
        raw = base64.b64decode(value)
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return Decimal(unscaled).scaleb(-scale)
    except Exception:
        return None

# ---------- Robust JSON Deserializer ----------
def safe_deserialize(data):
    """
    Prevents AttributeError: 'NoneType' object has no attribute 'decode'
    when encountering Kafka Tombstone messages (null values).
    """
    if data is None:
        return None
    return json.loads(data.decode("utf-8"))

# ---------- Kafka Consumer ----------
consumer = KafkaConsumer(
    "mysqlserver1.shop.orders",
    bootstrap_servers="localhost:29092",
    value_deserializer=safe_deserialize,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="orders-warehouse-sync"
)

# ---------- Postgres Connection ----------
conn = get_connection()
cur = conn.cursor()

print("✅ CDC Consumer started... Waiting for messages.")

try:
    for msg in consumer:
        # Handle Tombstone messages (Debezium sends these after a delete)
        if msg.value is None:
            print(f"skipping tombstone at offset={msg.offset}")
            consumer.commit()
            continue

        try:
            # Debezium wraps data in a 'payload' block
            payload = msg.value.get("payload")
            if payload is None:
                continue

            op = payload.get("op")

            # ---------- INSERT / UPDATE / SNAPSHOT (c=create, u=update, r=read/snapshot) ----------
            if op in ("c", "u", "r"):
                after = payload.get("after")
                if after is None:
                    continue

                cur.execute(
                    """
                    INSERT INTO orders (
                        order_id,
                        user_id,
                        amount,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id)
                    DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        after["order_id"],
                        after["user_id"],
                        decode_decimal(after["amount"], scale=2),
                        after["status"],
                        after["created_at"],
                        after["updated_at"],
                    )
                )

            # ---------- DELETE (d=delete) ----------
            elif op == "d":
                before = payload.get("before")
                if before is not None:
                    cur.execute(
                        "DELETE FROM orders WHERE order_id = %s",
                        (before["order_id"],)
                    )

            # ---------- Commit both DB and Kafka ----------
            conn.commit()
            consumer.commit()
            print(f"✔ Processed event: op={op}, id={msg.key}, offset={msg.offset}")

        except Exception as e:
            conn.rollback()
            print("❌ ERROR while processing message at offset", msg.offset)
            traceback.print_exc(file=sys.stdout)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    cur.close()
    conn.close()