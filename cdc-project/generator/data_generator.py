import mysql.connector
import random
import time

conn = mysql.connector.connect(
    host='localhost',
    port=3307,
    user='root',
    password='root',
    database='shop'
)
cursor = conn.cursor()

user_names = ["Alice", "Bob", "Charlie", "David"]
statuses = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]

while True:
    # insert user
    name = random.choice(user_names)
    email = f"{name.lower()}{random.randint(1,100)}@test.com"
    cursor.execute("INSERT INTO users (name,email) VALUES (%s,%s)", (name,email))
    user_id = cursor.lastrowid

    # insert order
    amount = random.randint(10,1000)
    status = random.choice(statuses)
    cursor.execute("INSERT INTO orders (user_id,amount,status) VALUES (%s,%s,%s)", (user_id,amount,status))

    # randomly update order
    if random.random() < 0.3:
        cursor.execute("UPDATE orders SET status=%s WHERE order_id=%s", (random.choice(statuses), user_id))

    # randomly delete order
    if random.random() < 0.1:
        cursor.execute("DELETE FROM orders WHERE order_id=%s", (user_id,))

    conn.commit()
    time.sleep(2)
