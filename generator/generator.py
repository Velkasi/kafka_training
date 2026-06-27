import random
import time

import psycopg2

names = ["Alice", "Bob", "Charlie", "Diana", "Erik", "Fatima", "Gabriel", "Hugo"]
statuses = ["pending", "paid", "shipped", "delivered", "cancelled"]

# Laisse le temps a Postgres de demarrer et de creer la table via init-source.sql
time.sleep(15)

conn = psycopg2.connect(
    host="postgres-source",
    dbname="sourcedb",
    user="postgres",
    password="postgres",
)
conn.autocommit = True
cur = conn.cursor()

print("Generateur de trafic demarre (Ctrl+C ou docker compose stop pour arreter).")

while True:
    if random.random() < 0.4:
        name = random.choice(names)
        amount = round(random.uniform(10, 500), 2)
        cur.execute(
            "INSERT INTO orders (customer_name, amount, status) "
            "VALUES (%s, %s, 'pending') RETURNING id",
            (name, amount),
        )
        order_id = cur.fetchone()[0]
        print(f"INSERT  order {order_id} ({name}, {amount} EUR)")
    else:
        cur.execute("SELECT id, status FROM orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if row:
            order_id, current_status = row
            new_status = random.choice(statuses)
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = now() WHERE id = %s",
                (new_status, order_id),
            )
            print(f"UPDATE  order {order_id}: {current_status} -> {new_status}")

    time.sleep(random.uniform(2, 5))
