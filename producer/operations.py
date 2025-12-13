import random
from typing import Optional

from helper import rand_email, rand_amount

DEFAULT_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]
DEFAULT_CURRENCIES = ["EUR", "USD", "GBP", "RSD"]
DEFAULT_COUNTRIES = ["RS", "DE", "FR", "GB", "US", "HU", "RO", "IT", "ES"]

def create_customer(cur) -> int:
    email = rand_email()
    country = random.choice(DEFAULT_COUNTRIES)
    cur.execute(
        """
        INSERT INTO customers(email, country)
        VALUES (%s, %s)
            RETURNING id
        """,
        (email, country)
    )
    return int(cur.fetchone()["id"])

def get_random_customer_id(cur) -> Optional[int]:
    cur.execute("SELECT id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return int(row["id"]) if row else None

def create_order(cur, customer_id: int) -> int:
    status = random.choice(DEFAULT_STATUSES[:2])
    amount = rand_amount()
    currency = random.choice(DEFAULT_CURRENCIES)

    cur.execute(
        """
        INSERT INTO orders(customer_id, status, amount, currency)
        VALUES (%s, %s, %s, %s)
            RETURNING id
        """,
        (customer_id, status, amount, currency)
    )
    return int(cur.fetchone()["id"])

def get_random_order_id(cur) -> Optional[int]:
    cur.execute("SELECT id from orders ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return int(row["id"]) if row else None

def update_random_order(cur) -> Optional[int]:
    oid = get_random_order_id(cur)
    if oid is None:
        return None

    update_type = random.choice(["status", "amount", "currency"])
    if update_type == "status":
        new_status = random.choice(DEFAULT_STATUSES)
        cur.execute("UPDATE orders SET status=%s WHERE id=%s", (new_status, oid))
    elif update_type == "amount":
        new_amount = rand_amount()
        cur.execute("UPDATE orders SET amount=%s WHERE id=%s", (new_amount, oid))
    else:
        new_currency = random.choice(DEFAULT_CURRENCIES)
        cur.execute("UPDATE orders SET currency=%s WHERE id=%s", (new_currency, oid))

    return oid

def update_random_customer(cur) -> Optional[int]:
    cid = get_random_customer_id(cur)
    if cid is None:
        return None
    new_country = random.choice(DEFAULT_COUNTRIES)
    cur.execute("UPDATE customers SET country=%s WHERE id=%s", (new_country, cid))
    return cid

def delete_random_order(cur) -> Optional[int]:
    oid = get_random_order_id(cur)
    if oid is None:
        return None
    cur.execute("DELETE FROM orders WHERE id=%s", (oid,))
    return oid

def delete_random_customer(cur) -> Optional[int]:
    cid = get_random_customer_id(cur)
    if cid is None:
        return None
    cur.execute("DELETE FROM customers WHERE id=%s", (cid,))
    return cid
