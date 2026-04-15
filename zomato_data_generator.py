import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
import os
import json

fake = Faker()
random.seed(42)
np.random.seed(42)

BASE_DIR = "zomato_data"

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

ensure_dir(BASE_DIR)

# -----------------------------
# Row counts / reference lists
# -----------------------------
N_CUSTOMERS = 5000
N_RESTAURANTS = 1000
N_AGENTS = 800
N_MENU_ITEMS = 10000
N_PROMOS = 200
N_ORDERS = 50000
N_FEEDBACK = 20000
N_EVENTS = 30000  # JSON events

cities = ["Bengaluru", "Mumbai", "Delhi", "Pune", "Hyderabad", "Chennai"]
areas = ["Central", "North", "South", "East", "West"]
cuisines = ["North Indian", "South Indian", "Chinese", "Italian", "Fast Food", "Bakery", "Healthy"]
vehicle_types = ["Bike", "Scooter", "Cycle"]
payment_methods = ["CASH", "UPI", "CARD", "WALLET"]

start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 1, 1)
date_range_days = (end_date - start_date).days

def random_datetime():
    return start_date + timedelta(days=random.randint(0, date_range_days),
                                  minutes=random.randint(0, 24*60-1))

# =============================
# 1) CUSTOMER
# =============================
customers = []
for cid in range(1, N_CUSTOMERS+1):
    city = random.choice(cities)
    is_prime_raw = random.choice(["1", "0", "Y", "N", "", "TRUE", "FALSE"])
    if random.random() < 0.05:
        is_prime_raw = ""  # blank -> STG default

    customers.append({
        "CUSTOMER_ID": cid,
        "CUSTOMER_NAME": fake.name(),
        "EMAIL": fake.email(),
        "PRIMARY_PHONE": fake.msisdn()[:10],
        "REGISTERED_AT": random_datetime().isoformat(sep=" "),
        "CITY": city,
        "AREA": random.choice(areas),
        "LAT": fake.latitude(),
        "LNG": fake.longitude(),
        "SEGMENT": random.choice(["New", "Active", "Loyal", "Churn_Risk"]),
        "IS_PRIME_MEMBER": is_prime_raw,
        "STATUS": random.choice(["ACTIVE", "INACTIVE", ""])
    })

df_customers = pd.DataFrame(customers)
dir_cust = os.path.join(BASE_DIR, "customer")
ensure_dir(dir_cust)
df_customers.to_csv(os.path.join(dir_cust, "CUSTOMER.csv"), index=False)

# =============================
# 2) RESTAURANT
# =============================
restaurants = []
for rid in range(1, N_RESTAURANTS+1):
    city = random.choice(cities)
    avg_rating = round(random.uniform(3.0, 4.8), 2)
    if random.random() < 0.03:
        avg_rating = ""  # blank rating

    is_active_raw = random.choice(["1", "0", "Y", "N", "", "TRUE", "FALSE"])
    restaurants.append({
        "RESTAURANT_ID": rid,
        "RESTAURANT_NAME": fake.company(),
        "CUISINE_PRIMARY": random.choice(cuisines),
        "CUISINE_SECONDARY": random.choice(cuisines),
        "CITY": city,
        "AREA": random.choice(areas),
        "LAT": fake.latitude(),
        "LNG": fake.longitude(),
        "AVG_RATING": avg_rating,
        "COMMISSION_RATE": round(random.uniform(0.15, 0.30), 3),
        "IS_ACTIVE": is_active_raw,
        "ONBOARDED_AT": random_datetime().isoformat(sep=" ")
    })

df_restaurants = pd.DataFrame(restaurants)
dir_rest = os.path.join(BASE_DIR, "restaurant")
ensure_dir(dir_rest)
df_restaurants.to_csv(os.path.join(dir_rest, "RESTAURANT.csv"), index=False)

# =============================
# 3) DELIVERY_AGENT
# =============================
agents = []
for aid in range(1, N_AGENTS+1):
    agents.append({
        "AGENT_ID": aid,
        "AGENT_NAME": fake.name(),
        "PHONE": fake.msisdn()[:10],
        "HIRE_DATE": random_datetime().isoformat(sep=" "),
        "CITY": random.choice(cities),
        "VEHICLE_TYPE": random.choice(vehicle_types),
        "STATUS": random.choice(["ACTIVE", "INACTIVE"])
    })

df_agents = pd.DataFrame(agents)
dir_agent = os.path.join(BASE_DIR, "delivery_agent")
ensure_dir(dir_agent)
df_agents.to_csv(os.path.join(dir_agent, "DELIVERY_AGENT.csv"), index=False)

# =============================
# 4) MENU_ITEM
# =============================
menu_items = []
for mid in range(1, N_MENU_ITEMS+1):
    rid = random.randint(1, N_RESTAURANTS)
    base_price = random.uniform(80, 600)
    price_val = round(base_price, 2)
    if random.random() < 0.03:
        price_val = ""  # blank -> default later

    menu_items.append({
        "MENU_ITEM_ID": mid,
        "RESTAURANT_ID": rid,
        "ITEM_NAME": fake.word().title() + " " + random.choice(["Thali", "Pizza", "Burger", "Bowl", "Meal"]),
        "CATEGORY": random.choice(["Main", "Starter", "Dessert", "Beverage"]),
        "PRICE": price_val,
        "IS_VEG": random.choice(["1","0","Y","N"]),
        "IS_ACTIVE": random.choice(["1","0","Y","N",""])
    })

df_menu = pd.DataFrame(menu_items)
dir_menu = os.path.join(BASE_DIR, "menu_item")
ensure_dir(dir_menu)
df_menu.to_csv(os.path.join(dir_menu, "MENU_ITEM.csv"), index=False)

# =============================
# 5) PROMOTION
# =============================
promos = []
for pid in range(1, N_PROMOS+1):
    start = random_datetime()
    end = start + timedelta(days=random.randint(7, 60))
    promos.append({
        "PROMO_ID": pid,
        "PROMO_CODE": f"PROMO_{pid}",
        "DISCOUNT_PERCENT": random.choice([10, 15, 20, 25, 30, 40, 50]),
        "START_DATE": start.isoformat(sep=" "),
        "END_DATE": end.isoformat(sep=" "),
        "MAX_DISCOUNT_AMT": random.choice([50, 75, 100, 150, 200]),
        "TARGET_SEGMENT": random.choice(["New", "Active", "Loyal", "Churn_Risk", "ALL"]),
        "TARGET_CITY": random.choice(cities + ["ALL"])
    })

df_promos = pd.DataFrame(promos)
dir_promo = os.path.join(BASE_DIR, "promotion")
ensure_dir(dir_promo)
df_promos.to_csv(os.path.join(dir_promo, "PROMOTION.csv"), index=False)

# =============================
# 6) ORDERS & RELATED
# =============================
orders = []
order_items = []
deliveries = []
payments = []
feedbacks = []

order_item_id = 1
trip_id = 1
payment_id = 1
feedback_id = 1

for oid in range(1, N_ORDERS+1):
    customer = random.randint(1, N_CUSTOMERS)
    restaurant = random.randint(1, N_RESTAURANTS)
    order_time = random_datetime()
    promo_id = random.choice([None] * 3 + list(range(1, N_PROMOS+1)))
    status = random.choice(["DELIVERED", "CANCELLED", "DELIVERED", "DELIVERED"])
    base_items = random.randint(1, 5)

    subtotal = 0
    for _ in range(base_items):
        menu_id = random.randint(1, N_MENU_ITEMS)
        qty = random.randint(1, 3)
        price = df_menu.loc[df_menu["MENU_ITEM_ID"] == menu_id, "PRICE"].iloc[0]
        price_val = float(price) if price != "" else random.uniform(80, 600)
        discount_item = round(price_val * random.choice([0, 0.05, 0.1]), 2)
        total_item = (price_val - discount_item) * qty
        subtotal += total_item
        order_items.append({
            "ORDER_ID": oid,
            "ORDER_ITEM_ID": order_item_id,
            "MENU_ITEM_ID": menu_id,
            "QTY": qty,
            "ITEM_PRICE": round(price_val, 2),
            "ITEM_DISCOUNT": discount_item,
            "TOTAL_ITEM_AMOUNT": round(total_item, 2)
        })
        order_item_id += 1

    discount_order = round(subtotal * random.choice([0, 0.05, 0.1, 0.15]), 2) if promo_id else 0
    if random.random() < 0.05:
        discount_order_raw = ""
    else:
        discount_order_raw = discount_order

    delivery_fee = random.choice([20, 30, 40, 50])
    if random.random() < 0.05:
        delivery_fee_raw = ""
    else:
        delivery_fee_raw = delivery_fee

    total_amount = subtotal - (discount_order if discount_order_raw != "" else 0) + (delivery_fee if delivery_fee_raw != "" else 0)

    expected_delivery_at = order_time + timedelta(minutes=random.choice([25, 30, 35, 40, 45]))
    actual_delivery_at = expected_delivery_at + timedelta(minutes=random.randint(-10, 30)) if status == "DELIVERED" else None
    cancellation_reason = None if status == "DELIVERED" else random.choice(["Customer Cancelled", "Restaurant Cancelled", "Payment Failure"])

    orders.append({
        "ORDER_ID": oid,
        "CUSTOMER_ID": customer,
        "RESTAURANT_ID": restaurant,
        "ORDER_CREATED_AT": order_time.isoformat(sep=" "),
        "ORDER_STATUS": status,
        "PAYMENT_METHOD": random.choice(payment_methods),
        "PROMO_ID": promo_id if promo_id is not None else "",
        "ORDER_SUBTOTAL": round(subtotal, 2),
        "ORDER_DISCOUNT": discount_order_raw,
        "DELIVERY_FEE": delivery_fee_raw,
        "TOTAL_AMOUNT": round(total_amount, 2),
        "EXPECTED_DELIVERY_AT": expected_delivery_at.isoformat(sep=" "),
        "ACTUAL_DELIVERY_AT": actual_delivery_at.isoformat(sep=" ") if actual_delivery_at else "",
        "CANCELLATION_REASON": cancellation_reason if cancellation_reason else ""
    })

    # DELIVERY_TRIP
    agent_id = random.randint(1, N_AGENTS)
    if status == "DELIVERED":
        pickup_time = order_time + timedelta(minutes=random.choice([10, 15, 20]))
        drop_time = actual_delivery_at
        distance_km = round(random.uniform(1.0, 12.0), 2)
        estimated_time_min = random.choice([20, 25, 30, 35])
        actual_time_min = int((drop_time - pickup_time).total_seconds() / 60)
        deliveries.append({
            "TRIP_ID": trip_id,
            "ORDER_ID": oid,
            "AGENT_ID": agent_id,
            "PICKUP_TIME": pickup_time.isoformat(sep=" "),
            "DROP_TIME": drop_time.isoformat(sep=" "),
            "DISTANCE_KM": distance_km,
            "ESTIMATED_TIME_MIN": estimated_time_min,
            "ACTUAL_TIME_MIN": actual_time_min,
            "SLA_BREACH_FLAG": 1 if actual_time_min > estimated_time_min + 5 else 0
        })
        trip_id += 1

    payments.append({
        "PAYMENT_ID": payment_id,
        "ORDER_ID": oid,
        "PAYMENT_STATUS": "SUCCESS" if status == "DELIVERED" else random.choice(["FAILED", "REFUNDED"]),
        "PAYMENT_AT": (order_time + timedelta(minutes=random.randint(1, 5))).isoformat(sep=" "),
        "PAYMENT_AMOUNT": round(total_amount, 2)
    })
    payment_id += 1

    if status == "DELIVERED" and random.random() < 0.4 and feedback_id <= N_FEEDBACK:
        rating = random.randint(1, 5)
        feedbacks.append({
            "FEEDBACK_ID": feedback_id,
            "ORDER_ID": oid,
            "CUSTOMER_ID": customer,
            "RATING": rating,
            "COMMENT": fake.sentence(nb_words=12),
            "SENTIMENT_SCORE": rating - 3 + random.uniform(-0.5, 0.5),
            "CREATED_AT": (order_time + timedelta(hours=random.randint(1, 48))).isoformat(sep=" ")
        })
        feedback_id += 1

df_orders = pd.DataFrame(orders)
dir_oh = os.path.join(BASE_DIR, "order_header")
ensure_dir(dir_oh)
df_orders.to_csv(os.path.join(dir_oh, "ORDER_HEADER.csv"), index=False)

df_order_items = pd.DataFrame(order_items)
dir_oi = os.path.join(BASE_DIR, "order_item")
ensure_dir(dir_oi)
df_order_items.to_csv(os.path.join(dir_oi, "ORDER_ITEM.csv"), index=False)

df_deliveries = pd.DataFrame(deliveries)
dir_del = os.path.join(BASE_DIR, "delivery_trip")
ensure_dir(dir_del)
df_deliveries.to_csv(os.path.join(dir_del, "DELIVERY_TRIP.csv"), index=False)

df_payments = pd.DataFrame(payments)
dir_pay = os.path.join(BASE_DIR, "payment")
ensure_dir(dir_pay)
df_payments.to_csv(os.path.join(dir_pay, "PAYMENT.csv"), index=False)

df_feedbacks = pd.DataFrame(feedbacks)
dir_fb = os.path.join(BASE_DIR, "customer_feedback")
ensure_dir(dir_fb)
df_feedbacks.to_csv(os.path.join(dir_fb, "CUSTOMER_FEEDBACK.csv"), index=False)

# =============================
# 7) JSON CUSTOMER_EVENTS
# =============================
dir_events = os.path.join(BASE_DIR, "customer_events")
ensure_dir(dir_events)
events_path = os.path.join(dir_events, "CUSTOMER_EVENTS.json")

event_types = ["SEARCH", "APP_OPEN", "ADD_TO_CART", "FAVORITE", "PROMO_VIEW"]

with open(events_path, "w", encoding="utf-8") as f:
    for eid in range(1, N_EVENTS + 1):
        cust_id = random.randint(1, N_CUSTOMERS)
        etype = random.choice(event_types)
        ts = random_datetime().isoformat(sep=" ")

        metadata = {}
        if etype == "SEARCH":
            metadata = {
                "search_query": random.choice(["biryani", "pizza", "burger", "thali", "salad"]),
                "device_os": random.choice(["Android", "iOS"]),
                "app_version": random.choice(["4.3.1", "4.3.2", "4.4.0"])
            }
        elif etype == "ADD_TO_CART":
            metadata = {
                "menu_item_id": random.randint(1, N_MENU_ITEMS),
                "quantity": random.randint(1, 4)
            }
        elif etype == "APP_OPEN":
            metadata = {
                "device_os": random.choice(["Android", "iOS"]),
                "referrer": random.choice(["Push_Notification", "Organic", "Ad"])
            }
        elif etype == "FAVORITE":
            metadata = {
                "restaurant_id": random.randint(1, N_RESTAURANTS)
            }
        elif etype == "PROMO_VIEW":
            metadata = {
                "promo_code": f"PROMO_{random.randint(1, N_PROMOS)}"
            }

        event = {
            "event_id": eid,
            "customer_id": cust_id,
            "event_type": etype,
            "event_ts": ts,
            "metadata": metadata
        }
        f.write(json.dumps(event) + "\n")

print(f"Data generated under folder: {BASE_DIR}")
