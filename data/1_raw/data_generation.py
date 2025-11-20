import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

np.random.seed(42)

# --- 1. customers.csv ---
n_customers = 1500
customer_ids = [f"C{100000+i}" for i in range(n_customers)]

def random_date():
    start = datetime(2019,1,1)
    end = datetime(2023,12,31)
    delta = end - start
    d = start + timedelta(days=np.random.randint(delta.days))
    # mix formats
    if random.random() < 0.3:
        return d.strftime("%d/%m/%Y")
    else:
        return d.strftime("%Y-%m-%d")

customers = pd.DataFrame({
    "customer_id": customer_ids,
    "signup_date": [random_date() for _ in range(n_customers)],
    "country": np.random.choice(["DE","FR","ES","IT","NL","??",""], size=n_customers, p=[.2,.2,.2,.2,.1,.05,.05]),
    "age": np.random.choice(list(range(18,80)) + [None,None,None], n_customers),
    "gender": np.random.choice(["M","F","X","Unknown",""], n_customers),
    "plan_type": np.random.choice(["basic","premium","gold","?",None], n_customers),
    "monthly_fee": np.round(np.random.uniform(5,60, n_customers),2)
})

# duplicates
customers = pd.concat([customers, customers.sample(20)]) 

# --- 2. transactions.csv ---
records = []
for cid in customer_ids:
    months = np.random.randint(3, 36)
    start_date = datetime(2021,1,1)
    for m in range(months):
        date = start_date + timedelta(days=30*m)
        records.append([
            cid,
            date.strftime("%Y-%m-%d"),
            np.random.normal(300,100),
            abs(np.random.normal(5,2)) if random.random()>0.1 else -abs(np.random.normal(1,1)), # some negative
            max(0,int(np.random.normal(50,20))),
            np.random.uniform(-5,120) # some negative payments
        ])

transactions = pd.DataFrame(records, columns=[
    "customer_id","date","call_minutes","data_usage_gb","sms_count","amount_paid"
])

# remove some customer ids to create orphan rows
transactions.loc[transactions.sample(60).index, "customer_id"] = "C999999"

# --- 3. support_interactions.csv ---
ticket_records = []
for _ in range(2500):
    cid = random.choice(customer_ids + ["C000000",""])  # missing/bad ids
    t = datetime(2021,1,1) + timedelta(days=np.random.randint(0,1500))
    # mix timestamp formats
    timestamp = t.strftime("%Y-%m-%d %H:%M:%S") if random.random() < 0.6 else t.strftime("%d/%m/%Y %H:%M")
    ticket_records.append([
        f"T{random.randint(10000,99999)}",
        cid,
        timestamp,
        random.choice(["email","phone","chat","","whatsapp"]),
        random.choice(["billing","technical","general","??",""]),
        np.random.choice([None] + list(np.random.randint(1,300,size=10))),
        np.random.choice([0,1,None])
    ])

support = pd.DataFrame(ticket_records, columns=[
    "ticket_id","customer_id","timestamp","channel","issue_type","resolution_time_min","was_resolved"
])

# save files
customers.to_csv("data/1_raw/customers.csv", index=False)
transactions.to_csv("data/1_raw/transactions.csv", index=False)
support.to_csv("data/1_raw/support_interactions.csv", index=False)
