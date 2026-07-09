# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SCM Data Generator
# MAGIC %md
# MAGIC # SCM Daily Data Generator
# MAGIC
# MAGIC Simulates daily file delivery from a business system for the Supply Chain Management demo pipeline.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Generates ~100 customer records and ~500 order records per run
# MAGIC - Writes date-stamped CSV files to `/Volumes/<catalog>/scm_volume/input_data/`（カタログはwidgetで指定）
# MAGIC - Includes intentional bad data for downstream quality checks:
# MAGIC   - ~10% of customers have missing/NULL email addresses
# MAGIC   - ~5% of orders have negative amounts
# MAGIC
# MAGIC **Idempotent behavior:**
# MAGIC - Creates schema and volume if they don't exist
# MAGIC - Same-day reruns overwrite that day's files
# MAGIC - Different days produce separate batches

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
# ジョブ実行時は base_parameters の "catalog" が注入される。対話実行時はデフォルト値を使用。
dbutils.widgets.text("catalog", "rongzi_catalog", "カタログ名")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "scm_volume"
VOLUME = "input_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Data generation parameters
NUM_CUSTOMERS = 100
NUM_ORDERS = 500
BAD_EMAIL_RATE = 0.10  # ~10% missing emails
BAD_AMOUNT_RATE = 0.05  # ~5% negative amounts

# Customer segments
SEGMENTS = ["Enterprise", "SMB", "Startup"]
SEGMENT_WEIGHTS = [0.3, 0.5, 0.2]

# Order statuses
STATUSES = ["completed", "pending", "cancelled"]
STATUS_WEIGHTS = [0.6, 0.25, 0.15]

print(f"Output path: {VOLUME_PATH}")
print(f"Customers per batch: {NUM_CUSTOMERS} (~{int(NUM_CUSTOMERS * BAD_EMAIL_RATE)} with bad email)")
print(f"Orders per batch: {NUM_ORDERS} (~{int(NUM_ORDERS * BAD_AMOUNT_RATE)} with negative amount)")

# COMMAND ----------

# DBTITLE 1,Idempotent Setup
# Create schema and volume if they don't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print(f"✓ Schema '{CATALOG}.{SCHEMA}' ready")
print(f"✓ Volume '{CATALOG}.{SCHEMA}.{VOLUME}' ready")

# COMMAND ----------

# DBTITLE 1,Generate Customers CSV
import uuid
import random
from datetime import date

random.seed(None)  # Different data each run
today = date.today().strftime("%Y%m%d")

# Generate customer data
first_names = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
               "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
               "Thomas", "Sarah", "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
               "Anthony", "Betty", "Mark", "Margaret", "Steven", "Sandra"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
domains = ["example.com", "corp.io", "business.net", "company.org", "enterprise.co"]

customers = []
for i in range(NUM_CUSTOMERS):
    customer_id = str(uuid.uuid4())
    first = random.choice(first_names)
    last = random.choice(last_names)
    name = f"{first} {last}"
    
    # ~10% chance of missing email (bad data)
    if random.random() < BAD_EMAIL_RATE:
        email = ""  # Missing email
    else:
        email = f"{first.lower()}.{last.lower()}@{random.choice(domains)}"
    
    segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
    customers.append((customer_id, name, email, segment))

# Write to CSV using plain Python file I/O
customers_file = f"{VOLUME_PATH}/customers_{today}.csv"
with open(customers_file, "w") as f:
    f.write("customer_id,name,email,segment\n")
    for cid, name, email, segment in customers:
        f.write(f"{cid},{name},{email},{segment}\n")

bad_email_count = sum(1 for _, _, email, _ in customers if email == "")
print(f"✓ Generated {len(customers)} customers ({bad_email_count} with missing email)")
print(f"  → {customers_file}")

# COMMAND ----------

# DBTITLE 1,Generate Orders CSV
from datetime import timedelta

# Generate orders referencing existing customer_ids
customer_ids = [c[0] for c in customers]

orders = []
for i in range(NUM_ORDERS):
    order_id = str(uuid.uuid4())
    customer_id = random.choice(customer_ids)
    
    # Base amount between 50 and 5000
    amount = round(random.uniform(50, 5000), 2)
    
    # ~5% chance of negative amount (bad data)
    if random.random() < BAD_AMOUNT_RATE:
        amount = round(-random.uniform(10, 500), 2)
    
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    
    # Order date: random date within last 30 days
    days_ago = random.randint(0, 30)
    order_date = (date.today() - timedelta(days=days_ago)).isoformat()
    
    orders.append((order_id, customer_id, amount, status, order_date))

# Write to CSV using plain Python file I/O
orders_file = f"{VOLUME_PATH}/orders_{today}.csv"
with open(orders_file, "w") as f:
    f.write("order_id,customer_id,amount,status,order_date\n")
    for oid, cid, amount, status, order_date in orders:
        f.write(f"{oid},{cid},{amount},{status},{order_date}\n")

bad_amount_count = sum(1 for _, _, amt, _, _ in orders if amt < 0)
print(f"✓ Generated {len(orders)} orders ({bad_amount_count} with negative amount)")
print(f"  → {orders_file}")

# COMMAND ----------

# DBTITLE 1,Verification
import os

print("=" * 60)
print("GENERATED FILES")
print("=" * 60)

# List all files in the volume
files = os.listdir(VOLUME_PATH)
for f in sorted(files):
    filepath = os.path.join(VOLUME_PATH, f)
    size = os.path.getsize(filepath)
    # Count lines (subtract 1 for header)
    with open(filepath, "r") as fh:
        line_count = sum(1 for _ in fh) - 1
    print(f"  {f:40s} {size:>8,} bytes  {line_count:>5} rows")

print("\n" + "=" * 60)
print("SAMPLE DATA")
print("=" * 60)

# Show first few rows of today's files
print(f"\n--- customers_{today}.csv (first 5 rows) ---")
with open(f"{VOLUME_PATH}/customers_{today}.csv", "r") as f:
    for i, line in enumerate(f):
        if i < 6:  # header + 5 rows
            print(line.strip())

print(f"\n--- orders_{today}.csv (first 5 rows) ---")
with open(f"{VOLUME_PATH}/orders_{today}.csv", "r") as f:
    for i, line in enumerate(f):
        if i < 6:
            print(line.strip())
