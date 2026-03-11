import duckdb
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

# Config
NUM_TRANSACTIONS = 10000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)

CARD_TYPES = ["Visa Debit", "Visa Credit", "Mastercard Debit", "Mastercard Credit", "Prepaid"]
STATUSES = ["approved", "declined", "pending", "reversed"]
COUNTRIES = ["US", "GB", "ZA", "NG", "KE", "AE", "SG", "BR", "MX", "IN"]
CURRENCIES = {"US": "USD", "GB": "GBP", "ZA": "ZAR", "NG": "NGN",
              "KE": "KES", "AE": "AED", "SG": "SGD", "BR": "BRL",
              "MX": "MXN", "IN": "INR"}
MERCHANTS = ["Retail", "Food & Beverage", "Travel", "E-commerce",
             "Utilities", "Healthcare", "Entertainment", "Education"]

def generate_transaction():
    country = random.choice(COUNTRIES)
    amount = round(random.expovariate(1/150), 2)  # realistic spend distribution
    is_fraud = random.random() < 0.02              # 2% fraud rate
    status = "declined" if is_fraud and random.random() < 0.6 else random.choices(
        STATUSES, weights=[0.85, 0.08, 0.05, 0.02])[0]

    return {
        "transaction_id": fake.uuid4(),
        "transaction_date": START_DATE + timedelta(
            seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
        ),
        "card_type": random.choice(CARD_TYPES),
        "amount": amount,
        "currency": CURRENCIES[country],
        "country": country,
        "merchant_category": random.choice(MERCHANTS),
        "status": status,
        "is_fraud": is_fraud,
        "cardholder_id": fake.uuid4(),
        "merchant_id": fake.uuid4(),
    }

# Generate transactions
print("Generating transactions...")
transactions = [generate_transaction() for _ in range(NUM_TRANSACTIONS)]
df = pd.DataFrame(transactions)

# Save to DuckDB
con = duckdb.connect("data/payments.duckdb")
con.execute("DROP TABLE IF EXISTS raw_transactions")
con.execute("CREATE TABLE raw_transactions AS SELECT * FROM df")

print(f"Inserted {len(df)} transactions into DuckDB")
print(con.execute("SELECT status, COUNT(*) as count FROM raw_transactions GROUP BY status").fetchdf().to_string())
con.close()