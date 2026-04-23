"""
Synthetic retail datasets for Databricks batch analytics (PySpark).

Writes CSVs under a Unity Catalog volume, by default:
  /Volumes/practice/demo_dw_raw/raw_data/{customers,products,transactions}/

Override the base path (if your volume lives elsewhere):
  export RETAIL_SYNTH_VOLUME_BASE="/Volumes/<catalog>/<schema>/<volume>/raw_data"

Or use another catalog name (same schema and subpath as above):
  export RETAIL_UC_CATALOG="your_catalog"

Run on Databricks (cluster / serverless notebook / job) or locally via Databricks Connect
(Cursor + `.venv311`): ensure `faker` and `pandas` are installed in that environment.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta
from typing import List, Sequence, Tuple

import pandas as pd
from databricks.connect import DatabricksSession
from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

def _get_base_volume() -> str:
    """Unity Catalog path for raw CSVs; default catalog is ``practice``."""
    explicit = os.environ.get("RETAIL_SYNTH_VOLUME_BASE", "").strip()
    if explicit:
        return explicit.rstrip("/")
    catalog = (os.environ.get("RETAIL_UC_CATALOG", "") or "practice").strip()
    return f"/Volumes/{catalog}/demo_dw_raw/raw_data"


def _spark() -> SparkSession:
    """Remote Spark via Databricks Connect (serverless) or classic cluster if configured."""
    builder = DatabricksSession.builder
    if hasattr(builder, "serverless"):
        return builder.serverless(True).getOrCreate()
    os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
    return builder.getOrCreate()


def generate_customers(spark: SparkSession, n: int = 10_000, seed: int = 42) -> DataFrame:
    """Customers with ~2% nulls in email/city combined and ~1% duplicate rows (total n rows)."""
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)

    dup_count = max(1, int(round(n * 0.01)))
    base_n = n - dup_count

    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    states = [
        ("New York", "NY"),
        ("Los Angeles", "CA"),
        ("Chicago", "IL"),
        ("Houston", "TX"),
        ("Phoenix", "AZ"),
        ("Philadelphia", "PA"),
        ("San Antonio", "TX"),
        ("San Diego", "CA"),
        ("Dallas", "TX"),
        ("Seattle", "WA"),
        ("Boston", "MA"),
        ("Denver", "CO"),
        ("Miami", "FL"),
        ("Atlanta", "GA"),
        ("Portland", "OR"),
    ]

    rows: List[dict] = []
    for customer_id in range(1, base_n + 1):
        city, state = rng.choice(states)
        email = fake.email()
        signup = fake.date_between(start_date="-5y", end_date="today")

        # ~2% combined: roughly half get null email, half null city, small overlap
        r = rng.random()
        if r < 0.01:
            email = None
        elif r < 0.02:
            city = None

        rows.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": email,
                "city": city,
                "state": state,
                "signup_date": signup.isoformat(),
                "loyalty_tier": rng.choice(tiers),
            }
        )

    # ~1% duplicate rows: exact copies of random existing rows (including customer_id)
    for _ in range(dup_count):
        rows.append(dict(rng.choice(rows)))

    schema = StructType(
        [
            StructField("customer_id", IntegerType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("signup_date", StringType(), True),  # ISO date string
            StructField("loyalty_tier", StringType(), True),
        ]
    )

    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf, schema=schema).withColumn(
        "signup_date", F.to_date(F.col("signup_date"))
    )


def generate_products(spark: SparkSession, n: int = 5_000, seed: int = 43) -> DataFrame:
    """Products with skewed category distribution."""
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)

    # Weighted categories (Electronics-heavy skew)
    categories: Sequence[Tuple[str, float]] = (
        ("Electronics", 0.38),
        ("Clothing", 0.18),
        ("Home & Kitchen", 0.14),
        ("Sports", 0.10),
        ("Beauty", 0.08),
        ("Books", 0.06),
        ("Toys", 0.06),
    )
    cat_names = [c[0] for c in categories]
    cat_weights = [c[1] for c in categories]

    brands_by_cat = {
        "Electronics": ["TechNova", "Voltiq", "BrightBit", "PulseGear"],
        "Clothing": ["UrbanThread", "NorthFold", "SilkLane", "TrailMark"],
        "Home & Kitchen": ["Homestead", "KettleCo", "LumaNest", "CraftHaus"],
        "Sports": ["PeakRun", "AquaEdge", "IronStride", "SummitPro"],
        "Beauty": ["GlowLab", "PureHue", "VelvetBloom", "LumenCare"],
        "Books": ["PageTurn", "InkTrail", "ShelfWise", "ChapterOne"],
        "Toys": ["PlayNest", "WonderBrick", "TinyTrek", "JoyStack"],
    }

    rows: List[dict] = []
    for product_id in range(1, n + 1):
        category = rng.choices(cat_names, weights=cat_weights, k=1)[0]
        brand = rng.choice(brands_by_cat[category])
        price = round(rng.lognormvariate(2.8, 0.35), 2)
        price = max(4.99, min(price, 899.99))
        created = fake.date_between(start_date="-4y", end_date="today").isoformat()
        name = f"{brand} {fake.catch_phrase().title()[:40]}"

        rows.append(
            {
                "product_id": product_id,
                "product_name": name,
                "category": category,
                "brand": brand,
                "price": price,
                "created_date": created,
            }
        )

    schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("created_date", StringType(), True),
        ]
    )
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf, schema=schema).withColumn(
        "created_date", F.to_date(F.col("created_date"))
    )


def _weighted_customer_ids(
    customer_ids: List[int],
    n_transactions: int,
    rng: random.Random,
    vip_fraction: float = 0.05,
    vip_share: float = 0.42,
) -> List[int | None]:
    """Skew: a small set of customers receive many transactions."""
    ids = sorted(set(customer_ids))
    if not ids:
        raise ValueError("No customer_ids available")

    k_vip = max(1, int(len(ids) * vip_fraction))
    vip = set(rng.sample(ids, k=k_vip))
    heavy = list(vip)
    regular = [i for i in ids if i not in vip]

    out: List[int | None] = []
    for _ in range(n_transactions):
        if rng.random() < vip_share and heavy:
            out.append(rng.choice(heavy))
        else:
            out.append(rng.choice(regular))
    return out


def generate_transactions(
    spark: SparkSession,
    customers_df: DataFrame,
    products_df: DataFrame,
    n: int = 25_000,
    seed: int = 44,
) -> DataFrame:
    """
    Transactions with ~1% null customer_id, ~1% duplicate transaction_id rows,
    and skewed customer activity.
    """
    rng = random.Random(seed)

    cust_ids = [r.customer_id for r in customers_df.select("customer_id").distinct().collect()]
    prod_rows = products_df.select("product_id", "price").collect()
    product_prices = {int(r.product_id): float(r.price) for r in prod_rows}
    product_ids = list(product_prices.keys())

    weighted_cust = _weighted_customer_ids(cust_ids, n, rng)

    rows: List[dict] = []
    start_ts = datetime.now() - timedelta(days=365)

    for tx_idx in range(1, n + 1):
        cust = weighted_cust[tx_idx - 1]
        if rng.random() < 0.01:
            cust = None

        pid = rng.choice(product_ids)
        base_price = product_prices[pid]
        qty = int(rng.choices([1, 2, 3, 4, 5, 6], weights=[0.45, 0.22, 0.12, 0.08, 0.07, 0.06], k=1)[0])
        noise = rng.uniform(0.92, 1.08)
        amount = round(base_price * qty * noise, 2)

        ts = start_ts + timedelta(
            seconds=int(rng.expovariate(1 / 3600)),
            minutes=rng.randint(0, 59),
            hours=rng.randint(0, 23),
            days=rng.randint(0, 364),
        )
        channel = rng.choices(["online", "store"], weights=[0.55, 0.45], k=1)[0]

        rows.append(
            {
                "transaction_id": tx_idx,
                "customer_id": cust,
                "product_id": pid,
                "quantity": qty,
                "transaction_amount": amount,
                "transaction_timestamp": ts,
                "channel": channel,
            }
        )

    # ~1% duplicate transaction_id: reuse ids for last chunk of rows
    dup_tx_ids = max(1, int(n * 0.01))
    for i in range(dup_tx_ids):
        target = rng.randint(1, n - dup_tx_ids - 1)
        rows[-(i + 1)]["transaction_id"] = target

    schema = StructType(
        [
            StructField("transaction_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("channel", StringType(), False),
        ]
    )
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf, schema=schema)


def write_dataset_csv(df: DataFrame, base_volume: str, subdir: str, name: str) -> str:
    path = f"{base_volume.rstrip('/')}/{subdir.strip('/')}"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )
    return path


def print_overview(label: str, df: DataFrame, sample_n: int = 5) -> None:
    print(f"\n=== {label} ===")
    df.printSchema()
    print(f"Row count: {df.count()}")
    df.show(sample_n, truncate=False)


def main() -> None:
    spark = _spark()

    customers = generate_customers(spark)
    products = generate_products(spark)
    transactions = generate_transactions(spark, customers, products)

    print_overview("customers (before write)", customers)
    print_overview("products (before write)", products)
    print_overview("transactions (before write)", transactions)

    base = _get_base_volume()
    print(f"\nWriting to base path: {base}\n")
    c_path = write_dataset_csv(customers, base, "customers", "customers")
    p_path = write_dataset_csv(products, base, "products", "products")
    t_path = write_dataset_csv(transactions, base, "transactions", "transactions")

    print("\nWrote CSV directories:")
    for p in (c_path, p_path, t_path):
        print(f"  {p}")


if __name__ == "__main__":
    main()