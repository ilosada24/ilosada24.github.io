# Lakeflow Spark Declarative Pipelines: your first declarative data pipeline, step by step

If you come from the world of traditional data engineering — Airflow, dbt, hand-rolled Spark jobs — you've probably heard of **Delta Live Tables**, or its more recent name, **Lakeflow Spark Declarative Pipelines**. The promise is appealing: you define *what* you want to transform, and Databricks takes care of the *how*. No manual dependency management, no hand-written retries, no worrying about execution order.

In this post we're going to build a complete pipeline from scratch, going from raw data to an analysis-ready table. Everything explained step by step, and **compatible with Databricks Free Edition** — you don't need a paid account to follow along.

---

## What is Lakeflow Spark Declarative Pipelines?

**Lakeflow Spark Declarative Pipelines (SDP)** is Databricks' framework for building declarative data pipelines on top of Delta Lake. If you've been around the Databricks ecosystem for a while, you'll know it as **Delta Live Tables (DLT)** — that's its former name. In June 2025, with the launch of Lakeflow at the Data + AI Summit, DLT became the declarative transformation layer within the Lakeflow platform, and got a new name and a revamped API.

> **Have existing DLT code?** No need to migrate it. `import dlt` still works. The new API (`from pyspark import pipelines as dp`) is the recommended path for new code, and it's what we'll use in this tutorial.

The core idea hasn't changed: instead of writing code that executes transformations in a specific order, **you declare the result you want** using decorators in Python or SQL. Databricks infers the dependency graph, handles retries, and guarantees data quality through *expectations*.

### When should you use it?

- Medallion pipelines (Bronze → Silver → Gold) on Delta Lake
- Incremental ingestion with CDC (Change Data Capture)
- When you want declarative data quality without building validations by hand
- Environments where Databricks is the primary platform

---

## Key concepts before writing any code

Before diving in, it's worth being clear on three concepts:

### New API: `@dp.table` vs `@dp.materialized_view`

With the new API, the distinction between streaming tables and materialized views is explicit through separate decorators:

| Decorator | What it creates | When to use it |
|---|---|---|
| `@dp.table` | Streaming Table | Incremental ingestion; processes only new data |
| `@dp.materialized_view` | Materialized View | Batch transformations that are fully recomputed |

In the old code with `import dlt`, everything was `@dlt.table` regardless of type. Now it's clearer.

### Expectations (data quality)

These are declarative constraints on your data. You can declare that a column can't be null, that a value must be within a range, and so on. Most importantly: you decide what happens when the constraint is violated (warn, drop the row, or fail the pipeline).

### Pipeline vs Job

A Lakeflow SDP pipeline is not a standard Databricks Job. It has its own UI, its own development mode, and its own scheduler. Jobs can orchestrate pipelines, but they are distinct entities.

---

## The scenario: online store sales

We're going to build a classic medallion pipeline with order data from a fictional online store:

- **Bronze**: ingest the raw orders from a JSON file
- **Silver**: clean, validate, and type the data
- **Gold**: compute sales metrics per product

---

## Step 1: Create the volume and prepare the sample data

In Free Edition (and in any modern workspace with Unity Catalog), files are stored in **UC Volumes**. First we create the volume from a SQL notebook:

```sql
-- Run this in a SQL notebook or in the Databricks SQL Editor
CREATE CATALOG IF NOT EXISTS tutorial;
CREATE SCHEMA IF NOT EXISTS tutorial.sales;
CREATE VOLUME IF NOT EXISTS tutorial.sales.raw_data;
```

Now we generate the test data. In a Python notebook:

```python
import json
from datetime import datetime, timedelta
import random

# Generate 100 fictional orders
products = ["laptop", "keyboard", "mouse", "monitor", "headphones"]
orders = []

for i in range(1, 101):
    base_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 90))
    orders.append({
        "order_id": f"ORD-{i:04d}",
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 500), 2),
        "order_date": base_date.strftime("%Y-%m-%d"),
        "customer_id": f"CUST-{random.randint(1, 20):03d}",
        # Deliberately introduce some dirty data
        "total": round(random.uniform(10, 2500), 2) if i % 10 != 0 else None
    })

# Save to the Unity Catalog Volume
path = "/Volumes/tutorial/sales/raw_data/orders.json"
dbutils.fs.put(
    path,
    "\n".join(json.dumps(o) for o in orders),
    overwrite=True
)

print(f"Generated {len(orders)} orders at {path}")
```

> **Why a Volume and not `/tmp/`?** Unity Catalog Volumes are the current standard for storing files in Databricks. They sit under Unity Catalog access control, they're compatible with Free Edition, and they're the recommended approach both in learning environments and in production.

---

## Step 2: Create the pipeline in the Databricks UI

1. In the sidebar, go to **Data Engineering → Lakeflow Pipelines** (or search for "Pipelines" in the navigation bar).
2. Click **Create pipeline**.
3. Fill in the basic fields:
   - **Pipeline name**: `sales_medallion`
   - **Pipeline mode**: *Triggered* (runs when you launch it manually) or *Continuous* (permanent streaming). For this tutorial use *Triggered*.
   - **Source code**: here you'll point to the notebook we'll create in the next step.
   - **Target catalog**: `tutorial`
   - **Target schema**: `sales_dev`

For now leave everything else at its defaults and save. We'll come back here to point to the notebook.

---

## Step 3: Write the pipeline code

Create a new Python notebook. **Important**: Lakeflow SDP notebooks use a special decorator-based syntax. Don't run the cells individually — they only work within the pipeline context. Databricks detects this automatically when the notebook is attached to a pipeline.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# Path to the file containing the mock data in the volume
PATH = "/Volumes/tutorial/sales/raw_data/"

# ============================================================
# BRONZE LAYER: ingest the raw data, untransformed
# ============================================================

@dp.table(
    comment="Orders ingested from the source JSON file",
    table_properties={"quality": "bronze"}
)
@dp.expect("total not null", "total IS NOT NULL")

def bronze_orders():
    """
    Reads the raw JSON data and loads it into a Delta table without
    applying any transformation
    """

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_date", StringType(), True),  # Arrives as a string
        StructField("customer_id", StringType(), True),
        StructField("total", DoubleType(), True),
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema)
        .load(PATH)
    )


# ============================================================
# SILVER LAYER: cleaning, validation, and proper typing
# ============================================================

@dp.table(
    comment="Clean, validated orders",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("order_id not null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid product",
                "product IN ('laptop', 'keyboard', 'mouse', 'monitor', 'headphones')")
@dp.expect("positive quantity", "quantity > 0") # warns but doesn't drop the record
@dp.expect_or_drop("positive unit_price", "unit_price > 0")

def silver_orders():
    """
    On top of the Bronze layer:
    - Cast order_date to DateType
    - Recompute the total (we don't trust the source value)
    - Apply expectations to guarantee quality
    """

    return (
        dp.read_stream("bronze_orders")
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("total_calculated",
                    F.round(F.col("quantity") * F.col("unit_price"), 2))
        .drop("total") # Discard the source total (random data with nulls)
        .withColumnRenamed("total_calculated", "total")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================
# GOLD LAYER: aggregated business metrics (Materialized View)
# ============================================================

@dp.materialized_view(
    comment="Total revenue and units sold per product",
    table_properties={"quality": "gold"}
)
def gold_sales_by_product():
    """
    Aggregate sales per product:
    - Total revenue
    - Units sold
    - Average price per unit
    - Number of orders

    We use a materialized view because this is a batch aggregation that is
    recomputed on every pipeline run
    """

    return (
        dp.read("silver_orders")
        .groupBy("product")
        .agg(
            F.round(F.sum("total"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            F.countDistinct("order_id").alias("num_orders")
        )
        .orderBy(F.desc("total_revenue"))
    )
```

### What changed compared to the old API (DLT)?

If you've seen older DLT examples, you'll notice these differences:

| Old API (`import dlt`) | New API (`from pyspark import pipelines as dp`) |
|---|---|
| `@dlt.table(...)` for everything | `@dp.table(...)` for streaming, `@dp.materialized_view(...)` for batch |
| `dlt.read("table")` | `dp.read("table")` or `dp.read_stream("table")` |
| `@dlt.expect(...)` | `@dp.expect(...)` |
| Streaming with `spark.readStream` + Auto Loader | Same, unchanged |

Old code with `import dlt` **still works** without modification. The new API is simply more explicit.

---

## Step 4: Attach the notebook to the pipeline and run it

1. Go back to the `sales_medallion` pipeline you created earlier.
2. Under **Source code**, add the path to the notebook you just created.
3. Click **Start**.

You'll see Databricks:
- Infer the dependency graph automatically (`bronze → silver → gold`)
- Run the layers in the correct order
- Show in real time the rows processed and the expectations applied

### The DAG you'll see in the UI
![alt](assets/img/01-dag.png)
---

## Step 5: Understanding expectations

This is one of the most powerful parts of Lakeflow SDP. Expectations have three behaviors:

```python
# Only records the violation as a metric, does nothing else
@dp.expect("name", "SQL condition")

# Drops the rows that violate the condition
@dp.expect_or_drop("name", "SQL condition")

# Fails the entire pipeline if any row violates it
@dp.expect_or_fail("name", "SQL condition")
```

In the pipeline UI, in each table's panel, you'll see how many rows passed, how many were dropped, and how many violations were recorded. This is the foundation of an **observable data quality** system with no need for external frameworks like Great Expectations.

---

## Step 6: Query the results

Once the pipeline has run, the tables are available in the `tutorial.sales_dev` schema. You can query them from any notebook or from the SQL Editor:

```sql
-- From the Databricks SQL Editor
SELECT * FROM tutorial.sales_dev.gold_sales_by_product
ORDER BY total_revenue DESC;
```

```python
# From a Python notebook
df = spark.table("tutorial.sales_dev.gold_sales_by_product")
display(df)
```

You can also explore them visually from the Databricks **Catalog Explorer**: go to **Catalog → tutorial → sales_dev** and you'll see the three tables with their metadata, lineage, and quality statistics.

---

## Development mode vs production mode

One thing that's confusing at first: the pipeline has two execution modes.

**Development mode**: every time you click *Start*, Databricks reuses the existing compute (fast startup, ideal for iterating). The data is fully reprocessed from scratch on every run.

**Production mode**: uses fresh compute on every run to guarantee clean environments. This is the mode you'll use once the pipeline is in production.

You switch between them from the pipeline's settings button, without touching the code.

> **On Free Edition**, all compute is serverless — there are no classic clusters to configure. This simplifies the initial setup quite a bit.

---

## Recap and next steps

What we covered today:

- What Lakeflow Spark Declarative Pipelines (SDP) is and how it relates to DLT
- The new API (`@dp.table`, `@dp.materialized_view`) and how it differs from the old one
- How to structure a Bronze → Silver → Gold medallion pipeline
- How to declare data quality with `@dp.expect`, `expect_or_drop`, and `expect_or_fail`
- How to use Unity Catalog Volumes to store the source data
- How to follow the whole tutorial on **Databricks Free Edition**, at no cost

This is just the entry point. Once you've got this down, the natural next steps are:

- **Advanced incremental ingestion with Auto Loader**: automatically detect new files in the Volume without reprocessing the old ones
- **CDC with `APPLY CHANGES INTO`**: sync tables from change streams (Kafka, Kinesis, etc.)
- **Parameterization with pipeline configurations**: separate dev, staging, and prod without duplicating code
- **Orchestration with Lakeflow Jobs**: chain pipelines together with other Databricks Jobs

If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
