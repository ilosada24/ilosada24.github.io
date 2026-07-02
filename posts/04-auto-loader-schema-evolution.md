# Auto Loader and schema evolution: bulletproof bronze ingestion

Every medallion architecture starts with the same unglamorous problem: files keep landing in a folder, their schemas keep drifting, and your bronze layer is supposed to ingest everything **exactly once** without anyone babysitting it. New columns appear because a backend team shipped on Friday. A field that was always a number arrives as a string. And the one thing you cannot do is silently lose data.

The Databricks answer to all of this is **Auto Loader** — the `cloudFiles` source — and the **Data Engineer Professional exam** tests it hard. Not the happy path; the failure semantics. What happens when a new column shows up? When a type doesn't match? When you re-run the stream? Those are the questions, and in this post we'll trigger every one of those situations on purpose.

On **Databricks Free Edition** we don't have an S3 bucket to point at, but we don't need one: Auto Loader reads **Unity Catalog Volumes** natively, and that's where our landing zone will live.

---

## Setup: a landing zone in a Volume

First, a schema and a Volume to act as the folder where files "arrive". I'm reusing the `tutorial` catalog and the `ventas` schema from the [earlier governance post](03-unity-catalog-governance.md), so if you followed along there, the schema already exists:

```sql
CREATE SCHEMA IF NOT EXISTS tutorial.ventas;
CREATE VOLUME IF NOT EXISTS tutorial.ventas.landing;
```

Now let's play the role of the upstream system and drop the first batch of orders, from a Python notebook:

```python
import json

base = "/Volumes/tutorial/ventas/landing"

batch1 = [
    {"order_id": 1, "customer": "Ana",   "amount": 42.5},
    {"order_id": 2, "customer": "Brais", "amount": 17.0},
]
dbutils.fs.put(f"{base}/orders_001.json",
               "\n".join(json.dumps(r) for r in batch1), True)
```

Two well-behaved JSON records. Enjoy them; it's the last clean data you'll see in this post.

A quick sanity check that the file actually landed where we think it did — cheap habit that saves a lot of "why is my stream empty" confusion later:

```python
display(dbutils.fs.ls(base))
```

---

## The ingestion stream, line by line

Here's the canonical Auto Loader pattern for incremental bronze ingestion:

```python
checkpoint = "/Volumes/tutorial/ventas/landing/_checkpoints/orders"

(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.inferColumnTypes", "true")        # infer real types, not all-strings
   .option("cloudFiles.schemaLocation", checkpoint)      # where the inferred schema lives
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .load(base)
 .writeStream
   .option("checkpointLocation", checkpoint)
   .option("mergeSchema", "true")                        # let the TARGET table evolve too
   .trigger(availableNow=True)                           # batch-style incremental run
   .toTable("tutorial.ventas.orders_bronze"))
```

Run it, then check the result:

```sql
SELECT * FROM tutorial.ventas.orders_bronze;
```

Two rows, plus a column you didn't ask for: **`_rescued_data`**, currently all nulls. Hold that thought — it's about to earn its keep.

Before we break things, five lines of that stream deserve a closer look, because each one carries exam weight:

**`format("cloudFiles")`** is what makes this Auto Loader rather than a plain file stream. Auto Loader discovers new files incrementally and keeps track of which ones it has already ingested — that bookkeeping is the heart of its exactly-once guarantee.

**`cloudFiles.inferColumnTypes`** exists because of a default that surprises everyone once: for formats that don't declare their own types — JSON, CSV — Auto Loader infers **every column as a string** unless you set this to `true`. The logic is defensive: a string can hold anything, so all-strings ingestion can never hit a type conflict. Safe, and useless for us — this post *needs* `amount` to be a real `double`, because the rescued-data demo below depends on having a type to violate. Leave the option out and `"not_a_number"` would slide into a string-typed `amount` without a whisper. (Don't confuse it with `inferSchema`, its cousin on the batch DataFrame reader — on Auto Loader, this is the option. And if an exam question says "all my JSON columns arrived as strings", this default is the answer.)

**`cloudFiles.schemaLocation`** is where Auto Loader persists the schema it inferred from the data, so the next run starts from the same schema instead of re-inferring from scratch (and so evolution has a baseline to evolve *from*). It's mandatory when you rely on inference — leave it out and the stream refuses to start. Pointing it at the same path as the checkpoint, like we did, is the common convention.

**`mergeSchema` on the writeStream** is the easy one to forget, because it looks redundant next to `schemaEvolutionMode` — and it isn't. Schema evolution has **two halves**. `cloudFiles.schemaEvolutionMode` governs the *source*: whether Auto Loader's inferred schema absorbs new columns. `mergeSchema` governs the *sink*: whether the target Delta table is allowed to grow those columns on write. Delta enforces its schema on every write, streaming or not, so with the first option but not the second you get the worst of both worlds: Auto Loader happily evolves, restarts... and the write dies with a `DELTA_METADATA_MISMATCH` because `orders_bronze` refuses the new column. Both toggles, or neither.

**`trigger(availableNow=True)`** tells the stream: process every pending file, then **stop**. This is the standard pattern for scheduled incremental jobs — you get streaming semantics (incremental, exactly-once) with batch economics (the cluster isn't running at 3 a.m. for no reason).

One thing I'd add before this ever reaches production, and something I wish I'd internalised earlier: a bronze table should record *where each row came from and when it arrived*. Auto Loader exposes the hidden `_metadata` column for exactly this, so tacking on a couple of provenance fields costs almost nothing:

```python
from pyspark.sql import functions as F

(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.inferColumnTypes", "true")
   .option("cloudFiles.schemaLocation", checkpoint)
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .load(base)
   .select("*",
           F.col("_metadata.file_path").alias("source_file"),
           F.current_timestamp().alias("ingested_at"))
 .writeStream
   .option("checkpointLocation", checkpoint)
   .option("mergeSchema", "true")
   .trigger(availableNow=True)
   .toTable("tutorial.ventas.orders_bronze"))
```

When a downstream number looks wrong six months from now, `source_file` is the difference between "I know which file to re-check" and a full-day archaeology dig. I'll keep the plain version in the examples below to stay focused on schema behavior, but this is the shape I actually ship.

---

## Now let's break the schema

Time for the upstream team to "ship on Friday". The second batch has a **new column** (`channel`) and a **type violation** (`amount` arriving as a string):

```python
batch2 = [
    {"order_id": 3, "customer": "Carla", "amount": 88.0,
     "channel": "web"},                                   # NEW column
    {"order_id": 4, "customer": "Diego", "amount": "not_a_number"},  # BAD type
]
dbutils.fs.put(f"{base}/orders_002.json",
               "\n".join(json.dumps(r) for r in batch2), True)
```

Re-run the stream cell and watch carefully, because two very different things happen to those two problems.

### The new column: fail, evolve, succeed

The run **fails**. On purpose. You'll see an `UnknownFieldException` complaining about `channel`. This is `addNewColumns` doing exactly what it promises:

1. The stream encounters a column not in the stored schema and fails the current run.
2. **Before dying, it updates the schema** at `schemaLocation` to include `channel`.
3. The **next** run starts with the new schema and processes the file happily.

Run the cell again and it goes through: the evolved source schema flows in, and `mergeSchema` lets `orders_bronze` grow the `channel` column to receive it. (If you'd left `mergeSchema` out, this is the exact moment you'd pay for it — the restart would die with `DELTA_METADATA_MISMATCH`, because the *source* evolved but the *sink* wasn't allowed to follow.) Why this fail-and-restart dance instead of just absorbing the column mid-flight? Because a schema change mid-stream could silently change the meaning of in-flight micro-batches. Failing forces a clean restart on the new schema. And in production this isn't even an inconvenience: a Lakeflow job with a retry policy restarts the stream automatically, and the schema has already evolved by then. The "failure" is a one-retry blip.

### The bad type: rescued, not failed

Here's the asymmetry the exam loves: `"not_a_number"` does **not** fail the stream. The stored schema says `amount` is a double; this value isn't; instead of dying or dropping the row, Auto Loader puts the offending value in **`_rescued_data`**:

```sql
SELECT order_id, amount, _rescued_data
FROM tutorial.ventas.orders_bronze
WHERE _rescued_data IS NOT NULL;
-- amount is NULL, _rescued_data = {"amount": "not_a_number", "_file_path": "..."}
```

The row arrives, `amount` is null, and the original string is preserved in a JSON blob alongside the file it came from. Nothing was lost — it's all sitting there waiting for a human (or a silver-layer rule) to decide what to do with it. That's the bronze-layer philosophy in one column: **ingest everything, judge later**.

If the `_rescued_data` name clashes with a real field in your source (it happens more than you'd think), rename it with `cloudFiles.rescuedDataColumn`. And if you already *know* the correct type of a field up front, you can skip the rescue dance entirely with a schema hint — Auto Loader will cast to that type instead of inferring:

```python
.option("cloudFiles.schemaHints", "amount double, order_id long")
```

Hints sit nicely between the two inference extremes: all-strings (the default), full inference (`inferColumnTypes`), or inference with surgical overrides for the columns where you know better than the sample.

So remember the split: **new columns** are a schema evolution event (behavior depends on the mode), **type mismatches** are a rescue event (the row lands, the bad value goes to `_rescued_data`).

---

## The four evolution modes

`addNewColumns` is the default, but there are four modes, and the exam will absolutely ask you to distinguish them:

| Mode | Where the new column goes | Stream behavior |
|---|---|---|
| `addNewColumns` (default) | Into the schema | Fails once, evolves, restart succeeds |
| `rescue` | Into `_rescued_data` | Never fails; schema is frozen |
| `failOnNewColumns` | Nowhere | Fails until you update the schema manually |
| `none` | Dropped | New data silently ignored (unless the rescued column is configured) |

A way to keep them straight: ask *"do I want the table to grow with the source?"* If yes → `addNewColumns`. If you want a frozen contract but zero data loss → `rescue` (everything unexpected piles up in `_rescued_data`). If you want a frozen contract enforced loudly → `failOnNewColumns`. And `none` is the "I accept silent data loss" mode, which is rarely what anyone actually wants.

The exam phrasing to watch for: *"the stream failed after new columns arrived — what happened?"* With the default mode, the answer is: nothing is wrong, the schema evolved, restart the stream (or let the job retry do it).

---

## Exactly-once and the checkpoint

Run the stream a third time, with no new files in the Volume. It starts, finds nothing to do, and stops. `orders_001.json` and `orders_002.json` are not re-ingested, no duplicate rows appear.

That memory lives in the **checkpoint**: Auto Loader records every ingested file in a RocksDB store inside the checkpoint directory. If you're the kind of person who only believes it when you see it, peek inside:

```python
display(dbutils.fs.ls(f"{checkpoint}/sources/0/rocksdb"))
```

The file list *is* the exactly-once guarantee. Which leads directly to the classic operational question — *"how do I reprocess everything from scratch?"* — and its answer: **new checkpoint** (and typically a new or truncated target table). Delete or change the checkpoint path, and Auto Loader has amnesia: every file in the landing zone is new again.

Corollary worth internalizing: the checkpoint and the target table are a *couple*. Reset one without the other and you get either duplicates (old table + new checkpoint) or a hole (new table + old checkpoint, since already-seen files won't re-ingest). There's also `cloudFiles.allowOverwrites` for the niche case of upstream systems that legitimately rewrite files in place — know it exists, use it reluctantly.

### Directory listing vs file notification

One last concept, exam-relevant even though you can't demo it on Free Edition. Auto Loader has two ways of discovering new files:

- **Directory listing** (default): periodically list the path and diff against the ingested set. Works everywhere — including UC Volumes, which is why everything in this post just worked.
- **File notification**: subscribe to cloud storage events (S3 → SQS, etc.) so arrivals are pushed instead of discovered. Scales much better when you have millions of files or very frequent arrivals, but requires cloud infrastructure permissions to set up the event plumbing.

The exam wants the trade-off: listing is simple and universal; notifications scale, at the cost of cloud setup. If a question mentions "millions of files per day" and "listing is slow", it's steering you toward notification mode.

---

## Recap and next steps

What we covered today:

- The bronze-ingestion contract: **incremental, exactly-once, lose nothing**, and how Auto Loader delivers it on a UC Volume
- **`cloudFiles.inferColumnTypes`**: JSON and CSV infer as **all strings** by default — set it to `true` for real types (or override selectively with `schemaHints`)
- **`schemaLocation`**: where inferred schemas persist, and why inference doesn't work without it
- **`mergeSchema`**: evolution has two halves — `schemaEvolutionMode` evolves the *source* schema, `mergeSchema` lets the *sink* table grow with it; forget the second and the restart dies with `DELTA_METADATA_MISMATCH`
- **`trigger(availableNow=True)`**: streaming correctness with batch economics — the default choice for scheduled jobs and for Free Edition quotas
- The **fail → evolve → restart** dance of `addNewColumns`, and why the one-time failure is a feature
- **`_rescued_data`**: type mismatches don't fail the stream, they get rescued with full provenance
- The **four evolution modes** and the exam's favorite failure-semantics questions
- The **checkpoint** as the source of exactly-once, and "new checkpoint" as the reprocessing answer
- **Directory listing vs file notification**, and when each discovery mode makes sense

If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
