# Change data capture three ways: Change Data Feed, MERGE and AUTO CDC

The previous post closed the series on orchestration, and that was meant to be the end of it. But every conversation I've had since about the **Data Engineer Professional exam** has circled back to the same gap: change data capture. It's the one topic where three different mechanisms have overlapping names, point in opposite directions, and get mixed up in exactly the way exam questions are designed to exploit.

So: a coda. One source of changes, three ways to deal with it — **Change Data Feed** to read changes *out* of a Delta table, **`MERGE`** to apply changes *into* one by hand, and **AUTO CDC** to apply them declaratively with SCD Type 1 or Type 2 for free. All of it runs on **Databricks Free Edition**: the first two mechanisms are plain SQL in a notebook or the SQL editor, the third needs a pipeline — and Free Edition allows one active pipeline per type, so you may have to stop the one from an earlier post before running this one.

---

## The mental model: which way does the arrow point?

Before any code, fix this picture, because it answers most of the exam questions on its own:

> **Change Data Feed** is an *output*: a Delta table telling you what changed inside it.
> **`MERGE`** and **AUTO CDC** are *inputs*: a stream of changes being applied to a target table.

They chain naturally — CDF on a silver table feeds a `MERGE` into gold — but they are not alternatives to each other, and a question that treats them as interchangeable is testing whether you know the direction of the arrow. Where the three genuinely compete is in *applying* changes, and there the split is `MERGE` (imperative, works anywhere, you own every edge case) versus AUTO CDC (declarative, pipelines only, the edge cases are handled for you).

---

## Setup: a landing table of changes

Our source is what a CDC tool like Debezium or Fivetran would hand you: one row per change, with an operation and a monotonically increasing sequence number.

```sql
CREATE SCHEMA IF NOT EXISTS tutorial.cdc;

CREATE OR REPLACE TABLE tutorial.cdc.customers_changes (
  customer_id INT,
  name        STRING,
  city        STRING,
  op          STRING,   -- INSERT | UPDATE | DELETE
  seq         BIGINT    -- the ordering column; never NULL
);

INSERT INTO tutorial.cdc.customers_changes VALUES
  (1, 'Ana',   'Zaragoza', 'INSERT', 1),
  (2, 'Bruno', 'Madrid',   'INSERT', 2),
  (3, 'Carla', 'Vigo',     'INSERT', 3),
  (1, 'Ana',   'Barcelona','UPDATE', 5),   -- Ana moves
  (1, 'Ana',   'Bilbao',   'UPDATE', 4),   -- arrives late, out of order
  (2, NULL,    NULL,       'DELETE', 6);   -- Bruno is gone
```

Two deliberate landmines in there: **several changes for the same key in one batch**, and **a late row with a lower sequence than one already applied**. Both are the heart of the exam's CDC questions, and both are silently wrong if you handle them naively.

---

## Mechanism 1: Change Data Feed

CDF makes a Delta table report its own history of row-level changes. It's off by default on classic Delta tables and switched on with a table property, at creation time or with an `ALTER TABLE`:

```sql
-- CDF only says something interesting about a table that gets mutated,
-- so the demo gets its own table (see the note below):
CREATE OR REPLACE TABLE tutorial.cdc.customers_current (
  customer_id INT, name STRING, city STRING
) TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- on a table that already exists, the same property arrives this way:
-- ALTER TABLE tutorial.cdc.customers_current
--   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

INSERT INTO tutorial.cdc.customers_current VALUES
  (1, 'Ana', 'Zaragoza'), (2, 'Bruno', 'Madrid'), (3, 'Carla', 'Vigo');  -- v1
UPDATE tutorial.cdc.customers_current SET city = 'Oviedo' WHERE customer_id = 3;  -- v2
DELETE FROM tutorial.cdc.customers_current WHERE customer_id = 2;                 -- v3
```

Note what we did *not* touch: the landing table. `customers_changes` has to stay **append-only**, because the AUTO CDC section streams from it and a Delta streaming source that has had an `UPDATE` or `DELETE` applied fails with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. That's not a quirk of the demo — a CDC landing table is an append-only log in production too.

Now read the feed:

```sql
SELECT customer_id, city, _change_type, _commit_version, _commit_timestamp
FROM table_changes('tutorial.cdc.customers_current', 1)   -- from version 1 to latest
ORDER BY _commit_version;
```

Six rows out of three statements: three `insert`s, an `update_preimage` / `update_postimage` pair for Carla, and a `delete` for Bruno.

Three metadata columns come back, and the exam cares about all three:

- **`_change_type`** — one of exactly four values: `insert`, `update_preimage`, `update_postimage`, `delete`. An `UPDATE` produces **two** rows: the before image and the after image. If a question asks why a CDF query returns more rows than the number of changed records, this is why.
- **`_commit_version`** — the table version that contained the change. This is your watermark for incremental processing.
- **`_commit_timestamp`** — when that commit happened.

The bounds of `table_changes(table, start[, end])` are **inclusive**, and both can be versions or timestamp strings. A batch read *requires* a starting point — unlike the streaming read below, there is no "give me everything" default.

### The three CDF traps

**1. Enabling CDF is not retroactive.** Change data exists only for commits made *after* the property was set. Flip it on today and the history from last week is simply not available as a feed — you can still time-travel to those versions, but you cannot diff them row by row. If the exam says "CDF was enabled at version 12, a query asks for changes from version 5", the answer is an error, not data.

**2. Change data is transient.** The feed is bounded by the table's retention: once a version falls out of the transaction log, or `VACUUM` removes the files behind it, its change data is gone. CDF is a mechanism for *incremental propagation*, not an audit log. If you need permanent history, the pattern is to write the feed into a table of your own — which is, not coincidentally, what SCD Type 2 does.

**3. Non-additive schema changes break reads across the boundary.** A rename, a drop or a type change makes a CDF query spanning that version fail. Split the read into before and after.

### Reading the feed as a stream

The incremental version, which is what you'd actually run in production:

```python
changes = (spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .table("tutorial.cdc.customers_current"))
```

On Free Edition the write side of that stream must use `trigger(availableNow=True)` — serverless compute rejects infinite triggers (`processingTime`, or no trigger at all), which is the same constraint the streaming post ran into.

One behaviour worth internalizing: **without `startingVersion`, a new stream first emits the entire current snapshot as `insert` rows**, then follows with changes. That's usually what you want when bootstrapping a downstream table and badly not what you want when the target is already populated — hence `startingVersion`. And resuming from an explicit version requires a *new* checkpoint location; reusing the old one means the checkpoint wins and your option is ignored.

> Newer runtimes on Unity Catalog also offer **automatic CDF** (in Public Preview), where changes are computed at read time from row tracking with no table property at all. So depending on the runtime behind your workspace, `table_changes()` may already work on a table where you never set the property. Worth knowing it exists; the property-based mechanism above is still the answer to "how do you enable change data feed", and the two can't be used together on the same table.

---

## Mechanism 2: `MERGE` by hand

Now the other direction: applying those changes to a silver table. Start from a target:

```sql
CREATE OR REPLACE TABLE tutorial.cdc.customers_silver (
  customer_id INT, name STRING, city STRING, seq BIGINT
);
INSERT INTO tutorial.cdc.customers_silver VALUES (1, 'Ana', 'Zaragoza', 1);
```

The instinct is to merge the change table straight in. Try it and you get:

```
Cannot perform Merge as multiple source rows matched and attempted to modify
the same target row in the Delta table...
```

That's landmine one. `MERGE` requires **at most one source row per target row**, and our batch has three changes for customer 1. The fix is to collapse each key to its latest change *before* merging — and "latest" means by sequence, not by arrival:

```sql
MERGE INTO tutorial.cdc.customers_silver AS t
USING (
  SELECT customer_id, name, city, op, seq FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY seq DESC) AS rn
    FROM tutorial.cdc.customers_changes
  ) WHERE rn = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.seq > t.seq AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.seq > t.seq                     THEN UPDATE SET
  t.name = s.name, t.city = s.city, t.seq = s.seq
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN
  INSERT (customer_id, name, city, seq)
  VALUES (s.customer_id, s.name, s.city, s.seq);
```

The column lists are spelled out on purpose. `UPDATE SET *` and `INSERT *` are the tempting shorthand, and here they would even run: they need **every target column** in the source (a missing one fails the analysis), but an extra source column like `op` is silently ignored — as long as schema evolution is off. Switch to `MERGE WITH SCHEMA EVOLUTION INTO ...` and the same `*` adds `op` to the target. `UPDATE SET * EXCEPT (op)` and `INSERT * EXCEPT (op)` are the middle ground: shorthand that can't leak the operation column into silver, evolution or not.

Four decisions in that statement, each one an exam point:

1. **The dedup window** is mandatory, not an optimization. One row per key per batch.
2. **`WHEN MATCHED` clauses are evaluated in order, first match wins** — so the `DELETE` arm goes above the `UPDATE` arm (below it, a newer delete would hit the update arm first and null out the row instead of removing it), and only the last clause may omit its condition.
3. **`s.seq > t.seq` is the out-of-order guard** — on both matched arms, because a stale delete is as wrong as a stale update — and it's the one worth being precise about. *Within* this batch, Ana's stale sequence-4 row was already discarded by the dedup window — `ORDER BY seq DESC` picked sequence 5. The guard earns its keep **across batches**: when sequence 4 shows up in tomorrow's file, after sequence 5 is already in silver, it's the only thing standing between you and Ana moving back to Bilbao. Prove it to yourself, once the `MERGE` above has run:

```sql
-- tomorrow's file: just the late row, merged on its own
MERGE INTO tutorial.cdc.customers_silver AS t
USING (
  SELECT * FROM VALUES (1, 'Ana', 'Bilbao', 'UPDATE', 4)
    AS late(customer_id, name, city, op, seq)
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.seq > t.seq AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.seq > t.seq                     THEN UPDATE SET
  t.name = s.name, t.city = s.city, t.seq = s.seq
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN
  INSERT (customer_id, name, city, seq)
  VALUES (s.customer_id, s.name, s.city, s.seq);
-- 4 > 5 is false: Ana stays in Barcelona. Drop the guard and she moves back to Bilbao.
```

The late row deliberately doesn't go through `customers_changes`: re-running the original `MERGE` over the whole landing table would let the dedup window pick sequence 5 again, and the guard would never be tested.

4. **`WHEN NOT MATCHED AND s.op <> 'DELETE'`** stops a delete for a row you never had from being inserted as a ghost record.

Run it and check: Ana is in Barcelona, Bruno is gone, Carla is there. Then run the exact same `MERGE` again — nothing changes. That's the quiet payoff: a keyed, sequence-guarded `MERGE` is **idempotent**, which is what makes it the correct sink inside `foreachBatch` (the streaming post) and what makes a replayed batch harmless.

---

## Mechanism 3: AUTO CDC in Lakeflow pipelines

Everything above is roughly thirty lines of SQL and four edge cases you had to know about. Inside a Lakeflow Declarative Pipeline, the same job is a declaration:

```sql
CREATE OR REFRESH STREAMING TABLE customers_scd1;

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO customers_scd1
FROM stream(tutorial.cdc.customers_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq)
STORED AS SCD TYPE 1;
```

Or in Python:

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

dp.create_streaming_table("customers_scd1")

dp.create_auto_cdc_flow(
    target="customers_scd1",
    source="tutorial.cdc.customers_changes",
    keys=["customer_id"],
    sequence_by=col("seq"),
    apply_as_deletes=expr("op = 'DELETE'"),
    except_column_list=["op", "seq"],
    stored_as_scd_type=1,
)
```

`SEQUENCE BY` does the dedup *and* the out-of-order protection: records whose sequence is below what's already been applied are dropped, so Ana's late sequence-4 update dies on its own. No window function, no clause ordering, no ghost-record guard.

**A naming note that matters in the exam room:** these APIs were renamed. `APPLY CHANGES INTO` became **`AUTO CDC INTO`**, and `dlt.apply_changes()` became **`create_auto_cdc_flow()`**, with identical syntax. The old names still work, so expect to see either spelling in questions and docs — recognize both, write the new ones.

### SCD Type 2: history for one keyword

Change one clause and the target stops being a current-state table and starts being a history:

```sql
CREATE OR REFRESH STREAMING TABLE customers_scd2;

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO customers_scd2
FROM stream(tutorial.cdc.customers_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq)
STORED AS SCD TYPE 2;
```

What you get in the target:

- Two generated columns, **`__START_AT`** and **`__END_AT`**, holding the sequence values where each version became active and stopped being active. **`__END_AT IS NULL` means "this is the current version"** — that's the predicate for querying current state out of an SCD2 table, and it's a favourite exam detail.
- Ana now has three rows: Zaragoza from 1 to 4, Bilbao from 4 to 5, Barcelona from 5 and still open. Look at the middle one: the late sequence-4 update that SCD Type 1 dropped is *kept* here, slotted into history by its sequence, not its arrival order. Same data, different answer to "what happens to late records" — exactly the kind of contrast the exam likes.
- **A delete closes the active version instead of removing history.** Bruno's row gets an `__END_AT` and stays. If a question asks how to keep deleted records queryable, that's SCD Type 2, not a soft-delete flag you invent.
- **`TRACK HISTORY ON * EXCEPT (...)`**, added after `STORED AS SCD TYPE 2`, restricts which columns create a new version — a change to an excluded column updates the current row in place. Add `TRACK HISTORY ON * EXCEPT (city)` to the flow above and Ana collapses back to a single row, Barcelona, open since sequence 1: in our data only the city ever changes, so there's no history left to keep. Without the clause, every changed column spawns history, which is how SCD2 tables quietly grow ten times larger than anyone expected.

In Python the equivalents are `stored_as_scd_type=2` and `track_history_except_column_list=["city"]`. If you declare an explicit schema for an SCD2 target, it must include `__START_AT` and `__END_AT` with the same type as `sequence_by`.

### The limits worth knowing

- The **target must be a streaming table**, declared before the flow.
- The flow needs **serverless pipelines or the Pro/Advanced edition** — Free Edition pipelines are serverless, so this runs; open-source Spark Declarative Pipelines don't have it.
- The **source must be a streaming source** (`FROM stream(...)`), which is why the landing table stayed append-only back in the CDF section.
- The **sequencing column must be sortable and non-NULL**. For composite ordering, wrap columns in a `STRUCT`: it sorts by the first field and breaks ties with the next.
- An AUTO CDC target is **not append-only**, so you can't `readStream` from it directly — you read its **change data feed**. The three mechanisms closing the loop.
- If your source gives you **full snapshots instead of a change log**, there's `AUTO CDC FROM SNAPSHOT` (`create_auto_cdc_from_snapshot_flow` in Python), which diffs consecutive snapshots for you and skips any snapshot whose version isn't newer than the last committed one.

---

## Choosing, in one paragraph

If you need to know **what changed in a Delta table**, it's Change Data Feed — and there is no competitor, only the question of batch or stream. If you need to **apply changes to a target**, ask whether you're inside a declarative pipeline: if yes, AUTO CDC, because SCD Type 1 and 2 arrive for free and the out-of-order and duplicate-key cases are someone else's problem. If no — a job, a notebook, a `foreachBatch` sink, a target that isn't a streaming table — it's `MERGE`, and you owe the target a dedup window, ordered `WHEN MATCHED` clauses, a sequence guard and a non-matched delete filter. The exam's phrasing tells you which world you're in: "in a Lakeflow pipeline" points at AUTO CDC; "in a `foreachBatch`" or "idempotent writes" points at `MERGE`.

---

## Recap

- **Change Data Feed** reads changes *out*: `delta.enableChangeDataFeed`, `table_changes(t, start[, end])` with inclusive bounds, four `_change_type` values with updates emitting pre- *and* post-images. Not retroactive, bounded by retention and `VACUUM`, and broken across non-additive schema changes.
- A **streaming CDF read** emits the full snapshot as inserts on first start unless you pin `startingVersion` — and changing that option needs a fresh checkpoint.
- **`MERGE`** applies changes *in*, correctly only with four pieces in place: dedup to one row per key, `DELETE` arm before `UPDATE` arm, `s.seq > t.seq` out-of-order guard on both, and no insert on an unmatched delete. Get them right and the statement is idempotent, which is what `foreachBatch` needs.
- **AUTO CDC** (formerly `APPLY CHANGES`) does the same declaratively: `KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, `COLUMNS * EXCEPT`, `STORED AS SCD TYPE 1 | 2`.
- **SCD Type 2** gives you `__START_AT` / `__END_AT` (current version = `__END_AT IS NULL`), closes rather than removes deleted records, slots late records into history instead of dropping them, and should almost always carry a `TRACK HISTORY ON * EXCEPT` list.

That's the gap filled. If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
