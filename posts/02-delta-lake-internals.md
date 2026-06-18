# Delta Lake internals you can poke at: the transaction log, deletion vectors, liquid clustering and time travel

Everyone who works with Databricks can recite the sales pitch: "Delta Lake gives you ACID transactions on top of Parquet". Fine. But if you're preparing the **Data Engineer Professional exam** — or if you've ever had to debug a table that mysteriously doubled in size — that one-liner won't save you. The exam assumes you understand what Delta does *under the hood*: what a commit actually writes, why a `DELETE` can finish in two seconds on a huge table, and what `VACUUM` quietly destroys.

The good news: every single internal mechanism is observable with plain SQL. No filesystem spelunking, no cloud storage console. In this post we'll create one table and dissect four internals on it — the transaction log, deletion vectors, liquid clustering, and time travel — and as always, everything runs on **Databricks Free Edition**.

---

## The mental model: a Delta table is files plus a diary

Before touching anything, get this picture in your head, because everything else in this post hangs off it:

> **A Delta table = a bunch of Parquet data files + a transaction log that says which files belong to which version.**

The log lives in a `_delta_log/` directory next to the data. Every commit — an `INSERT`, a `MERGE`, an `OPTIMIZE` — appends one JSON file to that directory describing what changed: which data files were added, which were logically removed, and some metadata about the operation. Readers reconstruct the current state of the table by replaying the log from the beginning.

"Replaying from the beginning" sounds expensive, and it would be — so every 10 commits Delta writes a **checkpoint** (a Parquet summary of the log so far), and readers start from the latest checkpoint instead of from zero.

Two consequences fall out of this design, and both show up on the exam:

1. **Writers never modify data files in place.** They write new files and update the log. That's how Delta gets snapshot isolation without locks on the data.
2. **"Deleting" data doesn't delete files.** The log just stops referencing them. The files stay on storage until something explicitly cleans them up (that something is `VACUUM`, and we'll get to it).

---

## Setup: one table, deliberately interesting

Let's create a table with enough rows for the internals to be visible, clustered by city (note the `CLUSTER BY` — that's liquid clustering, not partitioning, and the distinction matters later):

```sql
CREATE SCHEMA IF NOT EXISTS tutorial.delta_internals;

CREATE OR REPLACE TABLE tutorial.delta_internals.trips (
  trip_id BIGINT,
  city STRING,
  fare DOUBLE,
  ts TIMESTAMP
)
CLUSTER BY (city);   -- liquid clustering, not partitioning

INSERT INTO tutorial.delta_internals.trips
SELECT
  id,
  element_at(array('Zaragoza','Madrid','Vigo','Sevilla'), CAST(rand()*4 AS INT) + 1),
  round(rand()*50, 2),
  current_timestamp()
FROM range(1000000000);
```

1.000.000.000 fictional taxi trips across four Spanish cities, with random fares. That's our patient for the rest of the post.

---

## Internal #1: the transaction log is just JSON

You don't need filesystem access to inspect the log — `DESCRIBE HISTORY` exposes it as a table:

```sql
DESCRIBE HISTORY tutorial.delta_internals.trips;
```

You'll see two rows so far: version 0 (`CREATE OR REPLACE TABLE`) and version 1 (`WRITE`, our insert). Each row is one log entry, and the columns worth staring at are:

- **`operation`**: what kind of commit it was — `CREATE OR REPLACE`, `WRITE`, `MERGE`, `DELETE`, `OPTIMIZE`, `RESTORE`...
- **`operationMetrics`**: a map of counters. For our `WRITE` you'll see things like `numFiles` (how many Parquet files the insert produced) and `numOutputRows` (1.000.000.000).
- **`engineInfo`**: which engine wrote the commit. Useful in real life when three different tools write to the same table and you're trying to figure out who did what.

Get into the habit of running `DESCRIBE HISTORY` after every operation in this post. The exam loves questions of the form *"after running X, what appears in the table history?"* — and the cheapest way to internalize the answers is to have seen them with your own eyes rather than memorized them from a list.

One subtlety while we're here: the history shows *logical* operations, one per commit. A single `MERGE` that touches a million rows is still one row in the history, with the damage summarized in `operationMetrics`.

---

## Internal #2: deletion vectors, or how DELETE got fast

Time to delete something. Let's get rid of suspiciously cheap trips:

```sql
DELETE FROM tutorial.delta_internals.trips WHERE fare < 1.0;

DESCRIBE HISTORY tutorial.delta_internals.trips;
```

Look at the `operationMetrics` of the `DELETE` commit. Two fields tell the whole story:

- `numDeletionVectorsAdded` is **greater than zero**
- `numRemovedFiles` is **zero** (or tiny)

Here's why that's remarkable. Remember: Delta never modifies files in place. So under the original design (**copy-on-write**), deleting 50 rows from a 100 MB Parquet file meant rewriting the *entire file* without those 50 rows. Deleting a handful of rows scattered across a big table could trigger gigabytes of rewrites. Painful, slow, expensive.

**Deletion vectors** flip the strategy to **merge-on-read**. Instead of rewriting the file, Delta writes a small bitmap alongside it that says "rows 1.041, 7.230 and 88.412 of this file are dead". Readers load the file, consult the bitmap, and skip the dead rows. The `DELETE` itself becomes nearly instant regardless of file sizes.

Nothing is free, of course: the cost moved from write time to read time. Every read of that file now does a little extra work filtering out tombstoned rows. The physical cleanup happens later, in one of two ways:

```sql
-- Option 1: OPTIMIZE compacts files and materializes deletion vectors as it goes
OPTIMIZE tutorial.delta_internals.trips;

-- Option 2: explicitly purge deletion vectors without waiting for a regular OPTIMIZE
REORG TABLE tutorial.delta_internals.trips APPLY (PURGE);
```

The exam angle, condensed: deletion vectors make `DELETE`, `UPDATE` and `MERGE` faster (merge-on-read), at the price of read-time work, and `REORG TABLE ... APPLY (PURGE)` is the command that physically rewrites the affected files. If a question mentions "remove deletion vectors" or "rewrite files containing soft-deleted rows", that's the answer they're fishing for.

---

## Internal #3: OPTIMIZE and liquid clustering

If you ran the `OPTIMIZE` above, check the history again:

```sql
DESCRIBE HISTORY tutorial.delta_internals.trips;
-- operation = OPTIMIZE → compare numRemovedFiles vs numAddedFiles
```

`OPTIMIZE` compacts many small files into fewer large ones — that part everybody knows. But because our table was created with `CLUSTER BY (city)`, it does something more interesting: it **clusters** the data, physically co-locating rows for the same city in the same files. The point of that is **file pruning**: when a query filters by `city`, the engine can skip entire files whose min/max statistics show they can't contain matching rows.

This is a good moment to put liquid clustering in context, because it replaced two older techniques and the exam expects you to know *why*:

| Technique | How it works | The problem |
|---|---|---|
| **Hive-style partitioning** (`PARTITION BY`) | One physical directory per key value | Disaster with high-cardinality keys (millions of tiny files); the key is frozen forever |
| **Z-ordering** (`OPTIMIZE ... ZORDER BY`) | Multi-dimensional clustering during OPTIMIZE | Must be re-specified on every OPTIMIZE; rewrites data aggressively even when little changed |
| **Liquid clustering** (`CLUSTER BY`) | Declared once on the table, applied incrementally | This is the current recommendation |

The two killer features of liquid clustering: it's **incremental** (OPTIMIZE only rewrites what needs rewriting, not the whole table), and the clustering key is **changeable** — `ALTER TABLE trips CLUSTER BY (city, ts)` and you're done, no table rebuild. Try doing that with Hive partitioning.

Don't take the pruning on faith — verify it:

```sql
SELECT count(*) FROM tutorial.delta_internals.trips WHERE city = 'Zaragoza';
```

Run that from the SQL Editor and open the **query profile** (available on the Free Edition SQL warehouse). Look for "files pruned" vs "files read". If clustering did its job, most files were never opened. Seeing that number with your own eyes is worth more than any diagram.

![alt](assets/img/02/02-file-prune.png)

### Seeing it on disk: the same data in a Volume

The pruning numbers are convincing, but they're still an abstraction. Wouldn't it be better to *see* the actual files clustering produces, one per city? Normally you can't: a Unity Catalog **managed table** keeps its Parquet files in internal storage you have no path-based access to. Run `DESCRIBE DETAIL` and you'll get a `location`, but you can't `LIST` it.

There's exactly one place in Unity Catalog you *can* browse files with plain `LIST` (and the Catalog Explorer UI): a **Volume**. You can't register a metastore table inside a Volume, but you *are* allowed path-based Delta access there. So let's drop a clustered copy of our table into a Volume and peek under the hood.

First, a Volume to hold the files:

```sql
CREATE VOLUME IF NOT EXISTS tutorial.delta_internals.files;
```

Now write a copy of `trips` as a path-based Delta table, clustered by `city`. Pure SQL `CLUSTER BY` needs a registered table (which Volumes don't allow), so this one step uses the DataFrame writer — `clusterBy` is the API equivalent. We also force a deliberately tiny target file size, otherwise our 1.000.000.000-row dataset compacts into a single file and there's nothing to see:

```python
path = "/Volumes/tutorial/delta_internals/files/trips"

(spark.read.table("tutorial.delta_internals.trips")
    .write.format("delta")
    .clusterBy("city")
    .option("delta.targetFileSize", "512kb")   # tiny on purpose, to force a split
    .save(path))
```

That tiny `delta.targetFileSize` did its job at write time — the data is now spread across many files. But `OPTIMIZE` honors the *same* property, so if we leave it at 512 KB the compaction step would dutifully keep producing tiny files instead of consolidating each city. So before clustering, bump the target back up to a sane size:

```python
spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.targetFileSize' = '128mb')")
```


Then cluster it for real with `OPTIMIZE` — remember, liquid clustering is applied incrementally by `OPTIMIZE`, not at write time:

```sql
OPTIMIZE delta.`/Volumes/tutorial/delta_internals/files/trips`;
```

Note the ``delta.`...path...` `` syntax — that's how you address a path-based Delta table when there's no name in the metastore.

Now the fun part. List the directory:

```sql
LIST '/Volumes/tutorial/delta_internals/files/trips';
```

You'll see a `_delta_log/` directory — the JSON diary from Internal #1, now sitting in plain sight — and a pile of `.parquet` data files. Heads up: you'll likely see *more* Parquet files than are currently live. The pre-`OPTIMIZE` files are still physically there, dereferenced but not deleted, exactly as Internal #4 is about to explain. `LIST` shows everything on disk; it has no idea which files the table currently points at.

So to see only the **live** files — and which city each one holds — let the Delta reader do the subtraction for you. It only ever returns files the table currently points at, so just read the table, tag each row with the file it came from, and collapse by file:

```python
from pyspark.sql.functions import col, min, max, count

(spark.read.format("delta").load(path)
    .groupBy(col("_metadata.file_path").alias("file"))
    .agg(count("*").alias("rows"),
         min("city").alias("min_city"),
         max("city").alias("max_city"))
    .orderBy("min_city")
    .show(truncate=False))
```

The payoff: each live file has `min_city == max_city`. Madrid's rows are in their own files, Zaragoza's in others — liquid clustering physically co-located each city. And that min/max range is *exactly* what the engine uses to prune: Delta stores the same per-file min/max in the log, so when you filter `WHERE city = 'Zaragoza'`, every file whose `[min_city, max_city]` can't contain `'Zaragoza'` is skipped without ever being opened. You're now looking at the raw material behind the "files pruned" number from the query profile.

(Exact file counts depend on data size. If you still get a single file, lower `delta.targetFileSize` further or bump the row count in the setup insert.)

Want the diary entry itself? Just read one — it's only JSON (version 1 here is the `OPTIMIZE` commit):

```sql
SELECT * FROM json.`/Volumes/tutorial/delta_internals/files/trips/_delta_log/00000000000000000001.json`;
```

Every `add` action carries the same `path` and `stats` you just queried — this is the primary source that `DESCRIBE HISTORY` and the query planner read from.

---

## Internal #4: time travel, RESTORE, and what VACUUM really does

Remember the consequence of the log-based design: deleted data files aren't deleted, just dereferenced. That's what makes **time travel** possible:

```sql
-- How many trips did we have before the DELETE?
SELECT count(*) FROM tutorial.delta_internals.trips VERSION AS OF 1;

-- You can also travel by timestamp
SELECT count(*) FROM tutorial.delta_internals.trips TIMESTAMP AS OF '2026-06-12T10:00:00';
```

Reading an old version just means replaying the log up to that commit instead of to the end. And if reading isn't enough — say someone ran a catastrophic `UPDATE` in production — you can roll the whole table back:

```sql
RESTORE TABLE tutorial.delta_internals.trips TO VERSION AS OF 1;
```

Check the history afterwards: `RESTORE` doesn't rewrite data either. It's just another commit that points the table back at the old set of files. Cheap and instant.

### So when does data actually die?

Enter `VACUUM`. It physically deletes data files that are no longer referenced by any table version within the **retention window** — 7 days by default:

```sql
VACUUM tutorial.delta_internals.trips;          -- removes unreferenced files older than 7 days
VACUUM tutorial.delta_internals.trips DRY RUN;  -- shows what would be removed, deletes nothing
```

Two classic exam traps live here, and both have bitten real people in real jobs:

1. **VACUUM breaks time travel beyond the retention window.** If you `VACUUM` with the default retention and then try `RESTORE TABLE ... TO VERSION AS OF` a two-week-old version, it fails — the files that version needs are physically gone. The log entry still exists; the data doesn't. Time travel is bounded by retention, full stop.
2. **Lowering retention below 7 days requires disabling a safety check** (`spark.databricks.delta.retentionDurationCheck.enabled`). That check exists because vacuuming files that a long-running query or stream is still reading corrupts the read. And here's a detail worth knowing: on serverless compute — which is all you have on Free Edition — you can't set arbitrary Spark configs anyway. The platform protecting you from yourself is, in this case, a feature.

---

## Recap and next steps

One table, four internals, all observed with plain SQL:

- **The transaction log**: a Delta table is Parquet files plus a JSON diary; readers replay the log (from checkpoints, every 10 commits) to reconstruct state. `DESCRIBE HISTORY` is your window into it.
- **Deletion vectors**: `DELETE`/`UPDATE`/`MERGE` write small tombstone bitmaps (merge-on-read) instead of rewriting files (copy-on-write); `REORG TABLE ... APPLY (PURGE)` materializes them.
- **Liquid clustering**: `CLUSTER BY` declared once, applied incrementally by `OPTIMIZE`, key changeable later — the modern replacement for Hive partitioning and Z-ordering. Drop a copy into a **Volume** and you can `LIST` the actual files and read their `_delta_log` stats to watch each city land in its own file.
- **Time travel and VACUUM**: old versions are readable and restorable because files are dereferenced, not deleted — until `VACUUM` deletes anything outside the retention window.

Every `operationMetrics` field you inspected here is fair game for the Professional exam, and now you've seen them generated live instead of in a slide.

Natural follow-ups from here:

- **Governance on top of these tables**: row filters, column masks and dynamic views with Unity Catalog — that's the next post in this series
- **CDC with AUTO CDC**: now that you know what a MERGE commit looks like in the log, watch Lakeflow generate them for you
- **OPTIMIZE scheduling and predictive optimization**: when to compact, and when to let Databricks decide

If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
