# Structured Streaming for the Professional exam: watermarks, triggers and idempotent foreachBatch

If you ask people who've taken the **Data Engineer Professional exam** where they lost points, streaming comes up again and again. Not because the API is hard — you can learn `readStream`/`writeStream` in an afternoon — but because the questions are about *semantics*: what state Spark keeps in memory, when it decides to drop late data, which output mode emits what, and how to write to multiple tables without quietly breaking exactly-once.

The good news is that all of it is demoable on **Databricks Free Edition** with zero infrastructure. No Kafka, no event hub — Spark ships with a synthetic `rate` source that generates timestamped rows out of thin air, and that's all we need to watch every one of these semantics fire in real time.

---

## A synthetic event stream

The `rate` source emits rows with a `timestamp` and an incrementing `value`. Let's dress it up as something resembling user events:

```python
from pyspark.sql.functions import col, expr, window

events = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withColumn("user_id", (col("value") % 5).cast("int"))
    .withColumn("event_time", col("timestamp")))
```

Twenty events per second, five fictional users, each event carrying its own event time. Cheap, infinite, and perfect for poking at streaming behavior.

---

## Watermarks: the question of state

Here's the problem watermarks exist to solve. Say we count events per user in 30-second windows. Events can arrive late — a phone was offline, a network hiccupped — so when can Spark consider the window `[12:00:00, 12:00:30)` *finished*? Without an answer to that question, the honest thing to do is keep **every window ever opened** in memory, forever, just in case a straggler shows up. That's exactly what a streaming aggregation without a watermark does, and it's why state grows unboundedly until the job falls over.

A **watermark** is you telling Spark how late your data can reasonably be:

```python
agg = (events
    .withWatermark("event_time", "1 minute")
    .groupBy(window("event_time", "30 seconds"), "user_id")
    .count())

(agg.writeStream
    .option("checkpointLocation", "/Volumes/tutorial/ventas/landing/_chk/agg")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("tutorial.ventas.user_counts"))
```

"My data can be up to 1 minute late." With that promise, Spark can finalize windows and throw their state away.

Two practical notes before you run it. First, the trigger: on Free Edition you don't get a choice — serverless compute only accepts `availableNow`, and a `processingTime` trigger dies immediately with `[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED]` (the triggers section below has the full story). Second, `availableNow` turns this demo into a run-again loop: the checkpoint remembers when the stream started, and each run of the cell drains the events the `rate` source "generated" during the wall-clock time since the previous run. So the workflow is: run the cell, wait a couple of minutes, run it again, and query `tutorial.ventas.user_counts`.

Expect the table to be **empty after the first run**. That's not a bug — in `append` mode a window is held back until the watermark passes its end, so nothing can be emitted until at least a window's worth plus the 1-minute threshold of event time has accumulated. That delay is the semantics in action, and it's worth understanding precisely.

The exam triplet you must keep straight:

**1. The watermark is computed from data, not from the clock.** It's `max(event_time seen so far) − threshold`. If no data arrives, the watermark doesn't move. A question that implies the watermark advances with wall-clock time is testing exactly this misconception.

**2. Output mode decides when results are emitted.** With **`append`**, a window's result is emitted only *after* the watermark passes the window's end — the result is final, but late (that's the delay you observed: window end + 1 minute threshold before anything shows up). With **`update`**, partial results are emitted every batch as they change. And **`complete`** re-emits the entire result table every batch and keeps all state, which only makes sense for small aggregates. One more rule the exam checks: a streaming aggregation in `append` mode *requires* a watermark — without one, no window would ever be "final", and Spark refuses to start the query.

**3. Data later than the watermark is dropped silently.** No error, no metric screaming at you, no `_rescued_data` like Auto Loader gives you. The event simply never counts. This is the classic trap question: *"an event arrived 5 minutes late with a 1-minute watermark — what happens?"* Answer: nothing happens, and that's the point. It's gone. If your business can't tolerate that, your watermark threshold is a business decision, not a tuning parameter.

---

## Triggers: how often the micro-batches fire

Orthogonal to all of the above is *when* Spark processes data:

| Trigger | Behavior | Use case |
|---|---|---|
| `processingTime="10 seconds"` | A micro-batch every 10s | Continuous pipelines with latency targets |
| `availableNow=True` | Drain everything pending, then stop | Scheduled incremental jobs |
| *(default, no trigger)* | Micro-batches back-to-back, as fast as possible | When you want minimum latency and accept the cost |

The distinction underneath that table is **finite vs. infinite**. `processingTime` and the default both define an *infinite* query: the cluster stays up indefinitely, and the trigger only sets the cadence. A longer `processingTime` interval means fewer, larger, cheaper micro-batches at the cost of latency; the default fires the next batch the instant the previous one finishes, for minimum latency at maximum cost. What the interval does **not** change is the semantics — watermarks, output modes and state behave identically at any cadence. `availableNow` is the odd one out: a *finite* query that processes everything available at start, commits the checkpoint, and exits.

That distinction is also why the watermark demo above couldn't use `processingTime`: **serverless compute — which is all Free Edition offers — rejects infinite triggers outright** with `[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED]`. On a classic cluster all three rows of the table work; on serverless, `availableNow` is the only trigger that runs. For the exam you still need `processingTime` cold — the questions assume classic compute — but for following along on Free Edition, every `writeStream` in this series uses `availableNow`.

Beyond being the only option on serverless, it's also the pattern that matters most in practice (we already used it in the Auto Loader post): **`availableNow`** gives you streaming semantics — incremental processing, checkpoints, exactly-once — inside a job that starts, drains the backlog, and shuts down. Run it on a schedule and you have an incremental batch pipeline with none of the "which files did I already process?" bookkeeping. Which is also the answer to the quota question: even on compute where a continuous stream *is* allowed, leaving one running unattended is the fastest possible way to burn through your budget.

---

## foreachBatch: when toTable isn't enough

`toTable` does one thing: append the stream to one table. Real pipelines outgrow that quickly — you need **upserts** (the same key arriving again should update, not duplicate), or you need to **fan out** one stream into several tables. The escape hatch for both is `foreachBatch`: you hand Spark a function, and it calls it once per micro-batch with the batch as a plain DataFrame. Inside that function, you're in batch land — `MERGE`, multiple writes, whatever you need.

Here's the upsert pattern, which is worth memorizing because it's the answer to half the streaming design questions out there:

```python
def upsert_to_gold(microdf, batch_id):
    microdf.createOrReplaceTempView("batch_updates")
    microdf.sparkSession.sql("""
        MERGE INTO tutorial.ventas.user_counts_gold t
        USING batch_updates s
        ON t.user_id = s.user_id AND t.window = s.window
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(agg.writeStream
    .foreachBatch(upsert_to_gold)
    .option("checkpointLocation", "/Volumes/tutorial/ventas/landing/_chk/gold")
    .outputMode("update")
    .trigger(availableNow=True)
    .start())
```

One prerequisite that's easy to miss: **the gold table must exist before the first batch runs** — `MERGE INTO` won't create it for you, and the failure surfaces as a cryptic error buried inside the foreachBatch worker. Clone the schema without copying any rows:

```sql
%sql
CREATE TABLE tutorial.ventas.user_counts_gold AS
SELECT * FROM tutorial.ventas.user_counts WHERE 1=0
```

Note the pairing: `update` output mode emits changing partial counts, and the `MERGE` absorbs each revision into the gold table. Windows get refined as data arrives; the gold table always holds the latest version of each.

Two things about how this function is written, both of which exist because of where it *runs*. On serverless compute, `foreachBatch` doesn't execute in your notebook — Spark pickles the function and runs it in a **separate Python process with a cloned session** (fail inside it and the error says so: `[root session: ...][cloned session: ...] Found error inside foreachBatch Python process`). Two consequences:

1. **Notebook globals like `spark` don't exist there.** Everything the function needs must come in through its arguments — hence `microdf.sparkSession` instead of `spark`.
2. **Use SQL `MERGE INTO`, not the `DeltaTable` Python API.** You'll see `DeltaTable.forName(...).merge(...)` in plenty of tutorials, and it works fine on classic compute — but inside a serverless `foreachBatch` it goes through the Spark Connect wrapper (`delta.connect`), which is exactly where it tends to blow up. The temp-view-plus-SQL pattern above is the one Databricks' own docs use, and it runs anywhere.

### The same upsert on classic compute — the version the exam shows

On a classic cluster (all-purpose or job compute, which is what every Professional exam question silently assumes), none of the serverless constraints apply: `foreachBatch` runs in the same process as your notebook, so the global `spark` is visible inside the function, and the `DeltaTable` Python API works without the Connect wrapper in between. That's why exam snippets look like this:

```python
from delta.tables import DeltaTable

def upsert_to_gold(microdf, batch_id):
    gold = DeltaTable.forName(spark, "tutorial.ventas.user_counts_gold")
    (gold.alias("t")
        .merge(microdf.alias("s"),
               "t.user_id = s.user_id AND t.window = s.window")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
```

It's the exact same MERGE as the SQL version — `whenMatchedUpdateAll()` is `WHEN MATCHED THEN UPDATE SET *`, `whenNotMatchedInsertAll()` is `WHEN NOT MATCHED THEN INSERT *` — just spelled through the fluent API. You need to read *both* spellings fluently, because the exam mixes them freely.

**How this shows up in the exam.** The `foreachBatch` + MERGE combo is one of the most reliably tested patterns in the streaming domain, and the questions cluster into a few recognizable shapes:

- **"A pipeline needs to upsert streaming data into a Delta table — what do you use?"** The answer is always `foreachBatch` with a MERGE. Distractors will offer `toTable` with `outputMode("update")` (doesn't exist — table sinks can't upsert), a plain `MERGE INTO` on a streaming DataFrame (MERGE needs a batch source), or DLT `APPLY CHANGES` (valid tech, but wrong when the question is about Structured Streaming code).
- **Fill-in-the-blank code.** A snippet like the one above with a blank at `.foreachBatch(_____)` or at the function signature. Two things to have memorized: the function receives **`(DataFrame, batch_id)`** — in that order, called once per micro-batch — and it's passed to `foreachBatch` **by reference, without parentheses** (`.foreachBatch(upsert_to_gold)`, not `.foreachBatch(upsert_to_gold())`).
- **"The job restarted and now the gold table has duplicates — why?"** The scenario describes a blind `append` inside `foreachBatch`. The answer hinges on at-least-once semantics (next section): the batch replayed, the append wasn't idempotent. The fix offered among the options is a keyed MERGE or the `txnAppId`/`txnVersion` pair.
- **"Which output mode pairs with this?"** For an aggregation feeding a MERGE, it's `update` — you want each revision of a window emitted so the MERGE can absorb it. `append` would delay results until the watermark closes the window; `complete` would re-emit everything and make the MERGE pointlessly rewrite the whole table.

The reading strategy: when a question shows `foreachBatch` code, don't parse it line by line looking for syntax bugs — first classify what the *inside* of the function does (blind append? keyed MERGE? multi-table writes?) and what the question is really probing (idempotency? signature? output mode?). Ninety percent of the time it's one of the four shapes above wearing a different table name.

### The subtle point: at-least-once, and why MERGE saves you

Now the detail that separates a pass from a fail on this topic. `foreachBatch` gives you **at-least-once** execution: if the job crashes after your function ran but before the checkpoint recorded the batch as done, the same batch **runs again** on restart. Spark guarantees the batch contents are identical; it does *not* guarantee your function ran only once.

So the overall pipeline is exactly-once **only if your function is idempotent** — running it twice with the same input must converge to the same state:

- A keyed **`MERGE`** is idempotent. Re-upserting the same rows matches instead of inserting. Run it five times; the table looks the same.
- A blind **`INSERT`/append** inside `foreachBatch` is *not*. A replayed batch means duplicated rows, and you'll be the one explaining the inflated revenue dashboard.

What if you genuinely need plain appends to several Delta tables? There's a dedicated pattern for that — make the write itself transactional per batch:

```python
def append_twice(microdf, batch_id):
    (microdf.write.format("delta")
        .option("txnVersion", batch_id).option("txnAppId", "fanout_demo")
        .mode("append").saveAsTable("tutorial.ventas.sink_a"))
    (microdf.write.format("delta")
        .option("txnVersion", batch_id).option("txnAppId", "fanout_demo")
        .mode("append").saveAsTable("tutorial.ventas.sink_b"))
```

`txnAppId` + `txnVersion` let Delta recognize "I already committed batch 17 from this application" and skip the duplicate write on replay. That option pair is a known exam answer — recognize it on sight.

---

## Reading a Delta table as a stream

One last semantic, because chained pipelines (bronze table → silver stream) hit it constantly. Streaming *from* a Delta table treats it as an append-only source. The moment someone runs an `UPDATE`, `DELETE` or `MERGE` on that source table, downstream streams **fail by default** — Spark refuses to guess what a changed commit means for the stream.

You have two flags to handle it, and the difference between them is worth at least one exam question:

```python
spark.readStream
  .option("skipChangeCommits", "true")   # ignore commits that update/delete rows
  .table("tutorial.ventas.orders_bronze")
```

- **`skipChangeCommits`** (the current, recommended flag): commits that modified existing rows are *skipped entirely*. The stream only processes pure appends. Clean semantics, no surprises.
- **`ignoreChanges`** (the legacy flag): instead of skipping, it *re-emits the rewritten files* — meaning every untouched row that happened to live in a rewritten file comes through **again**. Duplicates downstream, with no warning.

If a question describes "the stream emitted duplicate rows after an upstream UPDATE", that's `ignoreChanges` behavior. If it asks how to keep a stream alive over a source that gets occasional GDPR deletes — `skipChangeCommits`.

---

## Recap and next steps

What we covered today:

- **Watermarks** bound state and define lateness: `max event time − threshold`, advancing with data, never with the clock
- **Output modes**: `append` emits final results after the watermark closes a window (and requires a watermark for aggregations); `update` emits revisions; `complete` re-emits everything
- Late data beyond the watermark is **dropped silently** — the most-tested trap in the domain
- **Triggers**: `availableNow` for scheduled incremental drains — and the only trigger serverless (and therefore Free Edition) accepts; `processingTime` for continuous micro-batches on classic compute
- **`foreachBatch`** unlocks MERGE and multi-table writes, but is at-least-once — pipelines stay exactly-once only through idempotent writes (keyed `MERGE`, or `txnAppId`/`txnVersion` for appends)
- The upsert has two spellings — SQL `MERGE INTO` (works everywhere, including serverless) and the `DeltaTable` fluent API (what exam snippets show, assuming classic compute) — and the exam questions about it come in four recognizable shapes
- **`skipChangeCommits` vs `ignoreChanges`** when streaming from Delta tables that suffer updates or deletes

If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
