# Data quality as code: expectations in Lakeflow Declarative Pipelines

Every data team has lived this story. Quality checks start as a couple of asserts in a notebook. Six months later they're a folder of validation utilities that three pipelines import, two pipelines copy-pasted an old version of, and one pipeline silently stopped calling after a refactor. Nobody knows which rules actually run anymore. Quality checks scattered across notebooks **rot**, and they rot fast.

Lakeflow Declarative Pipelines takes a different stance: quality rules are **declared on the table, next to the table**, as *expectations*. The framework enforces them on every run, counts every violation, and exposes the metrics without you building anything. We already used expectations briefly in the first post of this series; today we go deep — the three escalation levels, where the metrics live, the quarantine pattern, and the difference between expectations and constraints (a contrast the **Data Engineer Professional exam** is fond of).

As usual, everything here runs on **Databricks Free Edition**.

---

## The three escalation levels

An expectation is a name plus a SQL boolean condition, attached to a table with a decorator. What makes the system expressive is that *you choose the consequence* of a violation, and there are exactly three:

| Decorator | Invalid rows | Pipeline |
|---|---|---|
| `@dp.expect(...)` | **Kept**, counted in metrics | Continues |
| `@dp.expect_or_drop(...)` | **Dropped**, counted | Continues |
| `@dp.expect_or_fail(...)` | First one **aborts the update** | Fails, transaction rolled back |

If you memorize a single table from this post, make it this one. It's the highest-value nine cells in the whole exam domain. Read it as an escalation ladder: *observe* → *filter* → *refuse*.

Note the detail in the third row: `expect_or_fail` doesn't just stop the pipeline — the failing update is **rolled back**. The target table is left as it was before the run. No half-written batch to clean up.

---

## A pipeline that uses all three

Let's wire the ladder into a real pipeline. We'll reuse the bronze table from the Auto Loader post as our messy source (it has nulls and rescued garbage — perfect), but any imperfect table works:

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(name="orders_raw")
def orders_raw():
    return spark.read.table("tutorial.ventas.orders_bronze")

# Silver: measure some problems, refuse others
@dp.table(name="orders_silver")
@dp.expect("amount_present", "amount IS NOT NULL")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND order_id > 0")
def orders_silver():
    return spark.read.table("LIVE.orders_raw")

# Gold: garbage here should be impossible — if it isn't, stop everything
@dp.table(name="orders_gold")
@dp.expect_or_fail("positive_amount", "amount >= 0")
def orders_gold():
    return (spark.read.table("LIVE.orders_silver")
                 .where(col("amount").isNotNull()))
```

Look at how the severity climbs with the layer, because that's the design principle hiding in this example:

- In **silver**, a null `amount` is *expected* dirt — we keep the row and count it (`expect`). A null or non-positive `order_id`, though, makes the row unusable, so we drop it (`expect_or_drop`).
- In **gold**, a negative amount shouldn't be able to exist — silver already filtered the nulls, and amounts come from a source that can't produce negatives. If one shows up anyway, something upstream is *broken*, and the right move is to fail loudly before a dashboard reads it (`expect_or_fail`).

When you have several rules at the same level, there's a grouped form — and it accepts a plain dict, which matters for the pattern coming later:

```python
@dp.expect_all_or_drop({
    "valid_id": "order_id IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount >= 0",
})
```

(`expect_all` and `expect_all_or_fail` exist too, same idea. The exam occasionally name-drops these, so don't let the `_all_` variant throw you.)

One Free Edition note before you hit *Start*: the free tier allows **one active pipeline per type**, so rather than creating a fresh pipeline for this demo, add this file to the pipeline you already have from the earlier posts. The tables coexist happily.

---

## The same thing in SQL, because the exam asks both

Everything above has a SQL twin, and it's worth seeing once because the exam switches languages without warning. In SQL pipelines, expectations are `CONSTRAINT` clauses with an `ON VIOLATION` action:

```sql
CREATE OR REFRESH MATERIALIZED VIEW orders_silver (
  CONSTRAINT amount_present  EXPECT (amount IS NOT NULL),
  CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL AND order_id > 0)
                             ON VIOLATION DROP ROW
)
AS SELECT * FROM LIVE.orders_raw;
```

The mapping is mechanical once you've seen it:

| Python | SQL |
|---|---|
| `@dp.expect(...)` | `EXPECT (...)` — no `ON VIOLATION` clause |
| `@dp.expect_or_drop(...)` | `EXPECT (...) ON VIOLATION DROP ROW` |
| `@dp.expect_or_fail(...)` | `EXPECT (...) ON VIOLATION FAIL UPDATE` |

The subtle detail — and therefore the examinable one — is that the *default* (no `ON VIOLATION`) is the warn-and-keep behavior, not the drop. If a question shows a bare `EXPECT` constraint and asks what happens to invalid rows, the answer is: they land in the table, counted but kept.

---

## Where the metrics live (and the canonical exam answer)

Run the pipeline and click any table in the DAG: the panel shows each expectation with its pass/fail counts for that update. Nice for eyeballing. But the UI is not the real product — the real product is the **pipeline event log**, a queryable Delta table where every expectation result is recorded structurally:

```sql
SELECT
  details:flow_progress:data_quality:expectations
FROM event_log(TABLE(tutorial.ventas.orders_silver))
WHERE event_type = 'flow_progress'
  AND details:flow_progress:data_quality IS NOT NULL;
```

(Adjust the catalog/schema to wherever your pipeline publishes its tables.)

Each row gives you, per flow and per update: expectation name, dataset, `passed_records`, `failed_records`. It's JSON in a `details` column, hence the `:` path syntax.

This matters for the exam because *"how do you monitor data quality over time in a DLT pipeline?"* has one canonical answer: **query the event log**. Not "write counts to a side table from inside the pipeline", not "export the UI numbers" — the event log already is the history, it's already a Delta table, and it's already queryable from the Free Edition SQL warehouse. From there a dashboard is twenty minutes of work, and you can attach a SQL alert to the underlying query — *if `failed_records > 0`, email me* — and you've built quality monitoring without a single external framework.

---

## The quarantine pattern: don't just drop, route

Here's the limitation of the three levels: expectations can keep, drop, or fail — but they can't **route**. `expect_or_drop` throws bad rows away, and "away" means gone. In real pipelines you usually want to *look* at what was dropped: debug the upstream, recover what's fixable, or just have evidence when someone asks where their order went.

The standard solution is elegant: a twin table fed from the same source, with every rule **negated**.

```python
RULES = {"valid_amount": "amount IS NOT NULL AND amount >= 0"}

@dp.table(name="orders_quarantine")
@dp.expect_all_or_drop({k: f"NOT ({v})" for k, v in RULES.items()})
def orders_quarantine():
    return spark.read.table("LIVE.orders_raw")
```

Read it twice if you need to — it's a small mind-bender the first time. The quarantine table *drops everything that passes* the rules, so what survives is exactly what silver rejected. Same source, inverted predicate, zero rows lost between the two tables. And because `RULES` is a shared dict (define it once, use it in both tables), silver and quarantine can never drift out of sync. This is why the dict-based `expect_all_or_drop` form earns its place.

---

## Two pieces of judgment the exam (and your on-call rotation) will test

**Fail-fast is for the impossible, not the inconvenient.** `expect_or_fail` belongs on conditions that indicate a *broken invariant*: negative primary keys, duplicate natural keys after deduplication, a fact table referencing a dimension that can't be missing. It does not belong on "this field is sometimes null" — that's row-level noise, and `expect` or `expect_or_drop` handles it while the pipeline keeps serving everyone else. Overuse `expect_or_fail` and you've built a machine that converts every upstream hiccup into a 3 a.m. page. Your future self will not thank you.

**Expectations are not constraints.** Delta tables have `CHECK` constraints too (`ALTER TABLE t ADD CONSTRAINT c CHECK (amount >= 0)`), and the exam likes contrasting them:

| | Expectation | Delta `CHECK` constraint |
|---|---|---|
| Enforced for | Writes made by *this pipeline* | **All** writers, any engine |
| On violation | Your choice: keep / drop / fail | Write is rejected, always |
| Metrics | Counted in the event log | None — just an error |

A constraint is a property of the *table*; an expectation is a property of the *pipeline*. If a rogue notebook inserts directly into your gold table, expectations won't even notice — a `CHECK` constraint will reject the write. They compose: expectations for graduated, observable quality inside the pipeline, constraints as the table's last line of defense.

---

## Recap and next steps

What we covered today:

- The **three escalation levels** — `expect` (keep + count), `expect_or_drop` (drop + count), `expect_or_fail` (abort + roll back) — and reading them as observe → filter → refuse
- Matching severity to layer: tolerate and measure in silver, fail-fast on impossible states in gold
- **`expect_all_*`** grouped variants, driven by a rules dict
- The **event log** as the canonical, queryable home of quality metrics — and the canonical exam answer for quality monitoring
- The **quarantine pattern**: a twin table with negated rules, catching exactly what silver dropped
- **Expectations vs `CHECK` constraints**: pipeline-scoped and graduated vs table-scoped and absolute

The pipeline now defends its own quality and documents the defense. What's left is everything *around* the pipeline: how this code gets versioned, deployed to dev and prod without click-ops, and wired into CI/CD. That's Databricks Asset Bundles — the next post.

If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
