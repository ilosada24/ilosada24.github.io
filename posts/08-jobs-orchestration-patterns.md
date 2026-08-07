# Orchestration patterns in Lakeflow Jobs: task values, conditionals, repair runs and file triggers

The last pillar of the **Data Engineer Professional exam** — and of this series — is orchestration. Not the trivial version ("can you schedule a notebook?": yes, everyone can), but the questions that decide whether a multi-task job survives contact with production: how do tasks **communicate**? How does a job **branch**? When task 7 of 10 dies at 4 a.m., what's the recovery that doesn't re-run six hours of work? And can a job trigger itself when files land, instead of polling on a dumb schedule?

Every pattern in this post is demoable on **Databricks Free Edition**, which allows up to 5 concurrent job tasks — enough for all of it.

---

## Task values: passing data between tasks

First fact to internalize: tasks in a job run in **isolated contexts**. Each notebook task gets its own fresh execution environment; a variable defined in task one simply does not exist in task two. So how does the ingestion task tell the transform task how many rows arrived?

The official channel is **task values** — a small key-value store scoped to the job run:

```python
# Task "ingest" — at the end of the notebook
new_rows = spark.table("tutorial.ventas.orders_bronze").count()
dbutils.jobs.taskValues.set(key="row_count", value=new_rows)
```

```python
# Task "transform" — downstream
n = dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count",
                                default=0, debugValue=0)
```

Note that `get` names the *task* it reads from, not just the key — values are namespaced per task, so two tasks can both publish a `row_count` without colliding.

Two details here are classic exam material:

1. **Task values are for signals, not for data.** They must be JSON-serializable and small — think flags, counts, a date string, a status. If you catch yourself wanting to pass a DataFrame between tasks, the answer is: write it to a Delta table and pass the *table name*. The job graph moves metadata; the lakehouse moves data.
2. **`debugValue` is what makes the notebook runnable outside a job.** Run that `get` interactively — no job context, no upstream task — and instead of throwing, it returns the `debugValue`. Without it, you can't develop the notebook standalone. Small parameter, disproportionate exam presence.

---

## Conditional branching: If/else tasks

With a value published, the job can make decisions. The scenario every pipeline eventually needs: *if* ingestion brought new rows, transform and publish; *if not*, skip the heavy work and just log it. That's an **If/else condition task** between the two:

```
ingest ──► check_rows ──(true)──► transform ──► publish
                      └─(false)─► notify_no_data
```

The condition task references the task value directly, with the `{{...}}` templating syntax (this is the YAML shape; the UI builds the same thing with dropdowns):

```yaml
- task_key: check_rows
  depends_on: [{ task_key: ingest }]
  condition_task:
    op: GREATER_THAN
    left: "{{tasks.ingest.values.row_count}}"
    right: "0"
- task_key: transform
  depends_on: [{ task_key: check_rows, outcome: "true" }]
```

The `outcome: "true"` on the dependency is what binds `transform` to the *true* branch; a sibling task with `outcome: "false"` forms the other arm. Tasks on the untaken branch show as *excluded* in the run — not failed, not succeeded, just not applicable.

### Run-if: the other branching knob

The exam pairs conditionals with **`Run if` dependencies**, which govern what a task does when its *upstreams* misbehave. The default is `ALL_SUCCESS` — one upstream failure and everything downstream is skipped. The other values exist for the exceptions:

- A **cleanup** task that must run no matter what happened above it → `ALL_DONE`
- A **notification** task that fires when something (anything) failed → `AT_LEAST_ONE_FAILED`
- A join point that proceeds if its parallel upstreams didn't fail, even if some were skipped → `NONE_FAILED`

If a question says "the temp-table cleanup must execute even when the job fails", it's not asking about try/finally in your notebook — it's asking for `ALL_DONE`.

---

## Repair runs: the recovery question

Here's the scenario the exam loves, because the wrong answer is so tempting. A 10-task job ran for three hours and failed at task 7. The fix is a one-line parameter change. What do you do?

The tempting answer — run the whole job again — is wrong, and not just because it wastes three hours. The right answer is **Repair run**: re-execute *only the failed tasks and everything downstream of them*, reusing the results of the tasks that succeeded — including, crucially, their **task values**. Tasks 1–6 don't re-run; their outputs are still there; the run record stays a single run rather than two half-runs in your history.

Repair also accepts **new parameters**, which closes the loop on the most common failure of all: the job failed because someone passed a bad date. Repair with the corrected date; the successful upstream tasks are reused, the failed branch re-runs with the fix.

It's worth doing this once with your own hands so the UI is familiar. Make a task fail behind a parameter flag:

```python
if dbutils.widgets.get("should_fail") == "true":
    raise Exception("boom")
```

Run the job with `should_fail=true`, watch it fail, then hit **Repair run**, flip the parameter, and watch tasks 1–6 show as reused while only the broken branch executes.

### Retries vs repair

Same word-cloud, different tools, and the exam checks you know which is which. **Retries** are per-task, automatic, configured ahead of time (count + backoff) — they exist for *transient* failures, the flaky API that works on the second attempt. **Repair** is run-level, manual, after the fact — for failures that needed a human to change something. The design judgment: put retries on tasks where flakiness is expected and re-execution is cheap (API ingestion); leave them off tasks where re-running is expensive or where a failure means something is genuinely wrong and a person should look before anything re-executes.

---

## Job parameters vs task values: who sets what

One more communication channel, easy to confuse with task values because they share the templating syntax. **Job parameters** are key-value pairs defined on the *job* and pushed down to *every* task; they're how a human (or a repair run, or an API call) injects configuration from outside:

```yaml
parameters:
  - name: run_date
    default: "{{job.start_time.iso_date}}"
```

Inside any task they arrive as widgets (`dbutils.widgets.get("run_date")`), and in YAML they're referenced as `{{job.parameters.run_date}}`. The contrast worth keeping crisp:

- **Job parameters** flow *top-down*: set once at trigger time, visible to all tasks, overridable per run. Configuration.
- **Task values** flow *sideways*: produced by one task at runtime, read by downstream tasks. Computed signals.

The date a backfill should process is a job parameter; the row count the ingest task just observed is a task value. If a question describes "re-running the job for a different date", that's job parameters (possibly combined with a repair run); if it describes "a downstream task reacting to what an upstream task discovered", that's task values. Mixing these up is a cheap way to lose an easy point.

Also in the same family: dynamic value references like `{{job.start_time.iso_date}}` above, which the platform fills in at run time — handy for date-partitioned ingestion without any code computing "today".

---

## Triggers beyond cron

Scheduled jobs are fine until the data doesn't respect your schedule. Files land at unpredictable times; a cron job either runs uselessly (nothing arrived) or late (the file sat there for 40 minutes). The trigger built for this is **file arrival**:

```yaml
trigger:
  file_arrival:
    url: /Volumes/tutorial/ventas/landing/
```

Databricks polls the Volume (roughly every minute) and fires the job when new files appear. Now connect this with the Auto Loader post and admire the complete machine: **files land → trigger fires → the job runs an Auto Loader stream with `trigger(availableNow=True)` → the stream drains exactly the new files and stops → the job ends.** Near-real-time behavior, batch-level cost — on Free Edition's daily quota, this is precisely the architecture you want instead of an always-on stream.

The full trigger menu, for completeness:

- **Scheduled**: cron with a timezone. Know that a **paused** schedule still permits manual runs — pausing stops the clock, not the job.
- **File arrival**: as above, on a Volume or external location.
- **Continuous**: the job restarts whenever it completes — the wrapper for always-on streaming jobs, with built-in restart-on-failure.

---

## Concurrency and idempotency: the closing argument

One setting ties this post to the entire series: `max_concurrent_runs`. The default is **1**, and that default is load-bearing — if a run is still going when the next trigger fires, the new run queues or skips rather than executing alongside it. Two simultaneous runs of an ingestion job is how you double-process data.

But the deeper answer to *"ensure two runs never process the same data twice"* — and that's the exam phrasing to recognize — is layered, and we've already built every layer in this series: concurrency control at the job level (`max_concurrent_runs: 1`), exactly-once file tracking at the ingestion level (Auto Loader's checkpoint), and **idempotent writes** at the sink level (keyed `MERGE` in `foreachBatch`, from the streaming post). When all three hold, even a misbehaving trigger or a late manual run converges to the same correct state. That's what "production-grade" actually means: not that nothing ever goes wrong, but that the system is safe to re-run.

---

## Recap — and a series recap

Today's patterns:

- **Task values** for small signals between isolated tasks, with `debugValue` keeping notebooks runnable standalone
- **If/else condition tasks** branching on `{{tasks.x.values.y}}`, plus **Run if** rules (`ALL_DONE` for cleanups, `AT_LEAST_ONE_FAILED` for alerts)
- **Repair runs** that reuse successful tasks (and their values), optionally with corrected parameters — versus automatic per-task **retries** for transient flakiness
- **File arrival triggers** pairing with `availableNow` streams for event-driven, quota-friendly ingestion
- **`max_concurrent_runs: 1`** plus idempotent writes as the overlapping-runs safety net

If you've followed along from the first post: thank you, and good luck in the exam room. If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
