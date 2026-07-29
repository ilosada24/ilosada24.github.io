# From notebook chaos to CI/CD: Databricks Asset Bundles end to end

Let's be honest about what we've built so far in this series: pipelines and jobs clicked together in the UI, pointing at notebooks that live in a workspace folder. It works, it's great for learning — and it's exactly the setup that falls apart the day you need a second environment. How do you promote that pipeline to prod? Screenshot the settings and re-click them? Who changed the schedule last Tuesday? Nobody knows. The UI doesn't do code review.

**Databricks Asset Bundles (DABs)** are the official answer: your jobs and pipelines defined as YAML next to your source code, deployed per environment with a CLI, and wired into Git CI/CD like any other software. The **Data Engineer Professional exam** expects you to know them, and any team beyond one person needs them.

And yes — **Databricks Free Edition works fine as a deployment target**. The CLI authenticates against your free workspace like any other, so you can follow the whole post without a paid account.

---

## Bootstrap: CLI, auth, and a skeleton

Three commands take you from nothing to a working bundle. On your local machine (not in a notebook — this is the part of the story that happens on *your* laptop, in a terminal, in Git):

```bash
# Install the Databricks CLI (v0.2xx+, the new Go-based CLI —
# not the old Python one from pip, that one doesn't know about bundles)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Authenticate against your workspace (opens a browser for OAuth)
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# Generate a starter project
databricks bundle init default-python
```

The `init` wizard asks a few questions and generates this skeleton:

```
my_project/
├── databricks.yml          # bundle definition + targets
├── resources/
│   └── my_project.job.yml  # job definition
└── src/
    └── notebook.ipynb
```

That's the whole shape of a bundle: one `databricks.yml` at the root declaring *what this bundle is and where it deploys*, a `resources/` folder of YAML declaring *jobs and pipelines*, and your actual source code. All of it goes in Git.

---

## The databricks.yml that matters

Here's a trimmed-down version with everything load-bearing left in:

```yaml
bundle:
  name: blog_pipelines

variables:
  catalog:
    default: tutorial

targets:
  dev:
    mode: development        # <- the key concept
    default: true
    workspace:
      host: https://<workspace>.cloud.databricks.com

  prod:
    mode: production
    workspace:
      host: https://<workspace>.cloud.databricks.com
      root_path: /Workspace/Shared/.bundle/${bundle.name}/prod
```

A **target** is a named deployment destination — typically `dev`, `staging`, `prod` — and each can point at a different workspace, override variables, and most importantly declare a **mode**. That `mode` line is the single most examined DAB concept, so let's give it the space it deserves.

### `mode: development` vs `mode: production`

**`mode: development`** optimizes for iteration and for *not stepping on your teammates*:

- Every deployed resource gets prefixed with `[dev yourname]` — so when five engineers deploy the same bundle to a shared dev workspace, they get five independent copies of every job and pipeline, no collisions.
- Schedules and triggers are deployed **paused**. Your half-finished experiment will not start running nightly on its own.
- Concurrent runs are allowed, and everything lands under your user folder.

**`mode: production`** flips every one of those defaults: real names with no prefixes, schedules live from the moment you deploy, a shared deployment path, and stricter validation — it will, for example, complain if the job runs as your personal identity instead of a service principal, because prod things owned by a person's account stop working the day that person leaves.

The exam phrasing to recognize: *"multiple engineers share one workspace and must deploy without conflicts"* → `mode: development` and its prefixing. *"Why didn't my dev job run on schedule?"* → it deployed paused, on purpose.

---

## Declaring a pipeline and a job as code

Now the payoff: the pipeline and job we built across this series, as two YAML files in `resources/`. First the Lakeflow pipeline:

```yaml
# resources/cdc.pipeline.yml
resources:
  pipelines:
    cdc_pipeline:
      name: cdc_pipeline
      catalog: ${var.catalog}
      schema: ventas_cdc
      serverless: true
      libraries:
        - file:
            path: ../src/cdc_pipeline.py
```

Then a daily job that runs an ingestion notebook and triggers the pipeline:

```yaml
# resources/daily.job.yml
resources:
  jobs:
    daily_refresh:
      name: daily_refresh
      trigger:
        periodic: { interval: 1, unit: DAYS }
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ../src/ingest_autoloader.ipynb
        - task_key: cdc
          depends_on: [{ task_key: ingest }]
          pipeline_task:
            pipeline_id: ${resources.pipelines.cdc_pipeline.id}
```

Stop at that last line, because it's the quiet superpower of bundles: `${resources.pipelines.cdc_pipeline.id}`. The job needs the pipeline's ID — but the pipeline doesn't *have* an ID until it's deployed, and it has a *different* ID in dev and in prod. The cross-reference resolves at deploy time to whatever pipeline **this bundle deployed in this target**. No hardcoded IDs, no "update the prod config after deploying", no dev job accidentally triggering the prod pipeline. The bundle is self-contained.

Same story with `${var.catalog}`: define a variable once, override it per target (`tutorial` in dev, `prod_catalog` in prod), and the YAML never forks.

---

## The four commands

The entire lifecycle is four verbs:

```bash
databricks bundle validate          # schema + interpolation check, deploys nothing
databricks bundle deploy -t dev     # upload code, create/update resources
databricks bundle run daily_refresh -t dev
databricks bundle destroy -t dev    # tear down everything the bundle owns
```

`validate` catches YAML typos and broken `${...}` references before anything touches the workspace — it's the command that belongs in CI on every pull request.

`deploy` is **declarative**, and this word carries exam weight. It doesn't "run a script of creation steps"; it diffs your YAML against the workspace state and converges the workspace to match. Run it twice, nothing changes the second time. Rename a job in YAML and redeploy: the old one is replaced, not duplicated. And — the trap to know — if someone edits a bundle-owned job **in the UI**, the next `deploy` steamrolls their edit back to whatever the YAML says. The UI even shows a warning banner on bundle-managed resources for exactly this reason. The YAML in Git is the source of truth; everything else is a cache.

---

## Adopting what you already built: bundle generate

"Great, but I already have jobs and pipelines built in the UI from the earlier posts — do I rewrite them as YAML by hand?" No. The CLI can export an existing resource into bundle format:

```bash
# Find the job ID in the UI (or via: databricks jobs list)
databricks bundle generate job --existing-job-id 123456789
```

This drops a ready-made `resources/<job_name>.job.yml` into your bundle, plus downloads the referenced notebooks into `src/`. From that moment, the YAML is the source of truth and the UI version becomes bundle-managed on the next deploy. It's the standard migration path for teams with a workspace full of click-built jobs: generate, review the YAML (it's verbose — prune the defaults), commit, deploy.

The everyday development loop then settles into a rhythm that feels a lot like any other software project:

```bash
# edit code or YAML, then:
databricks bundle deploy -t dev      # seconds, incremental
databricks bundle run daily_refresh -t dev
databricks bundle summary            # what's deployed, with workspace URLs
```

`summary` is the underrated one — it prints every resource the bundle owns in the current target with direct links, which beats hunting through the workspace UI for your `[dev yourname]`-prefixed copies.

---

## CI/CD: GitHub Actions in fifteen lines

Once deployment is a CLI command, CI/CD stops being a project and becomes a workflow file:

```yaml
name: deploy-prod
on:
  push: { branches: [main] }
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate
      - run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

Merge to `main` → validate → deploy to prod. Add a second workflow on pull requests that runs only `validate` (and your tests), and you have the standard promotion story: every change reviewed, every deploy reproducible, every environment's state recoverable from Git history.

One honest caveat for Free Edition: there are no service principals on the free tier, so a realistic *prod* target with its `run_as` requirements isn't fully demoable — practice the dev flow end to end, which is identical. On a paid workspace, the one thing you'd change is authentication: prod deploys should run as a **service principal**, never as somebody's personal token.

---

## Exam checklist

Condensing the post into the bullets the exam draws from:

- The four commands — `validate`, `deploy`, `run`, `destroy` — and what each one touches
- `mode: development` semantics: `[dev yourname]` prefixes, **paused schedules**, per-user isolation in a shared workspace
- `mode: production` semantics: real names, live schedules, service-principal expectations
- Variables and interpolation: `${var.x}`, `${bundle.name}`, and resource cross-references like `${resources.pipelines.x.id}` that keep environments self-contained
- `deploy` is declarative: it converges workspace state to the YAML, and UI edits to bundle-owned resources get overwritten
- The one-line answer to *"how do you promote a job from dev to prod reproducibly?"* — **bundles deploy code and resource definitions together**, per target, from Git

---

## Recap and next steps

What we covered today:

- Why UI-built jobs stop scaling the moment a second environment (or second engineer) appears
- Bootstrapping a bundle: the Go CLI, OAuth login, `bundle init`
- `databricks.yml` anatomy: bundle, variables, **targets**, and the all-important `mode`
- Pipelines and jobs as YAML in `resources/`, glued by deploy-time cross-references
- The declarative `deploy` model and the Git-as-source-of-truth discipline that comes with it
- A complete GitHub Actions promotion flow, Free Edition caveats included

The bundle now ships our work. The remaining piece of the puzzle is what happens *inside* a job at runtime: how tasks pass values to each other, branch conditionally, recover from mid-run failures, and trigger on file arrivals. That's orchestration patterns in Lakeflow Jobs — the next and final post of the series.

If you have questions or want to see any of these topics in more detail, feel free to reach out on LinkedIn.
