# Fine-grained governance with Unity Catalog: row filters, column masks and dynamic views

Nobody gets excited about governance until the day HR salaries end up in a dashboard that the whole company can see.

It's also a topic the **Data Engineer Professional exam** hides everywhere: there's no dedicated governance section — instead, you get governance traps buried inside questions that look like they're testing something else entirely.

The three things you need to be able to tell apart — fast, under exam pressure — are **row filters**, **column masks**, and **dynamic views**. They're similar enough that it's easy to mix them up, which is exactly why the exam loves them.

One thing that helps: **Databricks Free Edition includes a full Unity Catalog metastore**, so you can actually build and test all three with real SQL. And even as a single user, you can simulate different access levels, since the governance functions check *group membership* — and you control what groups you belong to.

---

## Setup: a table that clearly needs protecting

Salary data is the classic example for a reason. Let's build one:

```sql
CREATE SCHEMA IF NOT EXISTS tutorial.ventas;

CREATE OR REPLACE TABLE tutorial.ventas.salaries (
  employee_id INT,
  full_name STRING,
  dni STRING,          -- Spanish national ID: definitely PII
  department STRING,
  salary DOUBLE
);

INSERT INTO tutorial.ventas.salaries VALUES
  (1, 'Ana L.',    '12345678A', 'data',    52000),
  (2, 'Brais S.',  '23456789B', 'data',    48000),
  (3, 'Carla R.',   '34567890C', 'finance', 61000),
  (4, 'Diego P.',  '45678901D', 'finance', 55000);
```

Names, national IDs, salaries, departments. The goal: HR admins see everything, each department only sees its own rows, and the `dni` column stays masked for anyone who doesn't need it.

---

## Mechanism #1: column masks — hide PII unless you're privileged

A **column mask** is a SQL UDF attached to a specific column. Every time someone queries the table, the column values pass through that function, and what comes out depends on who's asking. The key thing here: everyone queries **the same table, with the same name**. There's no alternate object or alias to remember.

First, the function:

```sql
CREATE OR REPLACE FUNCTION tutorial.ventas.mask_dni(dni STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('hr_admins') THEN dni
  ELSE concat('*****', right(dni, 3))
END;
```

Then attach it to the column:

```sql
ALTER TABLE tutorial.ventas.salaries
  ALTER COLUMN dni SET MASK tutorial.ventas.mask_dni;
```

From here, querying the table works as normal:

```sql
SELECT * FROM tutorial.ventas.salaries;
-- Not in hr_admins? You get *****78A instead of 12345678A
```

![alt](assets/img/03/03-masked-column.png)


I kept the last three characters instead of fully redacting the value. That's a deliberate choice — support teams often need just enough to verify identity ("can you confirm the ID ending in 78A?"). Full redaction is also valid, it just depends on what your policy requires. The mask function is plain SQL, so you can do whatever makes sense.

The `is_account_group_member('group_name')` function does most of the work throughout this post: it returns true if the current user belongs to that account-level group. There's also `current_user()` for per-user rules, but group-based policies hold up much better over time than lists of hardcoded email addresses.

---

## Mechanism #2: row filters — you only see your department

A **row filter** does the same thing for rows instead of column values: it's a boolean SQL UDF evaluated against each row, silently dropping the ones that return false.

```sql
CREATE OR REPLACE FUNCTION tutorial.ventas.dept_filter(department STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('finance_team') AND department = 'finance'
    OR is_account_group_member('data_team')    AND department = 'data'
    OR is_account_group_member('hr_admins');   -- sees everything

ALTER TABLE tutorial.ventas.salaries
  SET ROW FILTER tutorial.ventas.dept_filter ON (department);
```

The `ON (department)` part maps table columns to the function's parameters. The filter can reference as many columns as it needs.

Now the same `SELECT * FROM tutorial.ventas.salaries` returns different results depending on who runs it. A finance analyst sees Carla and Diego. A data engineer sees Ana and Brais. No error, no permission warning — the other rows just aren't there. That silence is intentional (you can't expose what doesn't appear to exist), but it's worth keeping in mind when someone files a bug saying the table is missing data.

### Testing both sides as a single user

Here's how to make this work on Free Edition without multiple accounts: go to **admin settings → Identity and access → Groups**, create `hr_admins`, `data_team`, and `finance_team`, then add or remove yourself. Re-run the query after each change and you'll see rows appear and disappear, and the mask flip on and off.

![alt](assets/img/03/03-groups.png)

Here's what it looks like when the data_team group is assigned to the query result:

![alt](assets/img/03/03-row-filter.png)

Before you start swapping group memberships, though, make sure you know how to remove the policies:

```sql
ALTER TABLE tutorial.ventas.salaries DROP ROW FILTER;
ALTER TABLE tutorial.ventas.salaries ALTER COLUMN dni DROP MASK;
```

It's genuinely easy to filter yourself out of your own demo table and end up staring at an empty result set.

---

## Mechanism #3: dynamic views — the older approach, still on the exam

Before row filters and column masks existed (they're both fairly recent additions), the same result was achieved with **dynamic views**: a view with the access logic embedded directly in its SQL.

```sql
CREATE OR REPLACE VIEW tutorial.ventas.salaries_secure AS
SELECT
  employee_id,
  full_name,
  CASE WHEN is_account_group_member('hr_admins')
       THEN dni ELSE 'REDACTED' END AS dni,
  department,
  salary
FROM tutorial.ventas.salaries
WHERE is_account_group_member('hr_admins')
   OR (is_account_group_member('finance_team') AND department = 'finance')
   OR (is_account_group_member('data_team')    AND department = 'data');
```

The result is the same. Where it diverges from filters and masks — and what the exam actually cares about — is the structure:

- The view is a **separate object** with a **different name**. Users have to query `salaries_secure`, not `salaries`.
- The view doesn't protect the base table at all. If users still have `SELECT` on `salaries`, they go around the view entirely. You have to **`REVOKE` access to the base table manually** and grant `SELECT` on the view in its place.
- On the upside, a view can do things filters and masks can't: joins, aggregations, column renames. Filters and masks can only hide parts of the existing table shape.

---

## How to tell them apart

| | Row filter / column mask | Dynamic view |
|---|---|---|
| Applied to | The table itself | A separate object |
| Users query | The real table name | The view name |
| Direct table access | Still governed | Must be revoked manually |
| Granularity | Per-column / per-row, composable | Whatever SQL you can write |
| Exam keyword | *"without creating additional objects"* | *"users must not access the base table"* |

The trap question comes in two flavors. First version: *"analysts must query the same table name but see masked values"* — that's filters or masks; a view would change the name. Second version: *"all direct access to the base table must be denied"* — that's a view plus `REVOKE`; filters and masks keep the base table queryable, just governed.

Those two keywords are the ones worth memorizing.

---

## Bonus: the GRANT model

None of this works if the privilege chain underneath is broken. Unity Catalog uses a hierarchical model — catalog → schema → table — and a grant at the bottom is useless without access at the levels above it:

```sql
GRANT USE CATALOG ON CATALOG tutorial TO `data_team`;
GRANT USE SCHEMA  ON SCHEMA tutorial.ventas TO `data_team`;
GRANT SELECT      ON TABLE tutorial.ventas.salaries TO `data_team`;
```

Two things worth remembering:

1. **`SELECT` without `USE CATALOG` and `USE SCHEMA` does nothing.** The user gets a permission error and files a ticket saying "you told me they had SELECT" — which is true, but they can't navigate to the table in the first place. The exam loves this one.
2. **Grants on a catalog or schema cascade down**, including to objects created in the future. `GRANT SELECT ON SCHEMA tutorial.ventas TO data_team` covers every table in that schema, including ones that don't exist yet. Very convenient, and also an easy way to accidentally over-share, so be deliberate about it.

---

## Recap

What we covered:

- **Column masks**: a SQL UDF attached to a column with `SET MASK` — same table name, different values per user
- **Row filters**: a boolean UDF attached with `SET ROW FILTER` — rows disappear silently based on group membership
- **Dynamic views**: the pre-filters approach — full SQL flexibility, but a separate object, and the base table needs manual lockdown
- **The decision keywords**: "same table name" → filters/masks; "deny base table access" → dynamic view + REVOKE
- **The GRANT hierarchy**: `USE CATALOG` → `USE SCHEMA` → `SELECT`, with grants cascading downward
- How to test everything on Free Edition by adding and removing yourself from groups

A few things on my list for future posts:

- **Attribute-based access control with tags**: policies that travel with classified columns instead of being bound to individual tables
- **Lineage in Catalog Explorer**: tracking who reads what, automatically
- **Auto Loader and schema evolution**: back to the pipeline side of things

If anything here was unclear or you want me to go deeper on a specific topic, feel free to reach out on LinkedIn.
