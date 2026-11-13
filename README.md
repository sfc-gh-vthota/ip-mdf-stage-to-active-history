# IP Metadata-Driven Framework: Stage → Active + History

A metadata-driven pattern for Snowflake that replaces concurrent `MERGE`/upsert writers with:

1. **Insert-only stage tables.** Writers (Lambdas, connectors, Snowpipe) only `INSERT`, so they never block each other on table locks.
2. **One generic SQL stored procedure** that, every 5 minutes, moves new stage rows into:
   - an **ACTIVE** table: one row per business key, latest version.
   - a **HISTORY** table: SCD Type 2, **one row per actual change**. If a customer address changes 5 times in a day, HISTORY gets 5 rows; a record that arrives again unchanged is ignored.
3. **A task graph (DAG)** that runs the load per table only when its stream has new data.

Everything is driven by one row per table in a mapping table. Onboarding a new table is a metadata insert, not new code.

```
 writers ──INSERT──▶ RAW_INSERT.<TABLE>  ──stream──▶ SP_FW_LOAD(mapping_id)   (one transaction)
                                                       ├─ MERGE  ▶ CORE.<TABLE>_ACTIVE
                                                       └─ SCD2   ▶ CORE.<TABLE>_HISTORY
                                                       audit     ▶ CONTROL.FW_RUN_AUDIT / FW_RUN_STEP_LOG
```

## Repository layout

| File | Purpose |
| --- | --- |
| `sql/01_setup.sql` | Database `IP_METADATA_FW`, schemas `RAW_INSERT` and `CONTROL`, warehouse `IP_MDF_LOAD_WH`, and 5 **sample** ServiceNow stage tables (INCIDENT, PROBLEM, CHANGE_REQUEST, SC_REQ_ITEM, SYS_USER). Replace these with your own stage tables. |
| `sql/02_mapping_table.sql` | Schema `CORE` and the mapping table `CONTROL.FW_TABLE_MAPPING`, with one row per sample table. |
| `sql/03_procedures.sql` | Audit tables and the procedures `SP_FW_ONBOARD`, `SP_FW_LOAD` and `SP_FW_LOAD_ALL` (Snowflake Scripting, `LANGUAGE SQL`). |
| `sql/04_onboard.sql` | Calls `SP_FW_ONBOARD` per mapping to create the ACTIVE, HISTORY and work tables and the stream. |
| `sql/05_task_dag.sql` | Task graph: root every 5 minutes, one child per table, plus a finalizer. |
| `sql/06_test_change_history.sql` | Repeatable test that shows change-level history on one key. |

Run the files in order, `01` → `06`, as a role that can create databases, warehouses and tasks (the scripts use `SYSADMIN`; executing tasks also needs `EXECUTE TASK` on the account).

## The mapping table

`IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING`, one row per entity:

| Column | Meaning |
| --- | --- |
| `MAPPING_ID` | Key passed to `SP_FW_LOAD(<MAPPING_ID>)`. |
| `ENTITY_NAME` | Logical name; also used for the work table name. |
| `STAGE_TABLE` | Insert-only source table (fully qualified). |
| `ACTIVE_TABLE` / `HISTORY_TABLE` | Targets (fully qualified). Created by `SP_FW_ONBOARD`. |
| `PRIMARY_KEY_COLS` | ARRAY of business key columns (composite keys supported). |
| `EVENT_TS_COL` | Source event timestamp that decides which version is newer (for example `SYS_UPDATED_ON`). |
| `EXCLUDE_COLS` | Stage-only columns that are **not** copied to ACTIVE or HISTORY (for example `_BATCH_ID`, `_LOAD_TS`). |
| `HASH_EXCLUDE_COLS` | Columns that are copied, but **ignored when deciding whether a record changed** (for example `SYS_MOD_COUNT`, `SYS_UPDATED_BY`). `EVENT_TS_COL` is always ignored. |
| `HISTORY_GRAIN` | `CHANGE` (one history row per change). |
| `HISTORY_TZ` | Time zone used to derive `HISTORY_DATE` (informational column on HISTORY). |
| `IS_ENABLED` | Disabled mappings are skipped. |

### Onboarding your own table

```sql
INSERT INTO IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING
  (MAPPING_ID, ENTITY_NAME, STAGE_TABLE, ACTIVE_TABLE, HISTORY_TABLE,
   PRIMARY_KEY_COLS, EVENT_TS_COL, EXCLUDE_COLS, HASH_EXCLUDE_COLS, COMMENTS)
SELECT 6, 'CUSTOMER',
       'IP_METADATA_FW.RAW_INSERT.CUSTOMER',
       'IP_METADATA_FW.CORE.CUSTOMER_ACTIVE',
       'IP_METADATA_FW.CORE.CUSTOMER_HISTORY',
       ARRAY_CONSTRUCT('CUSTOMER_ID'), 'UPDATED_AT',
       ARRAY_CONSTRUCT('_BATCH_ID', '_LOAD_TS'), ARRAY_CONSTRUCT(), 'Customer master';

CALL IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(6);   -- creates ACTIVE, HISTORY, work table, stream
CALL IP_METADATA_FW.CONTROL.SP_FW_LOAD(6);      -- first load: consumes the stream's initial snapshot
```

Run the first `SP_FW_LOAD` right after onboarding. On its first read, a new stream returns only the rows that existed when it was created; rows inserted after that are picked up from the next load on.

Then add a child task for it in `05_task_dag.sql` and add its stream to the root task's `WHEN` clause.

## What `SP_FW_LOAD` does

Outside the transaction:

1. Reads the mapping row and writes a `RUNNING` row to `FW_RUN_AUDIT`.
2. **Overlap guard:** skips the run if another run for the same mapping started in the last 60 minutes and is still `RUNNING`.
3. Builds the column lists, key join and change hash from `INFORMATION_SCHEMA`, so the procedure works for any table.
4. If the stream is empty, records `NO_DATA` and exits.

Inside **one transaction**:

| Step | What happens |
| --- | --- |
| `clear_work_table` | Empties the transient work table. |
| `count_stream_rows` | Counts new stage rows (`ROWS_READ`). |
| `changed_versions_to_work` | Takes every new version from the stream and keeps only real changes: <br>• rows **not newer** than ACTIVE are dropped (late arrivals) <br>• exact duplicates (same key and event timestamp) are dropped <br>• a version whose hash equals the version just before it (the first one is compared with ACTIVE) is dropped as unchanged |
| `merge_active` | `MERGE` the last kept version per key into ACTIVE. |
| `history_close_current` | Close the current HISTORY row of each changed key (`VALID_TO` = first new version's event time, `IS_CURRENT = FALSE`). |
| `history_insert_new_versions` | Insert every kept version, chained by `VALID_FROM` / `VALID_TO`; the last one is `IS_CURRENT = TRUE`. |
| `commit` | ACTIVE and HISTORY become visible together, and the **stream offset advances only here**. |

On any error the procedure rolls back. ACTIVE, HISTORY and the stream offset stay unchanged, so the same rows are retried on the next run. The audit row is set to `FAILED` with the failing step, SQLCODE, SQLSTATE and message, and the error is re-raised so the task shows as failed.

`SP_FW_LOAD_ALL()` runs every enabled mapping, one transaction per mapping. One failing table does not stop the others.

### Example: one key, two loads

Load 1, with 7 stage rows on the same day (Addr B repeated, Addr E duplicated) → **5 history rows**.
Load 2, with E again (unchanged), F (new) and a late 09:00 row → **1 new row**; E is closed and the late row is dropped.

| LOCATION | VALID_FROM | VALID_TO | IS_CURRENT |
| --- | --- | --- | --- |
| Addr A | 10:00 | 11:00 | FALSE |
| Addr B | 11:00 | 12:00 | FALSE |
| Addr C | 12:00 | 13:00 | FALSE |
| Addr D | 13:00 | 14:00 | FALSE |
| Addr E | 14:00 | 16:00 | FALSE |
| Addr F | 16:00 | | TRUE |

ACTIVE holds one row: Addr F. Reproduce it with `sql/06_test_change_history.sql`.

## Scheduling: task graph

```
TSK_FW_ROOT  (every 5 minutes, NO_OVERLAP; fires only if ANY stage stream has data)
  ├─ TSK_FW_LOAD_<TABLE>   (WHEN its own stream has data) → CALL SP_FW_LOAD(n)
  └─ ...
TSK_FW_FINALIZER          (after every graph run) → one summary row in FW_GRAPH_RUNS
```

- No warehouse runs when there is no new data; the `WHEN` checks don't use warehouse compute.
- Children run in parallel, and each table commits or rolls back on its own.
- The root task suspends itself after 10 consecutive failures (`SUSPEND_TASK_AFTER_NUM_FAILURES`).
- Manual run: `EXECUTE TASK IP_METADATA_FW.CONTROL.TSK_FW_ROOT;`

## Monitoring

```sql
-- Latest runs per table
SELECT ENTITY_NAME, STATUS, START_TS, DURATION_MS, ROWS_READ, BATCH_KEYS, DROPPED_ROWS,
       ACTIVE_INSERTED, ACTIVE_UPDATED, HIST_CLOSED, HIST_INSERTED, FAILED_STEP, ERROR_MESSAGE
FROM IP_METADATA_FW.CONTROL.FW_RUN_AUDIT ORDER BY START_TS DESC LIMIT 50;

-- Step-by-step detail (with query IDs) for one run
SELECT * FROM IP_METADATA_FW.CONTROL.FW_RUN_STEP_LOG WHERE RUN_ID = '<run_id>' ORDER BY STEP_NO;

-- One row per task graph run
SELECT * FROM IP_METADATA_FW.CONTROL.FW_GRAPH_RUNS ORDER BY FINALIZED_AT DESC LIMIT 20;

-- History integrity checks (all should return 0)
SELECT COUNT(*) FROM (SELECT SYS_ID FROM IP_METADATA_FW.CORE.SYS_USER_HISTORY
                      WHERE IS_CURRENT GROUP BY 1 HAVING COUNT(*) > 1);          -- >1 current row per key
SELECT COUNT(*) FROM IP_METADATA_FW.CORE.SYS_USER_HISTORY
 WHERE NOT IS_CURRENT AND VALID_TO IS NULL;                                       -- closed row without VALID_TO
```

| `FW_RUN_AUDIT.STATUS` | Meaning |
| --- | --- |
| `SUCCESS` | Loaded and committed. |
| `NO_DATA` | Stream was empty. |
| `SKIPPED_ALREADY_RUNNING` | Another run for the same mapping is in progress. |
| `FAILED` | Rolled back; see `FAILED_STEP` and `ERROR_MESSAGE`. Retried on the next run. |
| `RUNNING` | In progress (or the session died; ignored by the guard after 60 minutes). |

## Assumptions and notes

- **Deletes are not handled.** The stage is insert-only and only inserts and updates flow through.
- **History is never purged.**
- **`EVENT_TS_COL` must be set by the source.** It decides ordering; a version that is not newer than ACTIVE is treated as late and dropped.
- **Two versions with the same key and event timestamp:** only one is kept.
- **Stream retention:** the stage stream must be consumed within the table's data retention period (plus `MAX_DATA_EXTENSION_TIME_IN_DAYS`), or it goes stale. A 5-minute schedule keeps it well inside that.
- **Names:** object names come from the mapping table and are always quoted when used in dynamic SQL.
