-- IP-Metadata-Driven-Framework : 03 - audit tables + generic SQL (Snowflake Scripting) procedures
--   SP_FW_ONBOARD(mapping_id) : one-time per entity. Creates ACTIVE, HISTORY, work table and stream.
--   SP_FW_LOAD(mapping_id)    : stage (stream) -> ACTIVE (latest per key) + HISTORY (SCD2, one row per change),
--                               all writes in ONE transaction, with run audit + step log.
--   SP_FW_LOAD_ALL()          : runs every enabled mapping (one transaction per mapping).
USE ROLE SYSADMIN;
USE WAREHOUSE IP_MDF_LOAD_WH;

------------------------------------------------------------------------------
-- Audit tables
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS IP_METADATA_FW.CONTROL.FW_RUN_AUDIT (
    RUN_ID                VARCHAR(36)   NOT NULL,
    MAPPING_ID            NUMBER(10,0)  NOT NULL,
    ENTITY_NAME           VARCHAR(64),
    STATUS                VARCHAR(30)   NOT NULL,  -- RUNNING | SUCCESS | NO_DATA | FAILED | SKIPPED_ALREADY_RUNNING
    START_TS              TIMESTAMP_NTZ,
    END_TS                TIMESTAMP_NTZ,
    DURATION_MS           NUMBER,
    ROWS_READ             NUMBER,                  -- new stage rows in the stream
    BATCH_KEYS            NUMBER,                  -- changed versions kept from the stream (all keys)
    DROPPED_ROWS          NUMBER,                  -- late (not newer than ACTIVE), duplicate timestamp, or unchanged (same hash as previous version)
    ACTIVE_INSERTED       NUMBER,
    ACTIVE_UPDATED        NUMBER,
    HIST_SAMEDAY_UPDATED  NUMBER,                  -- unused since change-grain history (kept for compatibility)
    HIST_CLOSED           NUMBER,                  -- previously current versions closed (VALID_TO set)
    HIST_INSERTED         NUMBER,                  -- new versions (every change, not one per day)
    FAILED_STEP           VARCHAR(100),
    ERROR_MESSAGE         VARCHAR(4000),
    TRIGGERED_BY          VARCHAR(255),            -- task name, or user for manual calls
    SESSION_ID            NUMBER
);

CREATE TABLE IF NOT EXISTS IP_METADATA_FW.CONTROL.FW_RUN_STEP_LOG (
    RUN_ID         VARCHAR(36)   NOT NULL,
    STEP_NO        NUMBER        NOT NULL,
    STEP_NAME      VARCHAR(100)  NOT NULL,
    STATUS         VARCHAR(20)   NOT NULL,         -- OK | FAILED
    ROWS_AFFECTED  NUMBER,
    QUERY_ID       VARCHAR(64),
    START_TS       TIMESTAMP_NTZ,
    DURATION_MS    NUMBER,
    MESSAGE        VARCHAR(4000)
);

------------------------------------------------------------------------------
-- SP_FW_ONBOARD : create target objects from the mapping row (idempotent)
--   ACTIVE  = stage columns (minus EXCLUDE_COLS) + ROW_HASH + FW_* audit columns
--   HISTORY = same + HISTORY_DATE, VALID_FROM, VALID_TO, IS_CURRENT
--   WORK    = transient batch table used inside the load transaction
--   STREAM  = append-only stream on the stage table (SHOW_INITIAL_ROWS so existing rows load once)
------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(P_MAPPING_ID NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_found      NUMBER;
    v_entity     VARCHAR;
    v_stage      VARCHAR;
    v_active     VARCHAR;
    v_hist       VARCHAR;
    v_ev         VARCHAR;
    v_stage_q    VARCHAR;
    v_active_q   VARCHAR;
    v_hist_q     VARCHAR;
    v_wrk_q      VARCHAR;
    v_strm_q     VARCHAR;
    v_cols       VARCHAR;
    v_pk_found   NUMBER;
    v_pk_total   NUMBER;
    v_ev_found   NUMBER;
    v_sql        VARCHAR;
    v_fw_cols    VARCHAR DEFAULT 'CAST(NULL AS NUMBER(19,0)) AS ROW_HASH, CAST(NULL AS VARCHAR(36)) AS FW_RUN_ID, '
                              || 'CAST(NULL AS TIMESTAMP_NTZ) AS FW_INSERTED_AT, CAST(NULL AS TIMESTAMP_NTZ) AS FW_UPDATED_AT';
    v_hist_cols  VARCHAR DEFAULT 'CAST(NULL AS DATE) AS HISTORY_DATE, CAST(NULL AS TIMESTAMP_NTZ) AS VALID_FROM, '
                              || 'CAST(NULL AS TIMESTAMP_NTZ) AS VALID_TO, CAST(NULL AS BOOLEAN) AS IS_CURRENT';
    rs           RESULTSET;
    e_config     EXCEPTION (-20001, 'Invalid framework mapping');
BEGIN
    SELECT COUNT(*) INTO :v_found FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE MAPPING_ID = :P_MAPPING_ID;
    IF (v_found = 0) THEN
        RAISE e_config;
    END IF;
    SELECT ENTITY_NAME, STAGE_TABLE, ACTIVE_TABLE, HISTORY_TABLE, UPPER(EVENT_TS_COL)
      INTO :v_entity, :v_stage, :v_active, :v_hist, :v_ev
      FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE MAPPING_ID = :P_MAPPING_ID;

    -- Quote every name part ("DB"."SCHEMA"."TABLE"); mapping values never reach SQL unquoted.
    v_stage_q  := '"' || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 1)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 2)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 3)), '"', '') || '"';
    v_active_q := '"' || REPLACE(UPPER(SPLIT_PART(v_active, '.', 1)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_active, '.', 2)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_active, '.', 3)), '"', '') || '"';
    v_hist_q   := '"' || REPLACE(UPPER(SPLIT_PART(v_hist, '.', 1)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_hist, '.', 2)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_hist, '.', 3)), '"', '') || '"';
    v_wrk_q    := '"' || REPLACE(UPPER(SPLIT_PART(v_active, '.', 1)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_active, '.', 2)), '"', '') || '"."'
               || REPLACE(UPPER(v_entity), '"', '') || '_FW_WRK"';
    v_strm_q   := '"' || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 1)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 2)), '"', '') || '"."'
               || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 3)), '"', '') || '_FW_STRM"';

    -- Stage column list (minus EXCLUDE_COLS) and key/event validation, from INFORMATION_SCHEMA
    v_sql := 'WITH m AS (SELECT PRIMARY_KEY_COLS, EXCLUDE_COLS FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE MAPPING_ID = ?), '
          || 'pk AS (SELECT UPPER(f.value::VARCHAR) AS c FROM m, LATERAL FLATTEN(m.PRIMARY_KEY_COLS) f), '
          || 'ex AS (SELECT UPPER(f.value::VARCHAR) AS c FROM m, LATERAL FLATTEN(m.EXCLUDE_COLS) f), '
          || 'cols AS (SELECT c.COLUMN_NAME AS cn, c.ORDINAL_POSITION AS pos FROM "'
          ||   REPLACE(UPPER(SPLIT_PART(v_stage, '.', 1)), '"', '') || '".INFORMATION_SCHEMA.COLUMNS c '
          ||   'WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME NOT IN (SELECT c FROM ex)) '
          || 'SELECT LISTAGG(''"'' || REPLACE(cn, ''"'', ''""'') || ''"'', '', '') WITHIN GROUP (ORDER BY pos), '
          || '(SELECT COUNT(*) FROM cols JOIN pk ON pk.c = cols.cn), (SELECT COUNT(*) FROM pk), COUNT_IF(cn = ?) FROM cols';
    LET v_sch VARCHAR := UPPER(SPLIT_PART(v_stage, '.', 2));
    LET v_tbl VARCHAR := UPPER(SPLIT_PART(v_stage, '.', 3));
    rs := (EXECUTE IMMEDIATE :v_sql USING (P_MAPPING_ID, v_sch, v_tbl, v_ev));
    LET c1 CURSOR FOR rs;
    OPEN c1;
    FETCH c1 INTO v_cols, v_pk_found, v_pk_total, v_ev_found;
    CLOSE c1;
    IF (v_cols IS NULL) THEN
        RAISE e_config;   -- stage table not found
    END IF;
    IF (v_pk_found <> v_pk_total OR v_pk_total = 0 OR v_ev_found <> 1) THEN
        RAISE e_config;   -- key or event column missing in stage table
    END IF;

    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || v_active_q || ' AS SELECT ' || v_cols || ', ' || v_fw_cols
                   || ' FROM ' || v_stage_q || ' WHERE 1 = 0';
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || v_hist_q || ' AS SELECT ' || v_cols || ', ' || v_hist_cols || ', '
                   || v_fw_cols || ' FROM ' || v_stage_q || ' WHERE 1 = 0';
    EXECUTE IMMEDIATE 'CREATE TRANSIENT TABLE IF NOT EXISTS ' || v_wrk_q || ' DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT '
                   || v_cols || ', CAST(NULL AS NUMBER(19,0)) AS ROW_HASH, CAST(NULL AS DATE) AS HISTORY_DATE, '
                   || 'CAST(NULL AS BOOLEAN) AS IS_LAST, CAST(NULL AS TIMESTAMP_NTZ) AS NEXT_TS FROM '
                   || v_stage_q || ' WHERE 1 = 0';
    EXECUTE IMMEDIATE 'CREATE STREAM IF NOT EXISTS ' || v_strm_q || ' ON TABLE ' || v_stage_q
                   || ' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE';

    RETURN OBJECT_CONSTRUCT('mapping_id', P_MAPPING_ID, 'entity', v_entity, 'active', v_active_q,
                            'history', v_hist_q, 'work', v_wrk_q, 'stream', v_strm_q, 'status', 'ONBOARDED');
END;
$$;

------------------------------------------------------------------------------
-- SP_FW_LOAD : generic loader for one mapping row
--   1. read mapping, write RUNNING audit row (autocommit - survives rollback)
--   2. guard against a concurrent run for the same mapping
--   3. build column lists / join keys / hash from INFORMATION_SCHEMA
--   4. stream empty -> NO_DATA
--   5. ONE transaction: every changed version per key -> MERGE ACTIVE (last version) ->
--      HISTORY close current version / insert all new versions -> COMMIT
--      A version is kept when it is newer than ACTIVE and its hash differs from the version before it
--      (the version before the first one is the ACTIVE row). 5 address changes in a day = 5 history rows.
--   on any error: ROLLBACK (ACTIVE, HISTORY and stream offset unchanged), audit FAILED, re-raise
------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE IP_METADATA_FW.CONTROL.SP_FW_LOAD(P_MAPPING_ID NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_status      VARCHAR DEFAULT 'SUCCESS';
    v_step        VARCHAR DEFAULT 'init';
    v_steps       ARRAY   DEFAULT ARRAY_CONSTRUCT();
    v_t0          TIMESTAMP_NTZ DEFAULT SYSDATE();
    v_st          TIMESTAMP_NTZ;
    v_in_txn      BOOLEAN DEFAULT FALSE;
    v_found       NUMBER;
    v_enabled     BOOLEAN;
    v_entity      VARCHAR;
    v_stage       VARCHAR;
    v_active      VARCHAR;
    v_hist        VARCHAR;
    v_ev          VARCHAR;
    v_tz          VARCHAR;
    v_who         VARCHAR;
    v_stage_q     VARCHAR;
    v_active_q    VARCHAR;
    v_hist_q      VARCHAR;
    v_wrk_q       VARCHAR;
    v_strm_q      VARCHAR;
    v_cols        VARCHAR;
    v_wcols       VARCHAR;
    v_attrs       VARCHAR;
    v_set         VARCHAR;
    v_on_pk       VARCHAR;
    v_pk          VARCHAR;
    v_pk_found    NUMBER;
    v_pk_total    NUMBER;
    v_ev_found    NUMBER;
    v_evq         VARCHAR;
    v_day         VARCHAR;
    v_hash        VARCHAR;
    v_sql         VARCHAR;
    v_has_data    BOOLEAN;
    v_busy        NUMBER;
    v_n           NUMBER;
    v_qid         VARCHAR;
    v_err         VARCHAR;
    v_rows_read   NUMBER;
    v_batch       NUMBER;
    v_dropped     NUMBER;
    v_a_ins       NUMBER;
    v_a_upd       NUMBER;
    v_h_same      NUMBER;
    v_h_closed    NUMBER;
    v_h_ins       NUMBER;
    rs            RESULTSET;
    e_config      EXCEPTION (-20001, 'Invalid framework mapping or metadata');
BEGIN
    -- 1. mapping row ---------------------------------------------------------
    SELECT COUNT(*) INTO :v_found FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE MAPPING_ID = :P_MAPPING_ID;
    IF (v_found = 0) THEN
        RAISE e_config;
    END IF;
    SELECT ENTITY_NAME, STAGE_TABLE, ACTIVE_TABLE, HISTORY_TABLE, UPPER(EVENT_TS_COL), UPPER(HISTORY_TZ), IS_ENABLED
      INTO :v_entity, :v_stage, :v_active, :v_hist, :v_ev, :v_tz, :v_enabled
      FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE MAPPING_ID = :P_MAPPING_ID;
    IF (NOT v_enabled) THEN
        RETURN OBJECT_CONSTRUCT('mapping_id', P_MAPPING_ID, 'status', 'DISABLED');
    END IF;

    SELECT COALESCE(NULLIF(SYSTEM$CURRENT_USER_TASK_NAME(), ''), CURRENT_USER()) INTO :v_who;
    INSERT INTO IP_METADATA_FW.CONTROL.FW_RUN_AUDIT
        (RUN_ID, MAPPING_ID, ENTITY_NAME, STATUS, START_TS, TRIGGERED_BY, SESSION_ID)
        VALUES (:v_run_id, :P_MAPPING_ID, :v_entity, 'RUNNING', :v_t0, :v_who, CURRENT_SESSION());

    BEGIN
        -- 2. concurrency guard (stale RUNNING rows older than 60 min are ignored)
        v_step := 'concurrency_guard';
        SELECT COUNT(*) INTO :v_busy FROM IP_METADATA_FW.CONTROL.FW_RUN_AUDIT
         WHERE MAPPING_ID = :P_MAPPING_ID AND STATUS = 'RUNNING' AND RUN_ID <> :v_run_id
           AND START_TS > DATEADD(minute, -60, SYSDATE());

        IF (v_busy > 0) THEN
            v_status := 'SKIPPED_ALREADY_RUNNING';
        ELSE
            -- 3. resolve objects and column lists from metadata
            v_step := 'resolve_metadata';
            v_stage_q  := '"' || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 1)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 2)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 3)), '"', '') || '"';
            v_active_q := '"' || REPLACE(UPPER(SPLIT_PART(v_active, '.', 1)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_active, '.', 2)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_active, '.', 3)), '"', '') || '"';
            v_hist_q   := '"' || REPLACE(UPPER(SPLIT_PART(v_hist, '.', 1)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_hist, '.', 2)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_hist, '.', 3)), '"', '') || '"';
            v_wrk_q    := '"' || REPLACE(UPPER(SPLIT_PART(v_active, '.', 1)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_active, '.', 2)), '"', '') || '"."'
                       || REPLACE(UPPER(v_entity), '"', '') || '_FW_WRK"';
            v_strm_q   := '"' || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 1)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 2)), '"', '') || '"."'
                       || REPLACE(UPPER(SPLIT_PART(v_stage, '.', 3)), '"', '') || '_FW_STRM"';

            v_sql := 'WITH m AS (SELECT PRIMARY_KEY_COLS, EXCLUDE_COLS, HASH_EXCLUDE_COLS FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE MAPPING_ID = ?), '
                  || 'pk AS (SELECT UPPER(f.value::VARCHAR) AS c FROM m, LATERAL FLATTEN(m.PRIMARY_KEY_COLS) f), '
                  || 'ex AS (SELECT UPPER(f.value::VARCHAR) AS c FROM m, LATERAL FLATTEN(m.EXCLUDE_COLS) f), '
                  || 'hx AS (SELECT UPPER(f.value::VARCHAR) AS c FROM m, LATERAL FLATTEN(m.HASH_EXCLUDE_COLS, OUTER => TRUE) f), '
                  || 'cols AS (SELECT c.COLUMN_NAME AS cn, c.ORDINAL_POSITION AS pos, '
                  ||   '''"'' || REPLACE(c.COLUMN_NAME, ''"'', ''""'') || ''"'' AS q, (pk.c IS NOT NULL) AS is_pk FROM "'
                  ||   REPLACE(UPPER(SPLIT_PART(v_stage, '.', 1)), '"', '') || '".INFORMATION_SCHEMA.COLUMNS c '
                  ||   'LEFT JOIN pk ON pk.c = c.COLUMN_NAME '
                  ||   'WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME NOT IN (SELECT c FROM ex)) '
                  || 'SELECT LISTAGG(q, '', '') WITHIN GROUP (ORDER BY pos), '
                  ||        'LISTAGG(''w.'' || q, '', '') WITHIN GROUP (ORDER BY pos), '
                  ||        'LISTAGG(IFF(is_pk OR cn = ? OR cn IN (SELECT c FROM hx WHERE c IS NOT NULL), NULL, q), '', '') WITHIN GROUP (ORDER BY pos), '
                  ||        'LISTAGG(IFF(is_pk, NULL, q || '' = w.'' || q), '', '') WITHIN GROUP (ORDER BY pos), '
                  ||        'LISTAGG(IFF(is_pk, ''t.'' || q || '' = w.'' || q, NULL), '' AND '') WITHIN GROUP (ORDER BY pos), '
                  ||        'LISTAGG(IFF(is_pk, q, NULL), '', '') WITHIN GROUP (ORDER BY pos), '
                  ||        'COUNT_IF(is_pk), (SELECT COUNT(*) FROM pk), COUNT_IF(cn = ?) FROM cols';
            LET v_sch VARCHAR := UPPER(SPLIT_PART(v_stage, '.', 2));
            LET v_tbl VARCHAR := UPPER(SPLIT_PART(v_stage, '.', 3));
            rs := (EXECUTE IMMEDIATE :v_sql USING (P_MAPPING_ID, v_sch, v_tbl, v_ev, v_ev));
            LET c1 CURSOR FOR rs;
            OPEN c1;
            FETCH c1 INTO v_cols, v_wcols, v_attrs, v_set, v_on_pk, v_pk, v_pk_found, v_pk_total, v_ev_found;
            CLOSE c1;
            IF (v_cols IS NULL OR v_pk_total = 0 OR v_pk_found <> v_pk_total OR v_ev_found <> 1) THEN
                RAISE e_config;   -- stage table missing, or key/event column not in stage table
            END IF;

            v_evq  := '"' || REPLACE(v_ev, '"', '""') || '"';
            v_hash := 'HASH(' || v_attrs || ')';
            v_day  := IFF(v_tz = 'UTC', v_evq || '::DATE',
                          'CONVERT_TIMEZONE(''UTC'', ''' || REPLACE(v_tz, '''', '') || ''', ' || v_evq || ')::DATE');

            -- targets must exist (created by SP_FW_ONBOARD)
            EXECUTE IMMEDIATE 'SELECT 1 FROM ' || v_active_q || ' LIMIT 0';
            EXECUTE IMMEDIATE 'SELECT 1 FROM ' || v_hist_q   || ' LIMIT 0';
            EXECUTE IMMEDIATE 'SELECT 1 FROM ' || v_wrk_q    || ' LIMIT 0';

            -- 4. anything new in the stream?
            v_step := 'check_stream';
            rs := (EXECUTE IMMEDIATE 'SELECT SYSTEM$STREAM_HAS_DATA(''' || REPLACE(v_strm_q, '''', '''''') || ''')');
            LET c2 CURSOR FOR rs;
            OPEN c2;
            FETCH c2 INTO v_has_data;
            CLOSE c2;

            IF (NOT v_has_data) THEN
                v_status := 'NO_DATA';
                v_rows_read := 0;
            ELSE
                -- 5. single transaction ------------------------------------------
                BEGIN TRANSACTION;
                v_in_txn := TRUE;

                v_step := 'clear_work_table';
                v_st := SYSDATE();
                EXECUTE IMMEDIATE 'DELETE FROM ' || v_wrk_q;
                v_n := SQLROWCOUNT;
                v_qid := LAST_QUERY_ID();
                v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'OK', 'rows', v_n, 'qid', v_qid,
                                        'st', v_st, 'ms', DATEDIFF(millisecond, v_st, SYSDATE())));

                v_step := 'count_stream_rows';
                v_st := SYSDATE();
                rs := (EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_strm_q || ' WHERE METADATA$ACTION = ''INSERT''');
                v_qid := LAST_QUERY_ID();
                LET c3 CURSOR FOR rs;
                OPEN c3;
                FETCH c3 INTO v_rows_read;
                CLOSE c3;
                v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'OK', 'rows', v_rows_read, 'qid', v_qid,
                                        'st', v_st, 'ms', DATEDIFF(millisecond, v_st, SYSDATE())));

                -- s  : stream rows, one per (key, event ts)
                -- n  : only rows newer than ACTIVE (late rows dropped)
                -- u  : n + the ACTIVE row of those keys as the baseline
                -- k  : keep a new row only if its hash differs from the version right before it
                v_step := 'changed_versions_to_work';
                v_st := SYSDATE();
                EXECUTE IMMEDIATE 'INSERT INTO ' || v_wrk_q || ' (' || v_cols || ', ROW_HASH, HISTORY_DATE, IS_LAST, NEXT_TS) '
                    || 'WITH s AS (SELECT ' || v_cols || ', ' || v_hash || ' AS ROW_HASH FROM ' || v_strm_q
                    ||   ' WHERE METADATA$ACTION = ''INSERT'' QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || v_pk || ', ' || v_evq
                    ||   ' ORDER BY ' || v_evq || ') = 1), '
                    || 'n AS (SELECT w.* FROM s w WHERE NOT EXISTS (SELECT 1 FROM ' || v_active_q || ' t WHERE ' || v_on_pk
                    ||   ' AND t.' || v_evq || ' >= w.' || v_evq || ')), '
                    || 'u AS (SELECT ' || v_cols || ', ROW_HASH, 1 AS IS_NEW FROM n UNION ALL SELECT ' || v_cols
                    ||   ', ' || v_hash || ', 0 FROM ' || v_active_q || ' t WHERE EXISTS (SELECT 1 FROM n w WHERE ' || v_on_pk || ')), '
                    || 'k AS (SELECT * FROM u QUALIFY IS_NEW = 1 AND EQUAL_NULL(LAG(ROW_HASH) OVER (PARTITION BY ' || v_pk
                    ||   ' ORDER BY ' || v_evq || '), ROW_HASH) = FALSE) '
                    || 'SELECT ' || v_cols || ', ROW_HASH, ' || v_day || ', '
                    || 'LEAD(' || v_evq || ') OVER (PARTITION BY ' || v_pk || ' ORDER BY ' || v_evq || ') IS NULL, '
                    || 'LEAD(' || v_evq || ') OVER (PARTITION BY ' || v_pk || ' ORDER BY ' || v_evq || ') FROM k';
                v_batch := SQLROWCOUNT;
                v_dropped := v_rows_read - v_batch;
                v_qid := LAST_QUERY_ID();
                v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'OK', 'rows', v_batch, 'qid', v_qid,
                                        'st', v_st, 'ms', DATEDIFF(millisecond, v_st, SYSDATE())));

                v_step := 'merge_active';
                v_st := SYSDATE();
                v_sql := 'MERGE INTO ' || v_active_q || ' t USING (SELECT * FROM ' || v_wrk_q || ' WHERE IS_LAST) w ON ' || v_on_pk
                      || ' WHEN MATCHED THEN UPDATE SET ' || v_set
                      || ', ROW_HASH = w.ROW_HASH, FW_RUN_ID = ?, FW_UPDATED_AT = SYSDATE()'
                      || ' WHEN NOT MATCHED THEN INSERT (' || v_cols || ', ROW_HASH, FW_RUN_ID, FW_INSERTED_AT, FW_UPDATED_AT)'
                      || ' VALUES (' || v_wcols || ', w.ROW_HASH, ?, SYSDATE(), SYSDATE())';
                EXECUTE IMMEDIATE :v_sql USING (v_run_id, v_run_id);
                v_n := SQLROWCOUNT;
                v_qid := LAST_QUERY_ID();
                SELECT "number of rows inserted", "number of rows updated" INTO :v_a_ins, :v_a_upd
                  FROM TABLE(RESULT_SCAN(:v_qid));
                v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'OK', 'rows', v_n, 'qid', v_qid,
                                        'st', v_st, 'ms', DATEDIFF(millisecond, v_st, SYSDATE())));

                -- close the current version at the first new version of the key
                v_step := 'history_close_current';
                v_st := SYSDATE();
                v_sql := 'UPDATE ' || v_hist_q || ' t SET VALID_TO = w.FW_FIRST_TS'
                      || ', IS_CURRENT = FALSE, FW_RUN_ID = ?, FW_UPDATED_AT = SYSDATE() FROM (SELECT ' || v_pk
                      || ', MIN(' || v_evq || ') AS FW_FIRST_TS FROM ' || v_wrk_q || ' GROUP BY ' || v_pk || ') w WHERE '
                      || v_on_pk || ' AND t.IS_CURRENT';
                EXECUTE IMMEDIATE :v_sql USING (v_run_id);
                v_h_closed := SQLROWCOUNT;
                v_qid := LAST_QUERY_ID();
                v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'OK', 'rows', v_h_closed, 'qid', v_qid,
                                        'st', v_st, 'ms', DATEDIFF(millisecond, v_st, SYSDATE())));

                v_step := 'history_insert_new_versions';
                v_st := SYSDATE();
                v_sql := 'INSERT INTO ' || v_hist_q || ' (' || v_cols || ', HISTORY_DATE, VALID_FROM, VALID_TO, IS_CURRENT, '
                      || 'ROW_HASH, FW_RUN_ID, FW_INSERTED_AT, FW_UPDATED_AT) SELECT ' || v_wcols
                      || ', w.HISTORY_DATE, w.' || v_evq || ', w.NEXT_TS, w.IS_LAST, w.ROW_HASH, ?, SYSDATE(), SYSDATE() FROM '
                      || v_wrk_q || ' w';
                EXECUTE IMMEDIATE :v_sql USING (v_run_id);
                v_h_ins := SQLROWCOUNT;
                v_qid := LAST_QUERY_ID();
                v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'OK', 'rows', v_h_ins, 'qid', v_qid,
                                        'st', v_st, 'ms', DATEDIFF(millisecond, v_st, SYSDATE())));

                v_step := 'commit';
                COMMIT;          -- stream offset advances only here
                v_in_txn := FALSE;
            END IF;
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            v_err := LEFT('SQLCODE ' || SQLCODE || ' (' || SQLSTATE || '): ' || SQLERRM, 4000);
            IF (v_in_txn) THEN
                ROLLBACK;        -- ACTIVE, HISTORY and stream offset unchanged
            END IF;
            v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT('n', v_step, 's', 'FAILED', 'msg', v_err,
                                    'st', SYSDATE(), 'ms', 0));
            UPDATE IP_METADATA_FW.CONTROL.FW_RUN_AUDIT
               SET STATUS = 'FAILED', END_TS = SYSDATE(), DURATION_MS = DATEDIFF(millisecond, :v_t0, SYSDATE()),
                   ROWS_READ = :v_rows_read, BATCH_KEYS = :v_batch, DROPPED_ROWS = :v_dropped,
                   FAILED_STEP = :v_step, ERROR_MESSAGE = :v_err
             WHERE RUN_ID = :v_run_id;
            INSERT INTO IP_METADATA_FW.CONTROL.FW_RUN_STEP_LOG
            SELECT :v_run_id, f.index + 1, f.value:n::VARCHAR, f.value:s::VARCHAR, f.value:rows::NUMBER,
                   f.value:qid::VARCHAR, f.value:st::TIMESTAMP_NTZ, f.value:ms::NUMBER, f.value:msg::VARCHAR
              FROM TABLE(FLATTEN(INPUT => :v_steps)) f;
            RAISE;
    END;

    -- finish (SUCCESS / NO_DATA / SKIPPED_ALREADY_RUNNING) ------------------------
    UPDATE IP_METADATA_FW.CONTROL.FW_RUN_AUDIT
       SET STATUS = :v_status, END_TS = SYSDATE(), DURATION_MS = DATEDIFF(millisecond, :v_t0, SYSDATE()),
           ROWS_READ = :v_rows_read, BATCH_KEYS = :v_batch, DROPPED_ROWS = :v_dropped,
           ACTIVE_INSERTED = :v_a_ins, ACTIVE_UPDATED = :v_a_upd, HIST_SAMEDAY_UPDATED = :v_h_same,
           HIST_CLOSED = :v_h_closed, HIST_INSERTED = :v_h_ins
     WHERE RUN_ID = :v_run_id;
    INSERT INTO IP_METADATA_FW.CONTROL.FW_RUN_STEP_LOG
    SELECT :v_run_id, f.index + 1, f.value:n::VARCHAR, f.value:s::VARCHAR, f.value:rows::NUMBER,
           f.value:qid::VARCHAR, f.value:st::TIMESTAMP_NTZ, f.value:ms::NUMBER, f.value:msg::VARCHAR
      FROM TABLE(FLATTEN(INPUT => :v_steps)) f;

    RETURN OBJECT_CONSTRUCT('run_id', v_run_id, 'mapping_id', P_MAPPING_ID, 'entity', v_entity, 'status', v_status,
                            'rows_read', v_rows_read, 'batch_keys', v_batch, 'dropped', v_dropped,
                            'active_inserted', v_a_ins, 'active_updated', v_a_upd,
                            'hist_closed', v_h_closed, 'hist_inserted', v_h_ins,
                            'duration_ms', DATEDIFF(millisecond, v_t0, SYSDATE()));
END;
$$;

------------------------------------------------------------------------------
-- SP_FW_LOAD_ALL : run all enabled mappings; one failing entity does not stop the others
------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE IP_METADATA_FW.CONTROL.SP_FW_LOAD_ALL()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_out  ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_res  VARIANT;
    c1 CURSOR FOR SELECT MAPPING_ID FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING WHERE IS_ENABLED ORDER BY MAPPING_ID;
BEGIN
    FOR r IN c1 DO
        LET v_id NUMBER := r.MAPPING_ID;
        BEGIN
            CALL IP_METADATA_FW.CONTROL.SP_FW_LOAD(:v_id) INTO :v_res;
            v_out := ARRAY_APPEND(v_out, v_res);
        EXCEPTION
            WHEN OTHER THEN
                v_out := ARRAY_APPEND(v_out, OBJECT_CONSTRUCT('mapping_id', v_id, 'status', 'FAILED',
                                                              'error', LEFT(SQLERRM, 1000)));
        END;
    END FOR;
    RETURN v_out;
END;
$$;
