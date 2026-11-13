-- IP-Metadata-Driven-Framework : 02 - metadata mapping table
-- One row per entity. The generic loader SP_FW_LOAD(<MAPPING_ID>) reads this row
-- to know which stage table to read, which ACTIVE/HISTORY tables to write,
-- the business key and the column that decides "latest".
USE ROLE SYSADMIN;

CREATE SCHEMA IF NOT EXISTS IP_METADATA_FW.CORE
  COMMENT = 'ACTIVE (latest per key) and HISTORY (SCD2, one row per change) tables loaded by the framework';

CREATE TABLE IF NOT EXISTS IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING (
    MAPPING_ID        NUMBER(10,0)   NOT NULL PRIMARY KEY,
    ENTITY_NAME       VARCHAR(64)    NOT NULL,
    STAGE_TABLE       VARCHAR(255)   NOT NULL,    -- insert-only source (read via stream)
    ACTIVE_TABLE      VARCHAR(255)   NOT NULL,    -- 1 row per key, latest version
    HISTORY_TABLE     VARCHAR(255)   NOT NULL,    -- SCD2, one version per change (identical consecutive versions ignored)
    PRIMARY_KEY_COLS  ARRAY          NOT NULL,    -- business key (array => composite keys supported)
    EVENT_TS_COL      VARCHAR(255)   NOT NULL,    -- source timestamp that decides "latest"
    EXCLUDE_COLS      ARRAY,                      -- stage-only columns not copied to ACTIVE/HISTORY
    HASH_EXCLUDE_COLS ARRAY,                      -- copied, but ignored when deciding "did the record change" (EVENT_TS_COL always ignored)
    HISTORY_GRAIN     VARCHAR(10)    DEFAULT 'CHANGE' NOT NULL,
    HISTORY_TZ        VARCHAR(64)    DEFAULT 'UTC'   NOT NULL,
    IS_ENABLED        BOOLEAN        DEFAULT TRUE    NOT NULL,
    COMMENTS          VARCHAR(1000),
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT SYSDATE(),
    UPDATED_AT        TIMESTAMP_NTZ  DEFAULT SYSDATE(),
    UPDATED_BY        VARCHAR(255)   DEFAULT CURRENT_USER()
);
ALTER TABLE IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING ADD COLUMN IF NOT EXISTS HASH_EXCLUDE_COLS ARRAY;

MERGE INTO IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING t
USING (
    SELECT column1 AS MAPPING_ID, column2 AS ENTITY_NAME,
           'IP_METADATA_FW.RAW_INSERT.' || column2        AS STAGE_TABLE,
           'IP_METADATA_FW.CORE.' || column2 || '_ACTIVE'  AS ACTIVE_TABLE,
           'IP_METADATA_FW.CORE.' || column2 || '_HISTORY' AS HISTORY_TABLE,
           ARRAY_CONSTRUCT('SYS_ID')                      AS PRIMARY_KEY_COLS,
           'SYS_UPDATED_ON'                               AS EVENT_TS_COL,
           ARRAY_CONSTRUCT('_BATCH_ID', '_LOAD_TS')       AS EXCLUDE_COLS,
           ARRAY_CONSTRUCT('SYS_MOD_COUNT', 'SYS_UPDATED_BY') AS HASH_EXCLUDE_COLS,
           column3 AS COMMENTS
    FROM VALUES
        (1, 'INCIDENT',       'ServiceNow incident'),
        (2, 'PROBLEM',        'ServiceNow problem'),
        (3, 'CHANGE_REQUEST', 'ServiceNow change_request'),
        (4, 'SC_REQ_ITEM',    'ServiceNow sc_req_item (requested item)'),
        (5, 'SYS_USER',       'ServiceNow sys_user')
) s
ON t.MAPPING_ID = s.MAPPING_ID
WHEN MATCHED THEN UPDATE SET
    ENTITY_NAME = s.ENTITY_NAME, STAGE_TABLE = s.STAGE_TABLE, ACTIVE_TABLE = s.ACTIVE_TABLE,
    HISTORY_TABLE = s.HISTORY_TABLE, PRIMARY_KEY_COLS = s.PRIMARY_KEY_COLS, EVENT_TS_COL = s.EVENT_TS_COL,
    EXCLUDE_COLS = s.EXCLUDE_COLS, HASH_EXCLUDE_COLS = s.HASH_EXCLUDE_COLS, HISTORY_GRAIN = 'CHANGE', COMMENTS = s.COMMENTS,
    UPDATED_AT = SYSDATE(), UPDATED_BY = CURRENT_USER()
WHEN NOT MATCHED THEN INSERT
    (MAPPING_ID, ENTITY_NAME, STAGE_TABLE, ACTIVE_TABLE, HISTORY_TABLE, PRIMARY_KEY_COLS,
     EVENT_TS_COL, EXCLUDE_COLS, HASH_EXCLUDE_COLS, COMMENTS)
VALUES (s.MAPPING_ID, s.ENTITY_NAME, s.STAGE_TABLE, s.ACTIVE_TABLE, s.HISTORY_TABLE, s.PRIMARY_KEY_COLS,
        s.EVENT_TS_COL, s.EXCLUDE_COLS, s.HASH_EXCLUDE_COLS, s.COMMENTS);

SELECT * FROM IP_METADATA_FW.CONTROL.FW_TABLE_MAPPING ORDER BY MAPPING_ID;
