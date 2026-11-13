-- IP-Metadata-Driven-Framework : 06 - change-level history test (SYS_USER, key TEST-ADDR-001; LOCATION = customer address)
USE ROLE SYSADMIN; USE WAREHOUSE IP_MDF_LOAD_WH;
ALTER TASK IP_METADATA_FW.CONTROL.TSK_FW_ROOT SUSPEND;
-- reset the test key
DELETE FROM IP_METADATA_FW.RAW_INSERT.SYS_USER   WHERE SYS_ID = 'TEST-ADDR-001';
DELETE FROM IP_METADATA_FW.CORE.SYS_USER_ACTIVE  WHERE SYS_ID = 'TEST-ADDR-001';
DELETE FROM IP_METADATA_FW.CORE.SYS_USER_HISTORY WHERE SYS_ID = 'TEST-ADDR-001';

-- Run 1: 7 stage rows in one day -> expect 5 history rows (B repeated and exact duplicate are ignored)
INSERT INTO IP_METADATA_FW.RAW_INSERT.SYS_USER
SELECT 'TEST-ADDR-001','jdoe','John','Doe','jdoe@x.com','Analyst','IT', loc, NULL, TRUE,
       '2026-11-01'::TIMESTAMP_NTZ,'test', ts::TIMESTAMP_NTZ,'test', n, 'TEST-ADDR', SYSDATE()
FROM VALUES ('Addr A','2026-11-12 10:00:00',1), ('Addr B','2026-11-12 11:00:00',2), ('Addr B','2026-11-12 11:30:00',3),
            ('Addr C','2026-11-12 12:00:00',4), ('Addr D','2026-11-12 13:00:00',5), ('Addr E','2026-11-12 14:00:00',6),
            ('Addr E','2026-11-12 14:00:00',6) v(loc, ts, n);
CALL IP_METADATA_FW.CONTROL.SP_FW_LOAD(5);

-- Run 2: unchanged E later, new F, and a late row older than ACTIVE -> expect 1 new history row
INSERT INTO IP_METADATA_FW.RAW_INSERT.SYS_USER
SELECT 'TEST-ADDR-001','jdoe','John','Doe','jdoe@x.com','Analyst','IT', loc, NULL, TRUE,
       '2026-11-01'::TIMESTAMP_NTZ,'test', ts::TIMESTAMP_NTZ,'test', n, 'TEST-ADDR', SYSDATE()
FROM VALUES ('Addr E','2026-11-12 15:00:00',7), ('Addr F','2026-11-12 16:00:00',8), ('Addr LATE','2026-11-12 09:00:00',0) v(loc, ts, n);
CALL IP_METADATA_FW.CONTROL.SP_FW_LOAD(5);

ALTER TASK IP_METADATA_FW.CONTROL.TSK_FW_ROOT RESUME;
