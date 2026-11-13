-- IP-Metadata-Driven-Framework : 04 - onboard each mapping (one-time per entity, idempotent)
--   Creates CORE.<ENTITY>_ACTIVE, CORE.<ENTITY>_HISTORY, CORE.<ENTITY>_FW_WRK and stream <STAGE>_FW_STRM
USE ROLE SYSADMIN;
USE WAREHOUSE IP_MDF_LOAD_WH;

CALL IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(1);
CALL IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(2);
CALL IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(3);
CALL IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(4);
CALL IP_METADATA_FW.CONTROL.SP_FW_ONBOARD(5);

-- First load right after onboarding. A new stream returns only the rows that existed when it was
-- created on its first read, so run this once now; later inserts are picked up by the task graph.
CALL IP_METADATA_FW.CONTROL.SP_FW_LOAD_ALL();
