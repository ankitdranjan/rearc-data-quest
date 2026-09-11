-- unity_catalog_grants.sql
-- Read-only Gold analytics persona.

GRANT USE CATALOG ON CATALOG rearc TO `rearc-analytics`;
GRANT USE SCHEMA ON SCHEMA rearc.gold TO `rearc-analytics`;
GRANT SELECT ON SCHEMA rearc.gold TO `rearc-analytics`;

-- Verification:
-- SHOW GRANTS ON CATALOG rearc;
-- SHOW GRANTS ON SCHEMA rearc.gold;
