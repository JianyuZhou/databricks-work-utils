SELECT NOW() as check_time, name, hex(metastore_id), created_at, updated_at, "catalog" as securable_type
FROM mc_catalogs
WHERE deleted=0 and name like "%databricks%"
UNION ALL
SELECT NOW() as check_time, name, hex(metastore_id), created_at, updated_at, "share" as securable_type
FROM mc_shares
WHERE deleted=0 and name like "%databricks%"
UNION ALL
SELECT NOW() as check_time, name, hex(metastore_id), created_at, updated_at, "recipient" as securable_type
FROM mc_recipients
WHERE deleted=0 and name like "%databricks%";