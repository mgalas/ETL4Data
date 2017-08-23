INSERT OVERWRITE DIRECTORY '${AUDIT_PATH}'
SELECT printf("%s,%d", description, count(*)) FROM errors GROUP BY error_type,description;
INSERT OVERWRITE DIRECTORY '${STATS_PATH}'
SELECT printf("%s,%s", stat, value) FROM stats;