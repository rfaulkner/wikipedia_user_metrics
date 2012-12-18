create table e3_global_user_diff
SELECT user_id, gu_timestamp, rg_timestamp, DATEDIFF(gu_timestamp, rg_timestamp) AS delta FROM
(
SELECT a.user_id,
TIMESTAMP(g.gu_registration) AS gu_timestamp,
TIMESTAMP(a.user_registration) AS rg_timestamp
FROM
enwiki.user a
LEFT JOIN globaluser g
ON a.user_name = g.gu_name) AS fff
WHERE gu_timestamp IS NOT NULL;