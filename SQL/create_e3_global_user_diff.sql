
create table e3_global_user_diff
SELECT user_id, gu_timestamp, rg_timestamp, DATEDIFF(gu_timestamp, rg_timestamp) AS delta FROM
(
SELECT a.user_id, a.user_name, a.user_editcount AS lifetime_edits,
(SELECT (CASE WHEN SUM(contribs) IS NULL THEN 0 ELSE SUM(contribs) END) FROM enwiki.user_daily_contribs u WHERE u.user_id = a.user_id AND DATEDIFF(u.day, a.user_registration) <= 1) AS 1d_edits,
(SELECT (CASE WHEN SUM(contribs) IS NULL THEN 0 ELSE SUM(contribs) END) FROM enwiki.user_daily_contribs u WHERE u.user_id = a.user_id AND DATEDIFF(u.day, a.user_registration) <= 7) AS 1w_edits,
(SELECT (CASE WHEN SUM(contribs) IS NULL THEN 0 ELSE SUM(contribs) END) FROM enwiki.user_daily_contribs u WHERE u.user_id = a.user_id AND DATEDIFF(u.day, a.user_registration) <= 14) AS 2w_edits,
TIMESTAMP(g.gu_registration) AS gu_timestamp, TIMESTAMP(a.user_registration) AS rg_timestamp, TIMESTAMP(b.ept_timestamp) AS et_timestamp, TIMESTAMP(c.mbf_timestamp) AS mb_timestamp
FROM
enwiki.user a
LEFT JOIN
enwiki.edit_page_tracking b
ON
a.user_id = b.ept_user
LEFT JOIN
enwiki.moodbar_feedback c
ON
a.user_id = c.mbf_user_id
LEFT JOIN
globaluser g
ON
a.user_name = g.gu_name) AS fff WHERE gu_timestamp IS NOT NULL AND DATEDIFF(gu_timestamp, rg_timestamp) < -1;
