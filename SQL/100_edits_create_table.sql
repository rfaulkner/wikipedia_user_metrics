create table rfaulk.hundredth_edit as

(select rev_user, rev_user_text, count(*) total_revisions, min(rev_timestamp) as first_rev, max(rev_timestamp) as last_rev

from enwiki.revision

where rev_timestamp > '20080101000000' group by 1,2 having total_revisions >= 100);