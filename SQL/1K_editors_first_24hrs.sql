select

user_id,

user_name,

date_format(r.rev_timestamp, '%Y-%m-%d %H') as hour,

TIMESTAMPDIFF(HOUR,a.user_registration,rev_timestamp) as hour_since_reg,

count(*) as revs

from (select user_id, user_name, user_registration, date_format(ADDTIME(user_registration, '1 00:00:00'),'%Y%m%d%H%i%s') as one_day_later from enwiki.user where user_name in (select the_user_name from rfaulk.thousandth_edit) and not(isnull(user_registration))) as a

join enwiki.revision as r on r.rev_user = a.user_id

where r.rev_timestamp >= a.user_registration and r.rev_timestamp < a.one_day_later

group by 1,2,3;