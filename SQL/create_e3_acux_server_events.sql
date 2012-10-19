CREATE TABLE `e3_acux1_server_events` (
  `project` varbinary(255) NOT NULL DEFAULT '',
  `username` varbinary(255) NOT NULL DEFAULT '',
--   `userbuckets` varbinary(255) NOT NULL DEFAULT '',
  `user_id` varbinary(255) NOT NULL DEFAULT '',
  `timestamp` varbinary(255) NOT NULL DEFAULT '',
  `event_id` varbinary(255) NOT NULL DEFAULT '',
  `self_made` varbinary(255) NOT NULL DEFAULT '',
  `mw_user_token` varbinary(255) NOT NULL DEFAULT '',
  `version` varbinary(255) NOT NULL DEFAULT '',
  `by_email` varbinary(255) NOT NULL DEFAULT '',
  `creator_user_id` varbinary(255) NOT NULL DEFAULT ''
) ENGINE=MyISAM DEFAULT CHARSET=binary