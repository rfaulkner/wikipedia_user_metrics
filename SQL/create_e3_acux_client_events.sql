CREATE TABLE `e3_acux1_client_events` (
  `e3_acux_project` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_event` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_timestamp` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_user_category` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_user_token` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_namespace` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_lifetime_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_6month_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_3month_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_last_month_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_hash` varbinary(255) NOT NULL DEFAULT '',
  `e3_acux_referrer` varbinary(10000) NOT NULL DEFAULT ''
) ENGINE=MyISAM DEFAULT CHARSET=binary