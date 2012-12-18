
CREATE TABLE `e3_pef_iter2_log_data` (
  `e3pef_project` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_event` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_timestamp` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_user_category` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_user_token` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_namespace` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_lifetime_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_6month_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_3month_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_last_month_edit_count` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_page_id` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_rev_id` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_user_hash` varbinary(255) NOT NULL DEFAULT '',
  `e3pef_time_to_milestone` varbinary(255) DEFAULT NULL,
  `e3pef_revision_measure` varbinary(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=binary