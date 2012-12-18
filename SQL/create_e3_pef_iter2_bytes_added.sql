CREATE TABLE `e3_pef_iter2_bytesadded` (
  `user_id` int(5) unsigned NOT NULL DEFAULT 0,
  `bytes_added_net` varbinary(255) NOT NULL DEFAULT '',
  `bytes_added_abs` varbinary(255) NOT NULL DEFAULT '',
  `bytes_added_pos` varbinary(255) NOT NULL DEFAULT '',
  `bytes_added_neg` varbinary(255) NOT NULL DEFAULT '',
  `edit_count` varbinary(255) NOT NULL DEFAULT '',
  `hour_offset` varbinary(255) NOT NULL DEFAULT ''
) ENGINE=MyISAM DEFAULT CHARSET=binary