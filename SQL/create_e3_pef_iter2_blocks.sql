CREATE TABLE `e3_pef_iter2_blocks` (
  `user_id` int(5) unsigned NOT NULL DEFAULT 0,
  `block_count` varbinary(255) NOT NULL DEFAULT '',
  `first_block` varbinary(255) NOT NULL DEFAULT '',
  `last_block` varbinary(255) NOT NULL DEFAULT '',
  `ban` varbinary(255) NOT NULL DEFAULT ''
) ENGINE=MyISAM DEFAULT CHARSET=binary