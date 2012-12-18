#!/bin/bash
while read line
do
	c=$(mysql -B -e "SELECT user_id,user_name FROM enwiki.user WHERE user_id = \"$line\" LIMIT 1;" | sed '1d')
	echo $c
done
