#!/bin/bash
while read line
do
	c=$(mysql -B -e "SELECT page_id,page_title FROM enwiki.page WHERE page_id = \"$line\" LIMIT 1;" | sed '1d')
	echo $c
done
