#!/bin/bash

DBFILE="/tmp/guiledbitest.db"

rm -f $DBFILE
sqlite3 $DBFILE < a.sql
guile test.scm > test.out
if ! diff test.out test.out.expected; then
	echo "Test failed"
	exit 1
fi
