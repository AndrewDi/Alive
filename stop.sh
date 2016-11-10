#!/usr/bin/env bash
[ -f pid ] && cat pid|awk '{print "kill -9 "$1}'|sh -x