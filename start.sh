#!/usr/bin/env bash
nohup /usr/bin/java -jar Alive.jar &>/dev/null &
echo $! > pid