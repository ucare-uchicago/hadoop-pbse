#!/bin/bash

if [ -z "$1" ]
  then
    echo "Need log name"
    exit
fi

dt=$(date '+%Y%m%d_%H%M%S');
temp="logs_${dt}_${1}"
echo $temp

cd /tmp
mergelogs
mv logs "$temp"

tar -cvzf "$temp.tgz" "$temp" 1> /dev/null
mv "$temp.tgz" ~/
rm -rf "$temp"
