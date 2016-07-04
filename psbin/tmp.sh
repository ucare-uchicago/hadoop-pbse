#!/bin/bash

cd $TESTDIR/workGenLogs/
cp $PSBIN/genstats.py ./
sed -i "s/^SLOWNODE=.*/SLOWNODE=$1/" genstats.py
sed -i "s/^SLOWHOST=.*/SLOWHOST="\""$2"\""/" genstats.py
