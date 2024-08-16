#!/bin/zsh

mkdir -p rate

for i in $(seq 0 0.3 6) ; do
    echo -n "$i " >> rate/log
    (time ./nghc-rs optimize -i 50_000.db -o rate/$i -b $i) 2>> rate/log
done
