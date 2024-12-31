#!/bin/bash
for ((i=1;i<=100;i++));
do
    echo "ROUND $i";
    make project2c > ./out/2c/out-$i.log;
done
