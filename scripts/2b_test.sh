#!/bin/bash
for ((i=1;i<=100;i++));
do
    echo "ROUND $i";
    make project2b > ./out/2b/out-$i.log;
done
