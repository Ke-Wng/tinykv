#!/bin/bash
for ((i=1;i<=100;i++));
do
    echo "ROUND $i";
    make project3b > ./out/3b/out-$i.log;
done
