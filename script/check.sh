#!/bin/bash

for i in daim210 daim211 daim212 daim213 daim215 daim131 daim135
do
    echo -n $i:
    ssh $i ls -lh ~/fengcg/data | awk '{print $5,$9}'| tr "\n" " ";echo ' ' 
done
