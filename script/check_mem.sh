#!/bin/bash

for i in daim209 daim210 daim211 daim212 daim213 daim215 daim131 daim135
do
    echo -n $i:
	ssh $i cat /proc/meminfo |grep MemFree 
done
