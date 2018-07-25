#!/bin/bash

for i in daim210 daim211 daim212 daim213 daim215 daim131 daim135
do
	ssh $i 'mkdir -p /home/hadoop/fengcg/data'
	scp ~/fengcg/data/broden1_224.zip $i:~/fengcg/data &
done
