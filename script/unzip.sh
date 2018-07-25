#!/bin/bash

for i in daim210 daim211 daim212 daim213 daim215 daim131 daim135
do
	(ssh $i unzip ~/fengcg/data/broden1_224.zip -d ~/fengcg/data/ >/dev/null)&
done
