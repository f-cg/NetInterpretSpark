#!/bin/bash

for i in daim210 daim211 daim212 daim213 daim215 daim131 daim135
do
	(ssh $i pip install --user http://download.pytorch.org/whl/cpu/torch-0.4.0-cp27-cp27mu-linux_x86_64.whl  torchvision Pillow)&
done

