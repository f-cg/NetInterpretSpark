#!/bin/bash

for i in daim210 daim211 daim212 daim213 daim215 daim131 daim135
do
	(ssh $i pip install --user --proxy http://10.1.170.248:8888 http://download.pytorch.org/whl/cpu/torch-0.4.0-cp27-cp27mu-linux_x86_64.whl  torchvision Pillow)&
done

# 以上还是有些机器安装不上，只能挨个安装

