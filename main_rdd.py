from pyspark import SparkConf,SparkContext
import torch
import settings
from fo import hook_feature
import cv2
from model_loader import loadmodel
import numpy as np
import os
from per_image import per_image
conf = SparkConf().setAppName('NetIntepreter').setMaster('local[3]')
sc = SparkContext(conf=conf)
# model=loadmodel(hook_feature)
# im=cv2.imread('/home/lily/py/data/broden1_224/images/10038.jpg')
# im=np.transpose(im,[2,1,0])
# im=im[np.newaxis,:,:,:]
# input=torch.from_numpy(im)
# input=input.type(torch.FloatTensor)
# print(input.size())
# out=model(input)
# print(out)
# print(out.size())
index_file=settings.DATA_DIRECTORY+'small_index.csv'
if not os.path.exists(index_file):
    print(index_file+' index file not exists!')
    exit(0)
rdd = sc.textFile(index_file)
cl = rdd.map(per_image).collect()
print(len(cl))
print(cl)

# sc.textFile("file.csv") .map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
# 对index中的每一行并行处理。