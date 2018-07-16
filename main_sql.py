from pyspark import SparkConf,SparkContext
import torch
import settings
from fo import hook_feature
import cv2
from model_loader import loadmodel
import numpy as np
import os
from per_image import per_image
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

index_file=settings.DATA_DIRECTORY+'small_index.csv'
if not os.path.exists(index_file):
    print(index_file+' index file not exists!')
    exit(0)
df = spark.read.csv(index_file,schema='image string,split string,ih int,iw int,sh int,sw int,color string,object string,part string, material string,scene int,texture int', header=True)
cl = df.rdd.collect()
print(type(cl))
print(type(cl[0]))
print(type(cl[0]['sh']))
# sc.textFile("file.csv") .map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
# 对index中的每一行并行处理。