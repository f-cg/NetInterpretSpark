from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
import torch
import settings
from itertools import chain
import cv2
from model_loader import loadmodel
import numpy as np
import os
from per_image import per_image
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql import DataFrameStatFunctions as statFunc  

conf = SparkConf().setAppName('NetIntepreter').setMaster('local[3]')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
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
index_file = settings.DATA_DIRECTORY+'small_index_noheader.csv'
if not os.path.exists(index_file):
    print(index_file+' index file not exists!')
    exit(0)
rdd = sc.textFile(index_file)
features = rdd.flatMap(per_image)
features_df = sqlContext.createDataFrame(features)
df = features_df.rdd.flatMap(lambda row: [(row.layer_id, v) for v in chain(*row.feature_map)]).toDF(['layer_id', 'v'])
df.registerTempTable("df")
thresholds = sqlContext.sql("select layer_id, percentile_approx(v, 0.995) as med_val from df group by layer_id")
thresholds.show()

# thresholds = df.groupBy('layer_id').agg(statFunc(df).approxQuantile('v', [0.5], 0.1))

# cl = features.collect()
# print(len(cl))
# print(cl[0])
# print(cl[1])
# print(cl[3])

# sc.textFile("file.csv") .map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
# 对index中的每一行并行处理。
