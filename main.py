# encoding=utf-8
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
import settings
import os
from per_image import per_image
from compute_iou import compute_iau
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import pickle
# import pandas as pd
if not os.path.exists(settings.OUTPUT_FOLDER):
    os.makedirs(settings.OUTPUT_FOLDER)

conf = SparkConf().setAppName('NetIntepreter').setMaster('local[3]')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

index_rdd = sc.textFile(settings.INDEX_FILE)
#  得到每张图片在每个神经元上的特征图(list)。features_df(index_line, layer_id, feature_map)
features_rowrdd = index_rdd.flatMap(per_image)
features_df = sqlContext.createDataFrame(features_rowrdd)
features_df.cache()
#  展平特征图中的每个值，便于取top0.5% features_flat(layer_id,v)
features_flat = features_df.rdd.flatMap(
    lambda row: [(row.layer_id, v) for v in row.feature_map]).toDF(['layer_id', 'v'])
features_flat.registerTempTable('features_flat')
#  得到每个神经元的激活阈值  thresholds(layer_id,thresh)
thresholds = sqlContext.sql(
    'select layer_id, percentile_approx(v, 0.995) as thresh from features_flat group by layer_id')
features_thresh = features_df.join(
    thresholds, (features_df.layer_id == thresholds.layer_id), 'inner').drop(thresholds.layer_id)
#  得到每个 神经元概念对 对应的交和并，为了效率，去掉了交为0的那些行
iau = features_thresh.rdd.flatMap(compute_iau).toDF()
# iau.show()
iau.registerTempTable('iau')
iou = sqlContext.sql(
    'select layer_id, concept, sum(i)/sum(u) as iou from iau group by layer_id, concept')
# 保存到hdfs，以后可以加载df = park.read.load('dfs:///target/path/‘)
# iou.write.save('dfs:///target/path/', format='parquet')
# iou.show()
#  取前几名角色
window = Window.partitionBy(iou['layer_id']).orderBy(iou['iou'].desc())
top_iou = iou.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 2)
# top_iou.show() 
# thresholds = df.groupBy('layer_id').agg(statFunc(df).approxQuantile('v', [0.5], 0.1))
# toPandas会先collect到driver_node,保存为pandas的dataframe,便于本地分析
top_iou_pd = top_iou.toPandas()
with open(os.path.join(settings.OUTPUT_FOLDER,'iou_pd_top2'), 'wb') as f:
    pickle.dump(top_iou_pd,f)
