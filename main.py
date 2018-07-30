from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
import settings
import os
from per_image import per_image
from compute_iou import compute_iau
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import pickle

conf = SparkConf().setAppName('NetIntepreter').setMaster('spark://daim209:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
#sc.setLogLevel("ERROR")
index_rdd = sc.textFile(settings.INDEX_FILE, minPartitions=1000)
#print(index_rdd.collect())
#exit(0)
# features_df(index_line, layer_id, feature_map)
features_rowrdd = index_rdd.flatMap(per_image)
#print(features_rowrdd.collect())
#exit(0)
features_df = sqlContext.createDataFrame(features_rowrdd)
#features_df.registerTempTable('features_df')
#tmp = sqlContext.sql('select layer_id, count(*) as s from features_df group by layer_id')
#tmpdf=tmp.toPandas()
#with open(os.path.join(settings.OUTPUT_FOLDER,'iou_pd_top2'), 'wb') as f:
#    pickle.dump(tmpdf,f)
#exit(0)
#features_df.cache()
## features_flat(layer_id,v)
features_flat = features_df.rdd.flatMap(
    lambda row: [(row.layer_id, v) for v in row.feature_map]).toDF(['layer_id', 'v'])
features_flat.registerTempTable('features_flat')
#  thresholds(layer_id,thresh)
thresholds = sqlContext.sql(
    'select layer_id, percentile_approx(v, 0.995) as thresh from features_flat group by layer_id')
#thresholds_pd = thresholds.toPandas()
#with open(os.path.join(settings.OUTPUT_FOLDER,'iou_pd'), 'wb') as f:
#        pickle.dump(thresholds_pd,f)
#exit(0)
features_thresh = features_df.join(
    thresholds, (features_df.layer_id == thresholds.layer_id), 'inner').drop(thresholds.layer_id)
#features_thresh_pd = features_thresh.toPandas()
#with open(os.path.join(settings.OUTPUT_FOLDER,'features_thresh'), 'wb') as f:
#        pickle.dump(features_thresh_pd,f)
#exit(0)


#features_df.registerTempTable('features_df')
#thresholds.registerTempTable('thresholds')
#features_thresh = sqlContext.sql('select index_line, features_df.layer_id, feature_map, thresh from features_df, thresholds where features_df.layer_id == thresholds.layer_id')
if features_thresh.rdd.getNumPartitions()<3000:
    features_thresh=features_thresh.repartition(4000)
iau = features_thresh.rdd.flatMap(compute_iau).toDF()
#iau = features_thresh.repartition(1000).rdd.flatMap(compute_iau).collect()
#print(iau)
#exit(0)
#iau_pd = iau.toPandas()
#with open(os.path.join(settings.OUTPUT_FOLDER,'iau'), 'wb') as f:
#        pickle.dump(iau_pd,f)
#exit(0)
iau.registerTempTable('iau')
iou = sqlContext.sql(
    'select layer_id, concept, sum(i)/sum(u) as iou from iau group by layer_id, concept')
# save this to hdfs. df = park.read.load('dfs:///target/path/)
# iou.write.save('dfs:///target/path/', format='parquet')
# iou.show()
#  top iou rate
window = Window.partitionBy(iou['layer_id']).orderBy(iou['iou'].desc())
top_iou = iou.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 2)
# top_iou.show() 
# thresholds = df.groupBy('layer_id').agg(statFunc(df).approxQuantile('v', [0.5], 0.1))
# toPandas will collect dataframe to driver_node first and then save it to dataframe of pandas for local analysis
top_iou_pd = top_iou.toPandas()
with open(os.path.join(settings.OUTPUT_FOLDER,'iou_pd_top2'), 'wb') as f:
    pickle.dump(top_iou_pd,f)

'''
/home/hadoop/spark/bin/spark-submit --master spark://daim209:7077 --py-files /home/hadoop/fengcg/NetInterpretSpark/*

/home/hadoop/spark/bin/spark-submit --master spark://daim209:7077 --py-files /home/hadoop/fengcg/NetInterpretSpark/pyfiles.zip main.py
'''
