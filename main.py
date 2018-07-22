from pyspark import SparkConf, SparkContext, SQLContext
import pyspark
import settings
import os
from per_image import per_image
from compute_iou import compute_iau

conf = SparkConf().setAppName('NetIntepreter').setMaster('local[3]')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

index_file = settings.DATA_DIRECTORY+'small_index_noheader.csv'
if not os.path.exists(index_file):
    print(index_file+' index file not exists!')
    exit(0)
index_rdd = sc.textFile(index_file)
#  得到每张图片在每个神经元上的特征图(list)。features_df(index_line, layer_id, feature_map)
features_rowrdd = index_rdd.flatMap(per_image)
features_rowrdd.cache()
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
iou.show()
# thresholds = df.groupBy('layer_id').agg(statFunc(df).approxQuantile('v', [0.5], 0.1))
