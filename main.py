from pyspark import SparkConf, SparkContext, SQLContext
import os
from per_image import per_image_blob
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,dense_rank, col
import pickle
from settings import FEATURE_NAMES, DATA_DIRECTORY, INDEX_FILE, OUTPUT_FOLDER
import numpy as np
import scipy
import scipy.misc
from PIL import Image
from pyspark.sql import Row

#conf = SparkConf().setAppName('NetIntepreter').setMaster('spark://daim210:7077').set('spark.executor.cores', '2').set('spark.executor.memory', '4G').set('spark.driver.memory','12G').set("spark.sql.broadcastTimeout",  '3000')
#sc = SparkContext(conf=conf)
sc = SparkContext()
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")
## features_df(index_line, layer_id, feature_map)
features_rowrdd = sc.textFile(INDEX_FILE, minPartitions=69999).map(per_image_blob)
features_df = sqlContext.createDataFrame(features_rowrdd)
features_df.persist()
## features_flat(layer_id,v)
features_flat = features_df.rdd.flatMap(lambda row: [(FEATURE_NAMES[layer_idx]+'_'+str(unit_idx), v) for layer_idx, layer in enumerate(row.blob) for pic in layer for unit_idx, unit in enumerate(pic) for h in unit for v in h]).toDF(['layer_id', 'v'])
features_flat.registerTempTable('features_flat')
##  thresholds(layer_id,thresh)
thresholds = sqlContext.sql('select layer_id, percentile_approx(v, 0.995) as thresh from features_flat group by layer_id')
#thresholds.show()
thresholds_dict = sc.broadcast(thresholds.toPandas().set_index('layer_id').T.to_dict('layer_id'))
with open(os.path.join(OUTPUT_FOLDER,'thre_dict_v'), 'wb') as f:
        pickle.dump(thresholds_dict.value,f)
def compute_iau_blob(row):
    record = row.index_line.split(',')
    cols = ['image','split','ih','iw','sh','sw','color','object','part','material','scene','texture']
    mw,mh = int(record[cols.index('sw')]), int(record[cols.index('sh')])
    import scipy.misc
    Ms = [(FEATURE_NAMES[layer_idx]+'_'+str(unit_idx), scipy.misc.imresize(np.array(unit,dtype=np.uint8), (mh,mw)) > (thresholds_dict.value[FEATURE_NAMES[layer_idx]+'_'+str(unit_idx)])) for layer_idx, layer in enumerate(row.blob) for pic in layer for unit_idx, unit in enumerate(pic)]
    results=[]
    size = mh*mw
    cs = record[6:]
    for i, r in enumerate(cs):
        if r=='':
            continue
        es = r.split(';')
        for i, e in enumerate(es):
            if e.isdigit():
                for idx, M in Ms:
                    intersect = int(np.sum(M))
                    union = size
                    # assert(intersect<=union)
                    results.append(Row(**{'layer_id':idx, 'concept':int(e),'i':intersect,'u':union}))
            else:
                im_name = os.path.join(DATA_DIRECTORY,'images',e)
                concepts_img = np.array(Image.open(im_name))
                # assert(concepts_img.shape==(112,112,3))
                concepts_img = np.array([[y[0]+y[1]*256 for y in x] for x in concepts_img])
                for c in np.lib.arraysetops.unique(concepts_img):
                    if c==0:
                        continue
                    L = concepts_img==c
                    for idx, M in Ms:
                        intersect = int(np.sum(M*L))
                        union = int(np.sum(M+L))
                        # assert(intersect<=union)
                        results.append(Row(**{'layer_id':idx, 'concept':int(c),'i':intersect,'u':union}))
    return results

## iau(layer_id, concept, i, u)thresholds_dict
iau = features_df.rdd.flatMap(compute_iau_blob).toDF()

iau.registerTempTable('iau')
iou = sqlContext.sql('select layer_id, concept, sum(i)/sum(u) as iou from iau group by layer_id, concept')
#  top iou rate
window = Window.partitionBy(iou['layer_id']).orderBy(iou['iou'].desc())
top_iou = iou.select('*', dense_rank().over(window).alias('rank')).filter(col('rank') <= 2)
top_iou.show()
# toPandas will collect dataframe to driver_node first and then save it to dataframe of pandas for local analysis
top_iou_pd = top_iou.toPandas()
with open(os.path.join(OUTPUT_FOLDER,'iou_pd_top'), 'wb') as f:
    pickle.dump(top_iou_pd,f)
