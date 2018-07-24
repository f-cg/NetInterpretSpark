import numpy as np
import scipy
import scipy.misc
import math
from pyspark.sql import Row
from settings import DATA_DIRECTORY
from PIL import Image
import os

#feature_map|          index_line|  layer_id|            thresh
def compute_iau(row):
    record = row.index_line.split(',')
    feature_map = row.feature_map
    cols = ['image','split','ih','iw','sh','sw','color','object','part','material','scene','texture']
    thresh = row.thresh
    a = np.array(feature_map,dtype=np.uint8).reshape((int(math.sqrt(len(feature_map))),-1))
    S=scipy.misc.imresize(a, (int(record[cols.index('sh')]),int(record[cols.index('sw')])))
    M = S>thresh
    results=[]
    size = S.size
    esum = int(np.sum(M))
    if esum==0:
        return results
    cs = record[6:]
    for i, r in enumerate(cs):
        if r=='':
            continue
        es = r.split(';')
        for i, e in enumerate(es):
            if e.isdigit():
                intersect = esum
                union = size
                assert(intersect<=union)
                results.append(Row(**{'layer_id':row.layer_id, 'concept':int(e),'i':intersect,'u':union}))
            else:
                im_name = os.path.join(DATA_DIRECTORY,'images',e)
                concepts_img = np.array(Image.open(im_name))
                assert(concepts_img.shape==(112,112,3))
                concepts_img = np.array([[y[0]+y[1]*256 for y in x] for x in concepts_img])
                for c in np.unique(concepts_img):
                    if c==0:
                        continue
                    L = concepts_img==c
                    intersect = int(np.sum(M*L))
                    if intersect==0:
                        continue
                    union = int(np.sum(M+L))
                    assert(intersect<=union)
                    results.append(Row(**{'layer_id':row.layer_id, 'concept':int(c),'i':intersect,'u':union}))

    return results