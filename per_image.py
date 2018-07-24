import numpy as np
import torch
from PIL import Image
from settings import DATA_DIRECTORY
import os
from model_loader import loadmodel
from torchvision import transforms as trn
from torch.nn import functional as F
from settings import FEATURE_NAMES
from pyspark.sql import Row
from  itertools import chain
# image,split,ih,iw,sh,sw,color,object,part,material,scene,texture


def returnTF():
    # load the image transformer
    tf = trn.Compose([
        trn.Resize((224, 224)),
        trn.ToTensor(),
        trn.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    return tf


def per_image(line):
    # print(line)
    record = line.split(',')
    assert len(record) == 12
    im_name = os.path.join(DATA_DIRECTORY,'images',record[0])
    if not os.path.exists(im_name):
        return (im_name, 'False')
    img = Image.open(im_name)
    features_blobs = []

    def hook_feature(module, input, output):
        features_blobs.append(output.data.cpu().numpy())
    model = loadmodel(hook_feature)
    tf = returnTF()
    imgs = tf(img).unsqueeze(0)
    logit = model(imgs)
    # print(features_blobs[0].shape)
    flat_results = []
    for layer_i, layer_features in enumerate(features_blobs):
        for unit_id, map in enumerate(layer_features[0]):
            flat_results.append(Row(index_line=line, layer_id=FEATURE_NAMES[layer_i]+'_'+str(unit_id), feature_map=list(chain(*(map.tolist())))))

    # print(flat_results)
    # h_x = F.softmax(logit, 1).data.squeeze()
    # print(h_x[46])
    # # return (line,im)
    return flat_results
    # return [Row(layer_id='layer_1', feature_map=[[1.0, 2.0], [3.1, 4.1]]), Row(layer_id='layer_2', feature_map=[[1.0, 2.0], [3.1, 4.1]])]


def per_image_df(row):
    return row


if __name__ == '__main__':
    per_image('opensurfaces/6582.jpg,train,224,224,112,112,opensurfaces/6582_color.png,,,opensurfaces/6582_material.png,,')
