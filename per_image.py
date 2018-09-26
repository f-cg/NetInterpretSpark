from PIL import Image
from settings import DATA_DIRECTORY
import os
from model_loader import loadmodel
from torchvision import transforms as trn
from pyspark.sql import Row
# image,split,ih,iw,sh,sw,color,object,part,material,scene,texture

def returnTF():
    # load the image transformer
    tf = trn.Compose([
        trn.Resize((224, 224)),
        trn.ToTensor(),
        trn.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    return tf

def per_image_blob(line):
    # print(line)
    record = line.split(',')
    assert len(record) == 12
    im_name = os.path.join(DATA_DIRECTORY,'images',record[0])
    if not os.path.exists(im_name):
        return (im_name, 'False')
    img = Image.open(im_name)
    features_blobs = []

    def hook_feature(module, input, output):
        features_blobs.append(output.data.cpu().numpy().tolist())
    model = loadmodel(hook_feature)
    tf = returnTF()
    imgs = tf(img).unsqueeze(0)
    logit = model(imgs)
    # print(len(features_blobs))
    # print(len(features_blobs[0]))
    # print(len(features_blobs[0][0]))
    # print(len(features_blobs[0][0][0]))
    # print(len(features_blobs[0][0][0][0]))
    #import pickle
    #with open('blob', 'wb') as f:
    #    pickle.dump(features_blobs,f)
    #exit(0)
    return Row(index_line=line, blob=features_blobs)

if __name__ == '__main__':
    per_image('opensurfaces/6582.jpg,train,224,224,112,112,opensurfaces/6582_color.png,,,opensurfaces/6582_material.png,,')