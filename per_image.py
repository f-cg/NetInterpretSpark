import numpy
import cv2
from settings import DATA_DIRECTORY
import os
#image,split,ih,iw,sh,sw,color,object,part,material,scene,texture
def per_image(line):
    r=line.split(',')
    assert len(r)==12
    im_name=DATA_DIRECTORY+'images/'+r[0]
    if not os.path.exists(im_name):
        return (im_name,'False')
    im=cv2.imread(im_name)
    return (line,im)