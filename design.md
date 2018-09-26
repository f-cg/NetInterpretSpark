index(image_path,color,object,part,material,scene,texture)
color图片中的R通道的像素值代表不同的颜色标注，1-11，具体对应的颜色名称见c_color.csv
object图片中的R+256*G代表不同的物体标注，具体对应的名称见c_object.csv
part material同理
scene和texture是整张图片属于某几个标签，index表里只是数字。
多个标签的情况下用;分隔

像素标注数据在每个节点上都存在，只有index文件在dfs上，为了保证访问速度和便于python读取。

## 表及行数变化
image,split,ih,iw,sh,sw,color,object,part,material,scene,texture
index.csv 9
---per_image_blob---
features_df(index_line, blob) 9
    features_flat(layer_id,v) 225792
thresholds(layer_id,thresh) 512 =>thresholds_dict broadcast

iau(layer_id, concept, i, u) 58368
iou(layer_id, concept, iou)