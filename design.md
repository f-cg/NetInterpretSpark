index(image_path,color,object,part,material,scene,texture)
color图片中的R通道的像素值代表不同的颜色标注，1-11，具体对应的颜色名称见c_color.csv
object图片中的R+256*G代表不同的物体标注，具体对应的名称见c_object.csv
part material同理
scene和texture是整张图片属于某几个标签，index表里只是数字。
多个标签的情况下用;分隔

label(number,name,category)

feature_names(layer_name) #layer_name

map(image, map_values_list_dict)  |  map(image, layer_name_unit_id, map_values)

threshold(layer_name_unit_id, thresh)

index+map+threshold+map => 
iou(layer_name_unit_id, number, IoUkc)



select 
from feature_names,index