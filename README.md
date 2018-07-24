# 借助spark平台实现Network Dissection

基本原理和流程见https://github.com/CSAILVision/NetDissect-Lite

model_loader.py是直接搬过来的，其它全部重写

## 使用方式

修改settings.py里MODEL_FILE,FEATURE_NAMES等参数为自己所需，其它参数写死，以后根据需要再提供。

python3 main.py

## todo

根据需要完善和明确功能选项之后，将其封装为一个模块
