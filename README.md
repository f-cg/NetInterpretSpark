# 借助spark平台实现Network Dissection

基本原理和流程见https://github.com/CSAILVision/NetDissect-Lite

model_loader.py是直接搬过来的，其它全部重写

## 使用方式

修改settings.py里MODEL_FILE,FEATURE_NAMES等参数为自己所需，其它参数写死，以后根据需要再提供。

    zip py-files.zip *.py

    /home/hadoop/spark/bin/spark-submit --driver-memory 18g --executor-memory 8G --master spark://daim209:7077 --py-files /home/hadoop/fengcg/NetInterpretSparkNew/py-files.zip main.py

集群上的python环境年久失修，经过努力，只在daim209 daim210 daim211 daim212 daim213上安装好了pytorch等模块，daim215和daim131安装和使用torch会出现各种错误，daim135失联。

四台机器作worker node，花费半小时产生针对layer4的512个卷积核的角色。

script文件夹:便于检查节点内存、依赖的python包等情况