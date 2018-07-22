报错显示worker和driver的python版本不通：
在.bashrc中添加一行：
export PYSPARK_PYTHON=/usr/bin/python3


imresize uses PIL or Pillow to do the actual work. It is the conversion to a PIL image with mode 'L' that triggers the rescaling of the data values. If the input data type is not 8 bit, the values are scaled to fill the 8 bit range.
One way to avoid this is to ensure that the input array has data type numpy.uint8. Then the values are not rescaled.

Why do Python modules sometimes not import their sub-modules: 
https://stackoverflow.com/questions/3781522/why-do-python-modules-sometimes-not-import-their-sub-modules