######### global settings  #########
GPU = False                                  # running on GPU is highly suggested
# TEST_MODE = False                           # turning on the testmode means the code will run on a small dataset.
# CLEAN = True                               # set to 'True' if you want to clean the temporary large files after generating result
MODEL = 'resnet18'                          # model arch: resnet18, alexnet, resnet50, densenet161
# DATASET = 'places365'                       # model trained on: places365 or imagenet
# QUANTILE = 0.005                            # the threshold used for activation
# SEG_THRESHOLD = 0.04                        # the threshold used for visualization
# SCORE_THRESHOLD = 0.04                      # the threshold used for IoU score (in HTML file)
# TOPN = 10                                   # to show top N image with highest activation for each unit
# PARALLEL = 1                                # how many process is used for tallying (Experiments show that 1 is the fastest)
# CATAGORIES = ['object', 'part','scene','texture','color'] # concept categories that are chosen to detect: 'object', 'part', 'scene', 'material', 'texture', 'color'

# PREFIX='/home/lily/py/'
PREFIX='/home/hadoop/fengcg/'
OUTPUT_FOLDER = PREFIX+'result/' # result will be stored in this folder
MODEL_FILE = PREFIX+'data/resnet18_places365.pth.tar'
FEATURE_NAMES = ['layer4']
DATA_DIRECTORY = PREFIX+'data/broden1_224/'
# INDEX_FILE = '/home/lily/py/data/small_index_noheader.csv'
#INDEX_FILE = '/home/hadoop/fengcg/data/broden1_224/small_index_noheader.csv'
INDEX_FILE = 'hdfs://daim209:9000/fengcg/data/index9_noheader.csv'
NUM_CLASSES = 365
MODEL_PARALLEL = True
# WORKERS = 12
# BATCH_SIZE = 128
# TALLY_BATCH_SIZE = 16
# TALLY_AHEAD = 4

# hdfs dfs -put small_index.csv small_index_noheader.csv index.csv /fengcg/data