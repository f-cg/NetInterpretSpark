 Spark version 2.3.1 java8
 ## shell
 ./bin/spark-shell --master local[N] run locally with N threads
 ./bin/spark-shell --master spark://IP:PORT

 ## standalone
 ./sbin/start-master.sh -h <host-ip> 打印出master的ip和port。还可以控制log文件夹，内存使用量，配置文件等
 ./sbin/start-slave.sh <master-spark-URL>