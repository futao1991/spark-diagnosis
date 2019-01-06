本项目主要应用于spark诊断，可以诊断出spark作业运行过程中出现的性能问题
包括:
1. 长尾task
2. stage中数据倾斜
3. task gc过度
4. 调度延迟

同时可以采集executor信息，便于分析作业运行时executor内存使用走势

#### 使用方法
执行mvn clean package
得到spark-diagnosis-1.0-SNAPSHOT.jar包
将该jar包上传至hdfs路径上，设为/user/hive/spark/spark-diagnosis-1.0-SNAPSHOT.jar

提交spark作业时，添加以下配置:

spark.extraListeners=org.apache.spark.diagnosis.listener.SparkAppListener

spark.diagnosis.jvm.path=/user/hive/spark/executor_metrics

spark.jars=hdfs://xxx/user/hive/spark/spark-diagnosis-1.0-SNAPSHOT.jar

spark.executor.extraJavaOptions="-javaagent:spark-diagnosis-1.0-SNAPSHOT.jar=reporter=cn.fraudmetrix.spark.metrics.HdfsOutputReporter,metricInterval=5000,appIdPath=/user/hive/spark/executor_metrics"

注: /user/hive/spark/executor_metrics为保存executor运行信息的目录
