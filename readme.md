## 打包

```shell
git clone git@github.com:ohmycloud/sub_trip_with_structured_spark_streaming.git
cd sub_trip_with_structured_spark_streaming
mvn clean package -DskipTests
```

## 发送假数据

打开一个终端, 执行如下命令:

```shell
cd sub_trip_with_structured_spark_streaming
perl6 fake-streaming.pl6
```

## 启动 Structured Spark Streaming 程序

进入到程序所在根目录, 运行如下脚本:

```shell
#!/bin/sh

spark-submit \
  --class ohmysummer.SubTrip \
  --master local[2] \
  --deploy-mode client \
  --driver-memory 2g \
  --driver-cores 2 \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 4 \
  target/sub_trip_with_structured_streaming-1.0-SNAPSHOT.jar
```

观察程序的输出。