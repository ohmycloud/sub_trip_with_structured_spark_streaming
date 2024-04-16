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

# 模拟发送历史积压数据（会输出 2 个行程, 还有一个因为超时而结束的行程）
raku trips.raku --file=multi-trips.txt

+-----------------+-------------+-------------+------------+----------+------------+------------+-----------+
|vin              |tripStartTime|tripEndTime  |startMileage|endMileage|tripDuration|tripDistance|isTripEnded|
+-----------------+-------------+-------------+------------+----------+------------+------------+-----------+
|LSJA0000000000092|1713258259878|1713258318369|0           |58        |58          |58          |true       |
|LSJA0000000000092|1713258439034|1713258498465|59          |118       |59          |59          |true       |
+-----------------+-------------+-------------+------------+----------+------------+------------+-----------+

LSJA0000000000092 timeout with state: {Some(TripState(1713258619231,1713258678749,119,178))}

+-----------------+-------------+-------------+------------+----------+------------+------------+-----------+
|vin              |tripStartTime|tripEndTime  |startMileage|endMileage|tripDuration|tripDistance|isTripEnded|
+-----------------+-------------+-------------+------------+----------+------------+------------+-----------+
|LSJA0000000000092|1713258619231|1713258678749|119         |178       |59          |59          |true       |
+-----------------+-------------+-------------+------------+----------+------------+------------+-----------+

# 不模拟发送历史积压数据
raku trips.raku
```

## 启动 Structured Spark Streaming 程序

进入到程序所在根目录, 运行如下脚本:

```shell
#!/bin/sh

spark-submit \
  --class com.gac.x9e.SubTripApp \
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

- StatefulSessionApp

```bash
cd sub_trip_with_structured_spark_streaming
raku last-user.raku
```

运行 StatefulSessionApp, 观察结果。