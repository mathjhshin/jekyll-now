---
layout: post
title: Spark에서의 Bucketing
---



Big Data를 다룬다면 가장 흔하게 쓰이는 Framework가 Spark입니다.

최근에는 Big Data를 얼마나 잘 다루냐가, 조금 오바하면 Spark를 얼마나 잘 다루느냐, 인 것 같습니다.

Spark에서 Dataframe API를 사용한다면 Spark 자체가 기본적으로 많은 최적화를 해주지만, 그럼에도 불구하고 사용자가 실제로 데이터를 관찰하면서 최적화해야 하는 부분이 있습니다.

그 중 가장 중요한 것 중 하나는 가능한 한 Shuffle이 발생하는 것을 피하는 것입니다.

그러한 방법 중 하나가 이번에 다룰 주제인 Bucketing입니다.

[TOC]



# 1. Bucketing이란?

Bucketing은 각 파일에 저장된 데이터를 제어할 수 있는 파일 조직화 기법입니다. 이 기법을 사용하면 동일한 버킷 ID를 가진 데이터가 하나의 물리적 파티션에 모두 모여있기 때문에 데이터를 읽을 때 Shuffle을 피할 수 있습니다. 즉, 데이터가 이후의 사용 방식에 맞추어 사전에 파티셔닝되므로 조인이나 집계 시 발생하는 고비용의 Shuffle을 피할 수 있습니다.



# 2. Spark에서의 Bucketing

이러한 Bucketing의 개념은 Hive에도 있습니다.

그러나 Spark의 Bucketing과 Hive의 Bucketing은 약간 다른데,

- Hive는 Bucketing할 컬럼을 지정하면 해당 컬럼에 따라 Bucket이 나누어지지만
- Spark는 Bucketing할 컬럼을 지정하면 Partition마다 Bucket이 나누어지게 됩니다.

즉, 아래의 그림같이 Partition 개수만큼 Bucket이 나누어져 의도했던 것보다 small file이 많아질 수 있습니다.

![_config.yml]({{ site.baseurl }}/images/bucketing_image.png)





# 3. Bucketing을 함으로써 오는 실질적인 이득은?

실질적 이득은 Bucketing을 하지 않은 경우와 한 경우의 Physical Plan을 비교하면 알 수 있습니다.



### (1) Bucketing을 하지 않은 경우

아래는 scala code입니다.

```scala
import org.apache.spark.sql.SaveMode
// Make sure that you don't end up with a BroadcastHashJoin and a BroadcastExchange
// For that, let's disable auto broadcasting
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.range(10e4.toLong).write.mode(SaveMode.Overwrite).saveAsTable("t10e4")
spark.range(10e6.toLong).write.mode(SaveMode.Overwrite).saveAsTable("t10e6")
val t4 = spark.table("t10e4")
val t6 = spark.table("t10e6")
t4.join(t6, "id").foreach(_ => ())
```

이렇게 code를 짠 경우,  SortMergeJoin을 하기 위해 ShuffleExchange가 생기게 되어 Stage가 추가되게 됩니다.

![_config.yml]({{ site.baseurl }}/images/no_use_bucketing.png)

### (2) Bucketing을 한 경우

아래는 scala code입니다.

```scala
import org.apache.spark.sql.SaveMode
// Make sure that you don't end up with a BroadcastHashJoin and a BroadcastExchange
// For that, let's disable auto broadcasting
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.range(10e4.toLong).write.bucketBy(4, "id").sortBy("id").mode(SaveMode.Overwrite).saveAsTable("t10e4")
spark.range(10e6.toLong).write.bucketBy(4, "id").sortBy("id").mode(SaveMode.Overwrite).saveAsTable("t10e6")
val t4 = spark.table("t10e4")
val t6 = spark.table("t10e6")
t4.join(t6, "id").foreach(_ => ())
```

이렇게 code를 짠 경우, Bucketing이 되어 있으므로, ShuffleExchange가 발생하지 않고, 그 결과로 더 적은 Stage로 더 빠르게 SortMergeJoin이 수행 됩니다.

![_config.yml]({{ site.baseurl }}/images/use_bucketing.png)



# 4. Bucketing 사용시 주의점

##### (1) BucketBy method

Bucketing은 DataFrameWriter class의 bucketBy method를 통해 사용할 수 있는데 그 상세는 다음과 같습니다.

```python
bucketBy(numBuckets, col, *cols)
"""
Parameters:
	numBuckets – the number of buckets to save
	col – a name of a column, or a list of names.
	cols – additional names (optional). If col is a list it should be empty.
"""
```

##### (2) Bucketing 이점 적용의 요구사항

Bucketing의 이점을 살리기 위해서는 Bucket의 존재로 인해서 Exchange Query Plan이 나타나선 안 되는데, 이를 위해선 두가지 요구사항을 만족해야 합니다.

1. JOIN 시 Left Dataframe과 Right Dataframe의 partition의 갯수가 일치해야 합니다.
2. Partition은 HashPartitioning Scheme을 사용해야 합니다.

참고로 bucketing이 한 쪽만 되어 있어도 충분히 이득을 볼 수 있습니다. (단 파티션의 갯수는 같아야 합니다.)

##### (3) Bucketing 사용의 주의사항

Bucketing은 DataFrameWriter class의 BucketBy method를 통해 사용할 수 있다고 말씀 드렸지만, DataFrameWriter.save나 DataFrameWriter.insertinto, DataFrameWriter.jdbc 등의 메서드와 함께 사용할 수 없습니다.

버케팅은 Spark Catalog에서 관리되는 테이블에만 적용할 수 있으며, 따라서 DataFrameWriter.saveAsTable 메서드와 함께 사용되게 됩니다.

옵션으로 DataFrameWriter.sortBy method와도 함께 사용할 수 있습니다.

##### (4) Small files problem

초기에 말씀드렸 듯이 Spark에서의 Bucketing을 partition 마다 bucket이 나뉘므로, 예상보다 더 많은 file이 생길 수 있습니다.

예를 들어, partition이 200인데 bucketing을 16으로 주었다면, 200 x 16 = 3200 개의 file이 생기게 됩니다.

이는 유명한 Small files problem을 일으킬 충분한 소지가 됩니다.

##### (5) Bucketing된 파일의 partition 갯수는?

위의 예시에서, partition이 200인데 bucketing을 16으로 주었을 때 200 x 16 = 3200 개의 file이 생기게 되지만, 실제 해당 file로 저장된 Table을 불러와 rdd를 보면 partition 갯수는 16으로 나오게 됩니다.

##### (6) 1 Bucket = 1 Partition으로 만들려면?

이는 제가 직접 실험을 해봤는데,

첫 번째 시도한 방법은 단순히 partition 갯수를 1로 만들고 bucketing을 하여 1 x 16 = 16 이 되게 만드는 방법입니다.

```python
# this is a pyspark code
# df is a dataframe

numBuckets = 16
bucketCol = "id"
df.coalesce(1).write.mode("overwrite").format("parquet").bucketBy(numBuckets, bucketCol).sortBy(bucketCol).saveAsTable("testTable1")
```

code는 잘 작동하나, DataFrameWriter가 1개의 worker Node에서만 작동해서 굉장히 느리게 쓰여졌습니다. 원하는 결과는 얻었지만 속도가 느려 실효성이 없는 방법입니다.

두 번째 시도한 방법은, 미리 bucketing column과 동일한 컬럼을 기준으로 bucket 갯수와 동일한  갯수로 repartition을 하고, 그 후에 bucketing하여 저장하는 방법입니다. 이렇게 하면 16개의 worker node가 데이터를 쓰지만 개별 node에 들어있는 데이터가 정확히 한 bucket만 가지고 있어, 실제로는 1개의 partition에 정확히 1개의 bucket만 들어있는 형태가 되게 됩니다.

```python
# this is a pyspark code
# df is a dataframe

numBuckets = 16
bucketCol = "id"
df.repartition(numBuckets, bucketCol).write.mode("overwrite").format("parquet").bucketBy(numBuckets, bucketCol).sortBy(bucketCol).saveAsTable("testTable1")
```

역시 code는 잘 작동하고, coalesce를 활용한 code보다 훨씬 빠르게 쓰여지는 것을 볼 수 있었습니다.



# 5. 마치며

Bucketing 사용 시 주의사항과 Tip을 자세히 본 것 같은데 아직까지 sortBy를 왜 쓰는지는 명확히 알 수 없었습니다.

다음 Posting에서는 sortBy의 이점에 대해서 알아보도록 하겠습니다.



# Reference

스파크완벽가이드 9장

https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-bucketing.html