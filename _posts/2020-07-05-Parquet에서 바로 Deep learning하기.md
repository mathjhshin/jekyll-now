---
layout: post
title: Parquet에서 바로 Deep learning하기
---



Big Data를 다룬다면 가장 흔하게 쓰이는 Framework가 Spark입니다.

만약 저처럼 Data Science를 한다면, pyspark를 많이 사용하겠죠.

Spark와 가장 잘 호환되는 Data format은 Parquet이기 때문에, parquet로 Data Wrangling이나 Feature Engineering 작업을 많이 하게 됩니다.

그런데 Parquet File이나 Parquet Table을 Spark Dataframe으로 가져오는 방법은 널리 쓰이는데, Parquet file을 Tensorflow Dataset으로 읽어오거나 Spark Dataframe을 바로 Tensorflow의 Training dataset으로 사용하는 방법은 잘 알려져 있지 않습니다.

Spark로 작업한 Parquet File 또는 Spark Dataframe을 바로 Tensorflow나 Pytorch로 활용하고 싶은 욕구가 있지만, 달리 방도가 떠오르지 않는 것이죠.

저도 이런 고민을 예전에 했었는데, 달리 방법을 모르겠어서 결국 Parquet를 TFRecords로 변경하는 작업을 해준 뒤에 Tensorflow를 썻던 기억이 납니다.

이번 Blog post에서는 Parquet나 Spark Dataframe을 바로 Tensorflow로 data feeding을 할 수 있게 해주는 라이브러리인 Petastorm에 대해 알아보겠습니다.

[TOC]



# 1. Petastorm이란?

Petasotrm이란 Uber 개발자들이 Parquet에서 바로 Deep learning을 학습/평가하고자 하는 목적으로 개발한 open source library입니다.



# 2. Petastorm 사용법 개요

Petasotrm의 사용방법은 크게 두가지 분류가 있는데,

1. Spark Dataframe에서 Data Load를 하는 방법
2. Parquet file로부터 Data Load를 하는 방법

이 있습니다.



여기서 두번째 case의 경우는 다시 또 두가지 분류로 나누어지는데,

1. 임의의 Apache parquet 파일에서 읽는 방법
2. Petastorm parquet 파일에서읽는 방법

이 있습니다.



이렇게 읽은 Data는 Tensorflow, Pytorch, Pyspark로 읽을 수 있다고 하는데, pyspark 로 읽을 수 있는 것은 별로 중요하지 않아 보입니다. 왜냐하면 pyspark는 기본적으로 parquet를 지원하고 parquet를 사용하는 것을 권장하기 때문입니다.

Tensorflow의 경우에는 읽은 Data를 바로 tf.data API로 Data Loading을 할 수 있습니다.

Pytorch의 경우에는 읽은 Data를 바로 torch.DataLoader API로 Data Loading을 할 수 있습니다.

저는 TF 유저이기 때문에 Tensorflow에 대한 예제만 아래에서 다루도록 하겠습니다.





# 3. Petastorm 사용법 상세

### (1) Spark Dataframe에서 Data Load를 하는 방법

Spark Dataframe에서 바로 Data Loading이 되는 것이 아니라, Petastorm에서 파악하는 형태로 cache directory에 parquet file을 우선 저장하고 tf.data API로 data를 feeding하는 형태입니다.

이를 위해선 우선 cache directory를 spark에게 알려주어야 합니다.

여기서 cache directory라 함은 hdfs path를 특정하면 됩니다.

다음의 코드를 통해 알려줄 수 있습니다.

```python
from petastorm.spark import SparkDatasetConverter

# specify a cache dir first.
# the dir is used to save materialized spark dataframe files
cache_dir = 'hdfs:/...'
spark.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF, cache_dir)
```

Deep Learning에 사용할 Spark Dataframe은 이미 준비되었다고 가정하겠습니다.

준비된 Spark Dataframe을 Petastorm에서 파악하는 형태로 cache directory에 parquet file로 저장하기 위해서 Spark Dataset Converter API를 사용하겠습니다.

```python
from petastorm.spark import make_spark_converter

# `df` is a spark dataframe
# create a converter from `df`
# it will materialize `df` to cache dir.
converter = make_spark_converter(df)
```

이렇게 cache directory에 준비된 parquet를 tf.data API로 data loading을 하면 됩니다.

위에서 준비된 converter의 make_tf_dataset 메서드를 사용하면, tf.data.Dataset 객체가 만들어지게 됩니다.

```python
# make a tensorflow dataset from `converter`
with converter.make_tf_dataset() as dataset:
    # the `dataset` is `tf.data.Dataset` object
    # dataset transformation can be done if needed
    dataset = dataset.map(...)
    # we can train/evaluate model on the `dataset`
    model.fit(dataset)
    # when exiting the context, the reader of the dataset will be closed
```

위에서 model은 tf.keras로 만들어졌다고 가정되었습니다.

마지막으로 cached parquet files를 지우기 위해서는 converter의 delete 메서드를 사용하면 됩니다.

```python
# delete the cached files of the dataframe.
converter.delete()
```



### (2) Petastorm parquet 파일에서읽는 방법

우선 Petastorm parquet 파일을 만들어야 합니다.

이를 만드는 방법은 다음과 같습니다.

```python
import numpy as np
from petastorm.unischema import Unischema, UnischemaField, dict_to_spark_row
from petastorm.codec import ScalarCodec, CompressedImageCodec, NdarrayCodec
from petastorm.etl.dataset_metadata import materialize_dataset

HelloWorldSchema = Unischema('HelloWorldSchema', [
   UnischemaField('id', np.int32, (), ScalarCodec(IntegerType()), False),
   UnischemaField('image1', np.uint8, (128, 256, 3), CompressedImageCodec('png'), False),
   UnischemaField('other_data', np.uint8, (None, 128, 30, None), NdarrayCodec(), False),
])


def row_generator(x):
   """Returns a single entry in the generated dataset. Return a bunch of random values as an example."""
   return {'id': x,
           'image1': np.random.randint(0, 255, dtype=np.uint8, size=(128, 256, 3)),
           'other_data': np.random.randint(0, 255, dtype=np.uint8, size=(4, 128, 30, 3))}


def generate_hello_world_dataset(output_url='file:///tmp/hello_world_dataset'):
   rows_count = 10
   rowgroup_size_mb = 256
    
   # spark: SparkSession
   # sc = spark.sparkContext
   # Wrap dataset materialization portion. Will take care of setting up spark environment variables as
   # well as save petastorm specific metadata
   with materialize_dataset(spark, output_url, HelloWorldSchema, rowgroup_size_mb):

       rows_rdd = sc.parallelize(range(rows_count))\
           .map(row_generator)\
           .map(lambda x: dict_to_spark_row(HelloWorldSchema, x))

       spark.createDataFrame(rows_rdd, HelloWorldSchema.as_spark_schema()) \
           .coalesce(10) \
           .write \
           .mode('overwrite') \
           .parquet(output_url)
```

Unischema 오브젝트를 생성하여,  spark의 datatype과 tensorflow의 tf.Dtype, numpy의 np.dtype을 서로 호환되게 합니다. 이렇게 Unischema를 정의할 때, petastorm의 codec과 UnischemaField를 사용합니다.

정의된 Unischema는 rdd를 만들거나 spark Dataframe을 만들 때 meta data로써 사용합니다.

Petastorm parquet file을 저장할 때는 pyspark의 standard한 명령어를 사용하지만, 반드시 materialize_datset이라는 context manager와 함께 사용해야 합니다.



이렇게 만들어진 Petastorm parquet file은 make_reader 함수를 통해 불러올 수 있으며, make_petastorm_dataset 함수를 통해 tf.data로 데이터를 loading시킬 수 있습니다.

```python
from petastorm import make_reader
from petastorm.tf_util import make_petastorm_dataset

 with make_reader('hdfs://myhadoop/some_dataset') as reader:
    dataset = make_petastorm_dataset(reader)
    # the `dataset` is `tf.data.Dataset` object
    # dataset transformation can be done if needed
    dataset = dataset.map(...)
    # we can train/evaluate model on the `dataset`
    model.fit(dataset)
```



### (3) 임의의 Apache parquet 파일에서 읽는 방법

임의의 parquet file을 읽는 방법은 Petastorm parquet file을 읽는 방법과 비슷하나, make_reader 함수가 아니라 make_batch_reader 함수를 사용합니다. 그러나 make_reader와 make_batch_reader는 다음과 같은 차이점이 있습니다.

| make_reader                                                  | make_batch_reader                                            |
| ------------------------------------------------------------ | :----------------------------------------------------------- |
| Only Petastorm datasets (created using materializes_dataset) | Any Parquet store (some native Parquet column types are not supported yet. |
| The reader returns one record at a time.                     | The reader returns batches of records. The size of the batch is not fixed and defined by Parquet row-group size. |
| Predicates passed to `make_reader` are evaluated per single row. | Predicates passed to `make_batch_reader` are evaluated per batch. |



# 4. 마치며

본 포스팅은 사용기는 아니며, 사용 방법에 대한 Research 결과입니다.

사용을 하려면 아무래도 Spark가 설치된 Cluster 환경에서 tutorial을 진행해야 하기 때문에, 좀 더 준비가 필요할 것 같습니다.

사용을 해본 뒤에는 Petastorm 사용기를 Posting하도록 하겠습니다.





# Reference

[https://github.com/uber/petastorm](https://github.com/uber/petastorm)

[https://petastorm.readthedocs.io/en/latest/](https://petastorm.readthedocs.io/en/latest/)

[https://docs.microsoft.com/ko-kr/azure/databricks/applications/deep-learning/data-prep/petastorm](https://docs.microsoft.com/ko-kr/azure/databricks/applications/deep-learning/data-prep/petastorm)

