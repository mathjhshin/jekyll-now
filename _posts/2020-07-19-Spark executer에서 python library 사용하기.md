---
layout: post
title: Spark executer에서 python library 사용하기
---



Spark는 기능이 적기 때문에 반드시 보조적인 library와 함께 사용해야 합니다.

그런데 driver에서 library를 사용하는 것은 비교적 쉽고 간단한데, excuter에서도 python lirbary를 사용하려면 어떻게 해야 할까요?

특히 제가 data center 관리자가 아니라서 개별 executer에 python library를 수동으로 설치할 수 없다고 가정해봅시다.

그렇다면 제 personal python 환경을 어떻게 개별 executer에도 동일한 환경으로 구성해 code를 돌릴 수 있을까요?

이번에는 spark executer에서 python library를 사용하는 법을 알아보도록 하겠습니다.



[TOC]



# 1. spark-submit이란?

일반적으로 Spark를 이용한 ML model을 개발할 때는 Zepplin을 많이 이용할 것 같습니다. Zepplin이나 pyspark-shell과 같은 대화형 셸은 model 개발을 할 때는 좋으나 운영을 할 때는 더 안정적인 spark-submit을 추천합니다.

특히 이번 주제와 같이, client 사용자가 cluster의 여러 executer에 본인의 personal python 환경을 보내려는, 어떻게 보면 위험한 일을 하려고 할 때는 반드시 spark-submit을 사용해야 합니다.

python에서 spark-submit을 사용하려면 아래와 같은 제한이 있습니다.

(1) scala나 java의 main class와 같은 역할을 하는 python py file을 작성하고, 해당 file은 SparkSession을 생성하는 실행 가능한 스크립트 파일이어야 합니다. 다음은 예시입니다.

```python
from __future__ import print_function

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    print(spark.rabe(5000).where("id > 500").selectExpr("sum(id)").collect())
```

(2) 사용한 라이브러리나 custom module을 하나의 egg나 zip 파일 형태로 압축합니다. 이는 spark-submit의 --py-files 인수로 사용되게 됩니다



# 2. 만약 driver의 package를 별도의 설정 없이 사용하게 되면?

예시로서 driver에 tensorflow 2.2를 설치했고 그것을 pyspark에서 아래와 같이 사용했다고 해봅시다.

```python
import numpy as np
import pandas as pd
import tensorflow as tf
from pyspark import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def _float_feature(value):
  return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

def _int64_feature(value):
  return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

def serialize_example(x):
    feature = {"id": _int64_feature(x[0]), "some_value": _float_feature(x[1])}
    example_proto = tf.train.Example(features=tf.train.Features(feature=feature))
    return example_proto.SerializeToString()

row = Row("id", "some_value")
df = sc.parallelize([row(1, 20.0), row(1, 10.0), row(2, 25.0), row(2, 30.0)]).toDF()
df = df.select(F.struct(F.col("id"), F.col("some_value")).alias("tf_data"))
udf_fn = F.udf(serialize_example, returnType=StringType())
df = df.select(udf_fn("tf_data").alias("tfrecord_string"))
```

위 code는 spark dataframe의 data를 tfrecord로 직렬화하는 예제입니다.

만약 spark로 feature engineering을 했고, Tensorflow로  Deep Learning을 하려고 한다면, 위와 같은 code가 도움이 되겠죠.

그러나 저는 위 코드를 Cluster 환경에서 실행시켰을 때

ModuleNotFoundError: No module named 'tensorflow'

라는 에러 메시지와 함께 잘 동작하지 않았습니다.

반면 저의 Local Pyspark 환경에서 실행했을 때에는 오류 없이 잘 작동했습니다.

따라서 이는 executer에서 tensorflow 환경 설정이 되지 않아서 생긴 오류임이 분명했습니다.





# 3. Custom Python 환경을 executer에서 사용하기

아래 예시는 conda를 이용하여 만든 가상환경을 executer로 보내는 예제입니다.

conda-pack을 이용하여 가상환경을 tarball file로 만들어냅니다.

```sh
VENV_NAME="my-venv"
conda create -y -n "${VENV_NAME}" python=3.7
# install conda-pack
conda install -y -n "${VENV_NAME}" -c conda-forge conda-pack
# install libraries
conda install -y -n "${VENV_NAME}" numpy pandas scipy tensorflow
# archive verv into a tarball file
conda pack -f -n "${VENV_NAME}" -o "${VENV_NAME}.tar.gz"
```

이렇게 만든 tarball file을 hdfs에 upload합니다.

이  path를 my_hdfs_path 라고 합시다.

cluster manager는 yarn을 사용한다고 가정합니다.

그러면 아래와 같이 설정하면 됩니다.

```shell
spark-submit \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./my-venv/bin/python
  --conf spark.yarn.dist.archives=my_hdfs_path \
  --master yarn \
  --deploy-mode cluster \
  test.py
```





# Reference

http://alkaline-ml.com/2018-07-02-conda-spark/

https://spark.apache.org/docs/latest/submitting-applications.html