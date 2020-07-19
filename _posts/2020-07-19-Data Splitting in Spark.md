---
layout: post
title: Data Splitting in Spark
---



Spark ML은 Big Data에서 Machine Learning을 할 때 꼭 필요한 선택이 될 수 있습니다.

Cluster에서 학습을 할 수 있다는 굉장히 큰 장점이 있기 때문입니다.

그러나 이러한 장점 하나만 보고 Spark ML을 하기에는 아직 기능이 많이 부족한 상태입니다.

이번에는 Spark에서 Data Splitting  기능이 어느 정도로 제공이 되며, 어떤 부분이 여타의 ML Library에 비해 뒤쳐지는지 알아보도록 하겠습니다.



[TOC]



# 1. Data Splitting이란?

Machine Learning을 하기로 결심을 했고, Data가 준비가 되었다면 이 Data를 용도에 따라 분리를 해야 합니다.

이러한 과정을 Data Splitting이라고 부릅니다.

Data Splitting에도 여러 전략이 있으며, 여러 전략 중 하나를 선택하는 것은 그 당시 직면한 문제에 따라 선택이 달라집니다.

일반적으로 가장 쉬우면서 흔히 쓰이는 방법은 Holdout Data Splitting입니다.

![_config.yml]({{ site.baseurl }}/images/data_splitting.png)

Holdout Data Splitting 전략에선 다음과 같이 4가지의 개념이 등잡합니다.

- test data: 마지막 최종적으로 ML model의 Performance를 측정할 때 쓰이는 data
- train data: ML Model을 훈련시킬 때 사용하는 data
  - dev data: ML model을 훈련시킬 때 직접 Loss function의 minimization에 사용되는 data
  - validation data: ML model을 훈련시킬 때 Evaluation metric을 매번 관찰하며 학습의 방향을 결정할 때 사용되는 data

여기서 헷갈리는 개념이 등장하는데,

1. validation data와 test data의 차이
2. dev data와 train data의 차이

에 대해서 헷갈릴 수가 있습니다. 명확한 차이는 다음과 같습니다.

1. validation data는 ML model의 hyperparameter를 정하거나 feature를 추가하거나 scaling 방법을 바꾸거나 model algorithm을 바꾸는 등의 일련의 experiment를 할 때마다 해당 데이터를 이용한 metric을 산출하게 되며, 이러한 지표를 토대로 ML model 개발의 방향을 정하게 됩니다. 즉, 직접적으로 model fitting에 사용되지는 않지만 model learning에는 사용하게 되는 것입니다. 따라서 train data에 속하게 됩니다.
   반면에 test data는 이러한 의사결정에는 참여하지 않습니다. 대신 마지막에 ML model이 결정되었을 때 단 한 번 Performance가 잘 나오는지를 측정하고, 이런 측정 결과를 ML model의 Performance라고 믿게 됩니다. 즉 test data에서 측정된 accuracy를 ML model의 accuracy라고 믿게 되는 것이죠. validation data의 metric은 믿을 것이 못 되는 것이, ML model을 학습 방향이 validation metric을 높이는 방향으로 매번 정해지기 때문에 over-estimated가 되어 믿을 수 없는 숫자가 됩니다.
2. train data는 ML model을 훈련시킬 때 사용되는 data 전체의 통칭입니다. 이 때 사용된다는 것은 ML model을 fitting 시키기 위해 사용되는 데이터 뿐만이 아니라 hyperparameter 등을 결정할 때 참고하는 것도 포함입니다. 참고하는 것도 사용되는 것입니다.
   반면, dev data는 ML model을 훈련시킬 때 직접적으로 들어가는 data입니다. 즉, model fitting을 시킬 때 사용되는 data이며, SGB 등의 learning algorithm에서는 직접적으로 gradient를 만들어내는 data라고 볼 수 있습니다. Validation data와는 다르게 직접 model을 만들 때 참여하게 됩니다. dev data는 train data의 부분 집합입니다.

대표적으로 validation data가 직접적으로 model의 의사결정에 참여하는 예는 early stopping입니다.

![_config.yml]({{ site.baseurl }}/images/early_stopping.png)



이렇게 test data와 train data의 구분, 그리고 dev data와 validation data의 구분을 잘 하는 것은 매우 중요한 일입니다. 구분을 문제에 맞게 잘 하는 방법에 대해선 이후에 항목에서 다시 논의하겠습니다. 지금은 일반적인 방법인 Cross Validation에 대해서 살펴보겠습니다.

Cross validation은 train data를 모두 dev data로 사용하고 동시에 validation data로도 사용하고 싶은 욕망으로 탄생하게 되었습니다.

![_config.yml]({{ site.baseurl }}/images/cross_validation.png)

위 그림과 같이 train data를 K개의 fold로 나누어, 개별 fold를 K-1번 dev data로서 사용하고 1번 validation data로서 사용하게 함으로써 이러한 욕망을 이루게 됩니다.



또한 중요한 점은, train data와 test data, 그리고 dev data와 validation data의 구분은 초기에 전략을 정했다면 반드시 ML model을 개발하는 내내 동일하게 유지를 해야한다는 점입니다. K-fold CV를 사용했다면, 해당 Fold를 모두 동일한 데이터로 유지해야 합니다. 그래야 Metric끼리 비교가 가능하고 의사결정이 가능하게 됩니다.

일반적은 work flow는 아래 그림과 같습니다. (Parameter 만이 아니라 여러가지를 의사결정하는 부분이 다릅니다.)

![_config.yml]({{ site.baseurl }}/images/search_workflow.png)





# 2. Spark에서의 Train test split

그렇다면 spark에서 data splitting을 어떻게 할까요?

먼저, train test split을 보겠습니다.

이 때는 Spark dataframe의 randomSplit 메서드를 사용하면 됩니다.

```python
import requests
# data is a spark dataframe
test_size = 0.2
train_size = 1 - test_size
seed = 24
(train, test) = data.randomSplit([train_size, test_size], seed)
```

여기서 의문이 생길 수 있습니다.

Spark는 여러 Partition으로 구성되었는데 randomSplit 메서드를 사용할 때 seed만 동일한 숫자로 설정하면, 항상 같은 train data와 test data를 얻을 수 있을까?

즉, train test split을 ML model을 개발하는 기간 동안 동일하게 유지를 할 수 있을까?

답은 아닙니다.

![_config.yml]({{ site.baseurl }}/images/spark_randomSplit_inconsistency.png)

동일하게 유지가 안 될뿐더러, 기대하는 규칙( train data + test data = data)도 지켜지지 않는 모습을 볼 수 있습니다.

이는 왜 그럴까요?

##### train data + test data = data 의 공식이 성립하지 않는 결과가 나오는 이유

이는 spark dataframe의 lazy evaluation 때문입니다.

count()가 호출될 때마다 spark는 randomSplit()을 다시 적용하는 것입니다.

즉, 의도하지 않게 randomSplit()이 두 번 호출된 것입니다.

이를 해결하기 위해선 dataframe을 caching하면 됩니다.



caching을 통해 규칙이 성립되지 않는 이슈는 해결이 되었습니다.

그러면 원래 이슈인, train test split을 매번 동일하게 유지할 수 있는가에 대한 질문을 ㄹ봅시다.

만약 dataframe을 caching하면 항상 같은 결과가 나올까요?

그것은 아닙니다. 이는 왜 그럴가요?



##### 동일한 train data와 test data를 매번 유지할 수 없는 이유

왜냐하면 randomSplit 메서드를 호출할 dataframe이 매번 같은 data로 형성되기가 힘들기 때문입니다.

만약 해당 dataframe을 얻기 위해 Shuffle이 많이 발생하는 복잡한 Query가 필요하다면, 매번 생성되는 Data의 RDD는 동일한 Partition에 동일한 Order라고 기대하기 힘들 것입니다.



##### 동일한 train data와 test data를 매번 유지하려면 어떻게 해야하는가

두 가지 방법이 있습니다.

하나는 train test split하려하는 dataframe을 hdfs에 저장하고, 이후 매번 dataframe으로 불러온 뒤 caching을 한 다음에 randomSplit을 적용하면 됩니다.

두번째는 train test split하려하는 dataframe을 caching한 뒤 randomSplit을 통해 데이터를 분할하고, 분할된 데이터의 identifier만 hdfs에 저장하고, 이후 매번 실험할 때마다 식별자를 불러와서 inner join을 하면 됩니다.

저는 후자의 방법을 추천 드립니다.



# 3. Spark에서의 train data splitting

그렇다면 Spark에서 train data를 다시 dev data와 validation data로 나누거나 Cross Validation을 하려면 어떻게 하면 좋을까요?

##### Spark에서 dev data와 validation data로 나누기

Spark 공식 문서에서는 pyspark.ml.tuning 모듈의 TrainValidationSplit를 사용하는 것을 추천합니다.

```python
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

seed = 24
stages = ...
pipeline = Pipeline(stages=stages)
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5,
                          seed=seed)
# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(training)
```





##### Spark에서 Cross Validation하기

Spark 공식 문서에서는 pyspark.ml.tuning 모듈의 CrossValidator를 사용하는 것을 추천합니다.

```python
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

stages = ...
pipeline = Pipeline(stages=stages)
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
tvs = TrainValidationSplit(estimator=pipeline,
                           estimatorParamMaps=paramGrid,
                           evaluator=BinaryClassificationEvaluator(),
                           trainRatio=0.8,
                           seed=seed)
# Run TrainValidationSplit, and choose the best set of parameters.
model = tvs.fit(train)
```



그러나 다시 의문점이 생깁니다.

Seed만 동일하게 해준다면, TrainValidationSplit이나 CrossValidator를 사용하면 dev data / validation data 또는 Folds가 동일한 data로 매번 유지가 될까?

Parameter search 안에서는 동일한 data로 유지가 되는걸까?

우선 후자의 질문에 대해서는 Yes입니다.

그러나 전자의 질문에 대해서는 No입니다.

유지를 하려면 매번 동일한 데이터를 저장해서 Caching을 해줘야하는데, 굉장히 번거로울 뿐만 아니라 feature를 추가하면 데이터가 달라지기 때문에 의미가 없어집니다.

따라서 CV를 한다면 매번 Folds의 구성이 바뀌겠죠.

이는 원하는 방향이 아닙니다.

왜냐하면 CV metric을 통해 매번 의사결정을 할텐데, 의사결정을 하기 위한 metric의 오르내림이 model의 개선이 아니라 data의 구성으로 바뀌기 때문입니다.

그리고 Folds를 원하는 방향으로 컨트롤하는 옵션이 없습니다.

이는 다른 Package와 대비되는 부분입니다.



# 4. Scikit-learn에서의 Data Splitting

가장 성숙한 ML Framework이라 평가받는 scikit-learn에서는 data splitting의 기능을 어떻게 제공할까요?

기본적으로 scikit-learn은 in-memory data인 pandas dataframe을 기반이므로, data splitting은 seed만 잘 준다면 유지가 됩니다.

또한 여러가지 data splitting strategy에 대해서 기능을 제공합니다.



< 기본 train test split >

```python
from sklearn.model_selection import train_test_split

# data is a pandas dataframe
test_size = 0.2
train_size = 1 - test_size
seed = 24
train, test = train_test_split(data, test_size=test_size, random_state=seed)
```

< stratified train test split >

```python
from sklearn.model_selection import train_test_split

# data is a pandas dataframe
label = "label" # label is a categorical target
test_size = 0.2
train_size = 1 - test_size
seed = 24
y = data.pop(label)
X = data
X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=test_size, stratify=y, random_state=seed)
```

< K-fold >

```python
from sklearn.model_selection import KFold

k = 5
seed = 24

kf = KFold(n_splits=k, random_state=seed)
for train_index, test_index in kf.split(X):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]
```

< Stratified K-fold >

```python
from sklearn.model_selection import StratifiedKFold

k = 5
seed = 24

kf = StratifiedKFold(n_splits=k, random_state=seed)
for train_index, test_index in kf.split(X):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]
```

![_config.yml]({{ site.baseurl }}/images/stratifiedKfold.png)

< TimeSeriesSplit >

```python
from sklearn.model_selection import TimeSeriesSplit

k = 5

tscv = TimeSeriesSplit(n_splits=k)
for train_index, test_index in tscv.split(X):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]
```

![_config.yml]({{ site.baseurl }}/images/timeseriessplit.png)



위 기능들 중에서 특히 Stratified나 Time을 고려하는 기능은 아직 Spark에 없습니다.

그러나 필요하면 Spark에서도 반드시 사용을 해야하는 개념입니다.



# 5. Spark에서 Stratified Data Splitting 하기

위에서 배운 개념을 그대로 적용해봅니다.

미리 data를 caching하고, label의 class를 고려하여 data를 나누고, 이를 유지하기 위해 PK를 저장해봅시다.

```python
import pyspark
from pyspark.sql import functions as F

# df is a spark dataframe
label = "label" # label is a binanry class i.e. 0 or 1
pk = "pk"
seed = 24
test_size = 0.2
train_size = 1 - test_size

mosu = df.select(pk, label)
positive = mosu.filter(F.col(label) == 1).select(pk)
negative = mosu.filter(F.col(label) == 0).select(pk)
# for consistent data splitting, chche
positive.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
negative.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
train_positive, test_positive = positive.randomSplit([train_size, test_size], seed=seed)
train_negative, test_negative = negative.randomSplit([train_size, test_size], seed=seed)
train = train_positive.unionAll(train_negative)
test = test_positive.unionAll(test_negative)
# for consistent usage of train test split, write parquet
train.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable("train")
test.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable("test")
```

위와 같은 방식으로, Stratified K-Fold도 spark에서 사용할 수 있습니다.

개인적으로는 pyspark.ml.tuning 모듈의 CrossValidator를 사용하는 것을 비추천합니다.

위와 같이 직접 구현하는 것이 아직까지는 낫다고 봅니다.



# Reference

https://scikit-learn.org/stable/modules/cross_validation.html#cross-validation

https://medium.com/udemy-engineering/pyspark-under-the-hood-randomsplit-and-sample-inconsistencies-examined-7c6ec62644bc

https://towardsdatascience.com/train-test-split-and-cross-validation-in-python-80b61beca4b6

https://medium.com/@rahuljain13101999/why-early-stopping-works-as-regularization-b9f0a6c2772

https://mangastorytelling.tistory.com/entry/Owen-Zhang-Tips-for-data-science-competitions

https://spark.apache.org/docs/latest/ml-tuning.html#cross-validation