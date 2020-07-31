---
layout: post
title: Spark ML Custom Transformer(Estimator)
---



ML에서는 다양한 타입의 변수를 다룰 수 있어야 합니다.



Spark ML에는 scikit-learn과 유사한 Pipeline API가 있습니다.

Spark ML Pipeline의 구성요소로는 대표적으로 Transformer와 Esimator가 있습니다.

이러한 Transformer와 Estimator로 DAG를 구성해서 Pipeline을 완성시키고, 이를 fitting 후 pipeline transformer로서 사용할 수 있습니다.

매우 강력한 무기인데, 이를 만약에 Custom Transformer나 Custom Estimator로 Pipeline을 구성할 수 있다면 굉장히 좋겠죠.

본 Post에서는 Spark ML에서 Custom Transformer / Estimator를 만드는 방법을 알아보도록 하겠습니다.



[TOC]



# 1. Custom Transformer 만들기

Custom Transformer를 만드는 방법은 현재 어떤 책이나 docs를 봐도 나와있지 않습니다.

제가 조사한 내용의 출처는 stack overflow이고, 완성도 있는 내용은 아닙니다.

그리고 stack overflow에서 설명한 내용도 사실 다 알아듣지는 못했습니다.

그러나 활용도가 많을 것으로 예상되어 관련 내용을 정리해서 적겠습니다.



우선 Custom Transformer를 만들기 위해선 pyspark.ml의 Transformer를 상속해야 합니다.

Custom Transformer를 pipeline으로 구성했을 때 read / write 메서드를 자동으로 사용할 수 있게 하기 위해서, pyspark.ml.util의 DefaultParamsReadable과 DefaultParamsWritable을 상속하면 좋습니다.

만약 Custom Transformer가 Input Column이 존재하면 pyspark.ml.param.shared의 HasInputCol을 상속받아야 합니다.

또한 Custom Transformer가 output Column이 존재하면 pyspark.ml.param.shared의 HasOutputCol을 상속받아야 합니다.

이러한 내용을 종합하여 nltk의 기능을 사용한 Custom Transformer - NLTKWordPunctTokenizer를 만들어보도록 하겠습니다.

```python
import nltk
from pyspark import keyword_only  ## < 2.0 -> pyspark.ml.util.keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
# Available in PySpark >= 2.3.0 
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable  
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

class NLTKWordPunctTokenizer(
        Transformer, HasInputCol, HasOutputCol,
        # Credits https://stackoverflow.com/a/52467470
        # by https://stackoverflow.com/users/234944/benjamin-manns
        DefaultParamsReadable, DefaultParamsWritable):

    stopwords = Param(Params._dummy(), "stopwords", "stopwords",
                      typeConverter=TypeConverters.toListString)


    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, stopwords=None):
        super(NLTKWordPunctTokenizer, self).__init__()
        self.stopwords = Param(self, "stopwords", "")
        self._setDefault(stopwords=[])
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setStopwords(self, value):
        return self._set(stopwords=list(value))

    def getStopwords(self):
        return self.getOrDefault(self.stopwords)

    # Required in Spark >= 3.0
    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    # Required in Spark >= 3.0
    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _transform(self, dataset):
        stopwords = set(self.getStopwords())

        def f(s):
            tokens = nltk.tokenize.wordpunct_tokenize(s)
            return [t for t in tokens if t.lower() not in stopwords]

        t = ArrayType(StringType())
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        return dataset.withColumn(out_col, udf(f, t)(in_col))
```

핵심 로직은 _transform 메서드에 구성합니다.



이렇게 만들고 나면 아래와 같이 활용할 수 있습니다.

```python
sentenceDataFrame = spark.createDataFrame([
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
], ["label", "sentence"])

tokenizer = NLTKWordPunctTokenizer(
    inputCol="sentence", outputCol="words",  
    stopwords=nltk.corpus.stopwords.words('english'))

tokenizer.transform(sentenceDataFrame).show()
```





# 2. Custom Estimator 만들기

Custom Estimator를 만드는 방법 또한 현재 어떤 책이나 docs를 봐도 나와있지 않습니다.

마찬가지로 조사한 내용의 출처는 stack overflow이고, 완성도 있는 내용은 아닙니다.

그래도 활용도가 많을 것으로 예상되어 관련 내용을 정리해서 적겠습니다.



우선 Estimator는 fit 메서드를 통해 내부적인 Parameter를 학습하여 Model(Transformer) 객체를 만들어야 합니다.

Stack overflow의 답변을 추측컨데, 내부적인 Parameter를 설정하고 명시하기 위해서 pyspark.ml.param.Params 를 상속한 Has{$MyParameter} Class를 만들어야 할 것 같습니다.

학습을 하기 위해 설정해야하는 Parameter는 Custom Estimator Class를 만들 때 상속해야 합니다.

학습 후 얻어지는 Parameter는 학습의 결과로 나오는 Custom Model Class를 만들 때 상속해야 합니다.

만약, 학습을 하기 위해 설정해야하는 Parameter로 Custom Model의 Logic에서 사용해야 한다면, 마찬가지로 상속해야 합니다.

Custom Transformer와 비슷하게, Custom Estimator를 만들기 위해선 pyspark.ml의 Estimator를 상속해야 합니다.

또한 마찬가지로, HasInputCol, HasPredictionCol, DefaultParamsReadable, DefaultParamsWritable 등을 상황에 맞게 상속하면 좋습니다.

이러한 내용을 종합하여 수치형 컬럼이 평균과 표준편차를 고려했을 때, threshold * 표준편차, 를 벗어나는지 판정하는 Estimator를 만들어보도록 하겠습니다.

```python
from pyspark.ml.pipeline import Estimator, Model, Pipeline
from pyspark.ml.param.shared import *
from pyspark.sql.functions import avg, stddev_samp
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable 
from pyspark import keyword_only 


class HasMean(Params):

    mean = Param(Params._dummy(), "mean", "mean", 
        typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasMean, self).__init__()

    def setMean(self, value):
        return self._set(mean=value)

    def getMean(self):
        return self.getOrDefault(self.mean)
    
    
class HasStandardDeviation(Params):

    standardDeviation = Param(Params._dummy(),
        "standardDeviation", "standardDeviation", 
        typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasStandardDeviation, self).__init__()

    def setStddev(self, value):
        return self._set(standardDeviation=value)

    def getStddev(self):
        return self.getOrDefault(self.standardDeviation)


class HasCenteredThreshold(Params):

    centeredThreshold = Param(Params._dummy(),
            "centeredThreshold", "centeredThreshold",
            typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasCenteredThreshold, self).__init__()

    def setCenteredThreshold(self, value):
        return self._set(centeredThreshold=value)

    def getCenteredThreshold(self):
        return self.getOrDefault(self.centeredThreshold)
    
    
class NormalDeviation(Estimator, HasInputCol, 
        HasPredictionCol, HasCenteredThreshold,
        # Available in PySpark >= 2.3.0 
        # Credits https://stackoverflow.com/a/52467470
        # by https://stackoverflow.com/users/234944/benjamin-manns
        DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self, inputCol=None, predictionCol=None, centeredThreshold=1.0):
        super(NormalDeviation, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # Required in Spark >= 3.0
    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    # Required in Spark >= 3.0
    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @keyword_only
    def setParams(self, inputCol=None, predictionCol=None, centeredThreshold=1.0):
        kwargs = self._input_kwargs
        return self._set(**kwargs)        

    def _fit(self, dataset):
        c = self.getInputCol()
        mu, sigma = dataset.agg(avg(c), stddev_samp(c)).first()
        return NormalDeviationModel(
            inputCol=c, mean=mu, standardDeviation=sigma, 
            centeredThreshold=self.getCenteredThreshold(),
            predictionCol=self.getPredictionCol())


class NormalDeviationModel(Model, HasInputCol, HasPredictionCol,
        HasMean, HasStandardDeviation, HasCenteredThreshold,
        DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self, inputCol=None, predictionCol=None,
                mean=None, standardDeviation=None,
                centeredThreshold=None):
        super(NormalDeviationModel, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)  

    @keyword_only
    def setParams(self, inputCol=None, predictionCol=None,
                mean=None, standardDeviation=None,
                centeredThreshold=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)           

    def _transform(self, dataset):
        x = self.getInputCol()
        y = self.getPredictionCol()
        threshold = self.getCenteredThreshold()
        mu = self.getMean()
        sigma = self.getStddev()

        return dataset.withColumn(y, (dataset[x] - mu) > threshold * sigma) 
```

핵심 로직은 Estimator의 경우 _fit 메서드에, Model(Transformer)의 경우 _transform 메서드에 구성합니다.

이렇게 만들고 나면 아래와 같이 활용할 수 있습니다.

```python
df = sc.parallelize([(1, 2.0), (2, 3.0), (3, 0.0), (4, 99.0)]).toDF(["id", "x"])

normal_deviation = NormalDeviation().setInputCol("x").setCenteredThreshold(1.0)
model  = Pipeline(stages=[normal_deviation]).fit(df)

model.transform(df).show()
## +---+----+----------+
## | id|   x|prediction|
## +---+----+----------+
## |  1| 2.0|     false|
## |  2| 3.0|     false|
## |  3| 0.0|     false|
## |  4|99.0|      true|
## +---+----+----------+
```





# 3. Next Step

솔직히 어렵고 잘 이해가 가지 않는 부분이 많습니다.

왜 set, get 등을 일일이 모두 해줘야하는가.

@keyword_only 데코레이션이 왜 붙는 것인가.

등 의문점이 남아 있습니다.

다음 Post에서는 좀 더 정확하게 조사해서 보충 내용을 올리도록 하겠습니다.



# Reference

https://spark.apache.org/docs/latest/ml-pipeline.html#pipeline-components

https://stackoverflow.com/questions/32331848/create-a-custom-transformer-in-pyspark-ml

https://stackoverflow.com/questions/37270446/how-to-create-a-custom-estimator-in-pyspark

https://stackoverflow.com/questions/41399399/serialize-a-custom-transformer-using-python-to-be-used-within-a-pyspark-ml-pipel/44377489#44377489

https://spark.apache.org/docs/latest/api/python/pyspark.ml.html

https://docs.databricks.com/applications/machine-learning/mllib/advanced-mllib.html