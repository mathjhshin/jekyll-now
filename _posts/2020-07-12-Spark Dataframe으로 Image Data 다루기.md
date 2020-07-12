---
layout: post
title: Spark Dataframe으로 Image Data 다루기
---



Big Data를 다룬다면 가장 흔하게 쓰이는 Framework가 Spark입니다.

Machine Learning에서는 아주 흔하게 쓰이지는 않습니다. 그러나 데이터의 크기가 아주 크다면 Spark가 굉장히 좋은 선택이고, 저 또한 애용하고 있습니다.

이번에는 Image Data를 Spark Dataframe으로 다루는 방법을 알아 보도록 하겠습니다.



[TOC]



# 1. Spark 공식 문서에서 권장하는 방법

spark 공식 문서에서 권장하는 Image data를 읽는 방법을 먼저 보도록 하겠습니다.

공식 문서에서는 pyspark.ml.image module을 사용하라고 가이드가 나와있습니다.

```python
df = spark.read.format("image").option("dropInvalid", True).load("data/mllib/images/origin/kittens")
```

위와 같은 code로 Image data를 불러오면 pyspark.ml.image.ImageSchema로 데이터가 정의되는데, 이 때 image column의 schema는 다음과 같습니다.

- origin: `StringType` (represents the file path of the image)
- height: `IntegerType` (height of the image)
- width: `IntegerType` (width of the image)
- nChannels: `IntegerType` (number of image channels)
- mode: `IntegerType` (OpenCV-compatible type)
- data: `BinaryType` (Image bytes in OpenCV-compatible order: row-wise BGR in most cases)

실제로 데이터를 보면 다음과 같이 나옵니다.

```python
df.select("image.origin", "image.width", image.height").show(truncate=False)
+-----------------------------------------------------------------------+-----+------+
|origin                                                                 |width|height|
+-----------------------------------------------------------------------+-----+------+
|file:///spark/data/mllib/images/origin/kittens/54893.jpg               |300  |311   |
|file:///spark/data/mllib/images/origin/kittens/DP802813.jpg            |199  |313   |
|file:///spark/data/mllib/images/origin/kittens/29.5.a_b_EGDP022204.jpg |300  |200   |
|file:///spark/data/mllib/images/origin/kittens/DP153539.jpg            |300  |296   |
+-----------------------------------------------------------------------+-----+------+
```

그러나 이 방법의 문제점은, image file만이 들어있는 hdfs path에서 image data를 가져온다는 점입니다.

만약 제가 Big Data를 가지고 있고, 그 안에는 Text도 정형데이터도 있으며 image data는 단순히 여러 컬럼 중 하나인 경우에는 사용할 수가 없습니다.

저의 경우는 1000개 이상의 컬럼이 있었고, 그 중 1개의 컬럼이 image url이었습니다.

이런 경우는 어떻게 해야 할까요?



# 2. Spark Dataframe의 여러 컬럼 중 하나가 image url인 경우

저의 경우에는 위에서 말씀드렸다시피 여러 컬럼 중 하나가 image url인 상황이었습니다.

그러한 상황에서는 pyspark.ml.image module을 사용할 수가 없었습니다.

Google에 검색해도 마땅한 해법은 나오지 않습니다.

여기서부터는 그냥 제가 쓰는 방법인데 아주 좋은 해답은 아닙니다.

아직도 더 좋은 해답을 찾고 있습니다. 혹시 아는 분은 저에게 알려주시면 감사하겠습니다.

제가 사용하는 방법은 image url로부터 image data를 가져오는 python code를 udf로 만드는 방법입니다.

여기서 udf는 spark UDF일 수도 있고 pandas_udf일 수도 있습니다.

저는 바로 Vector 형태로 데이터를 parsing하기 위해 spark UDF와 VectorUDT를 함께 사용했습니다.

```python
import requests
import numpy as np
from PIL import Image
from io import BytesIO
from pyspark.ml.linalg import Vectors, VectorUDT

def get_img_url(url):
    response = request.get(url)
    img = Image.open(BytesIO(response.content))
    img = img.resize((32, 32))
    img_array = np.array(img)[:, :, :3].flatten() / 255
    return Vectors.dense(img_array)

get_img_url_udf = F.udf(get_img_url, VectorUDT())
df = df.withColumn("image_vector", get_img_url_udf("image_url"))
```

위와 같은 방식으로 하면, spark Dataframe의 여러 컬럼 중 하나로 image url이 있어도 image data를 불러올 수 있습니다.



# 3. Next Step

위의 방법에는 한계점이 있습니다.

가장 큰 한계점은 spark dataframe의 data type에 Tensor가 없어서, 32 x 32 x 3의 Tensor data로 읽어올 수가 없었습니다. 어쩔 수 없이 flatten을 한 뒤에 vector나 ArrayType(DoubleType())으로 불러들이는 방법을 택했습니다. 따라서 32 x 32 x 3이 아닌 3072 dim의 vector가 되었습니다.

위의 한계는 꾀나 크리티컬한데, 그 이유는 일반적으로 Deep Learning on Image를 할 때에는 32 x 32 x 3의 Tensor로 불러들여 Convolution 등의 Architecture를 사용하기 때문입니다. 제가 가져온 것으로는 pca나 FNN 정도 밖에 할 수가 없었습니다.

물론 spark에서 Deep Learning을 하는 것도 Research를 많이 해야하는 영역이므로, Tensor column을 만들 수 있어도 Deep Learning을 하는 것이 쉬운 일은 아닙니다.

다음에는 Spark Cluster 위에서 Deep Learning을 한 것으로 Posting을 할 수 있으면 좋겠네요.



# Reference

https://spark.apache.org/docs/latest/ml-datasource.html

https://spark.apache.org/docs/latest/api/python/pyspark.ml.html