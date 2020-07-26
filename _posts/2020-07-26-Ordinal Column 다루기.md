---
layout: post
title: Ordinal Column 다루기
---



ML에서는 다양한 타입의 변수를 다룰 수 있어야 합니다.

대표적으로 수치형 타입과 범주형 타입이 있습니다.

위 두 타입을 다루는 방법은 굉장히 잘 알려져 있는데, 다른 타입은 어떻게 다루는지는 잘 알려져 있지 않습니다.

만약 범주형이지만 순서가 있으면 어떻게 될까요?

이러한 변수를 Ordinal 타입이라고 부릅니다.

본 Post에서는 Ordinal 타입의 변수를 다루는 방법에 대해서 알아보도록 하겠습니다.



[TOC]



# 1. Ordinal Column 생성하기

데이터를 csv 파일이나 parquet에서 불러왔습니다.

여기서는 pandas로 불러왔다고 가정하겠습니다.

처음 데이터를 불러오면, 범주형 데이터이니 string이나 object로 불러와지겠네요.

이를 ordinal column으로 인식시켜주려면 어떻게 해야할까요?

pandas의 categorical 모듈을 사용하면 됩니다.

```python
# df is a pandas dataframe
ord_col = "ordinal_categorical_column"
df[ord_col].astype('category', copy=False)
df[ord_col].cat.as_ordered(inplace=True)
```

그런데 위와 같이 사용하면 ordering이 제대로 되었는지 의심이 되죠.

따라서 ordering 을 명확하게 주려면 다음과 같이 하면 됩니다.

pandas의 categorical 모듈을 사용하면 됩니다.

(1) type 전환 시 명시적으로 설정하는 방법

```python
from pandas.api.types import CategoricalDtype

# df is a pandas dataframe
ord_col = "ordinal_categorical_column"
ord_type = CategoricalDtype(['a', 'b', 'c'], ordered=True)
df[ord_col].astype(ord_type, copy=False)
```

(2) categorical column이 ordered임을 설정할 때 명시적으로 설정하는 방법

```python
# df is a pandas dataframe
ord_col = "ordinal_categorical_column"
df[ord_col].astype('category', copy=False)
df[ord_col].cat.set_categories(['a', 'b', 'c'], ordered=True)
```

개인적으로는 (2) option을 추천 드립니다.



# 2. Ordinal Column 전처리하기

Ordinal Column을 ML Modeling의 feature로서 사용하려 합니다.

이 때 가장 먼저 생각나는 방법은 One-hot encoding이겠죠.

하지만 One-hot encoding을 사용하면, 해당 변수의 ordering 정보를 잃게 됩니다.

따라서 변수의 모든 정보를 활용하는 방법은 아닙니다.

변수의 Ordering 정보까지 모두 보존하면서 feature로 활용하기 위한 전처리 방법에는 어떤 것들이 있을까요?



## (1) OrdinalEncoder

Scikit-learn에서 제공하는 일반적인 방법은 OrdinalEncoder를 사용하는 것입니다.

아래와 같이 사용할 수 있습니다.

```python
from sklearn.preprocessing import OrdinalEncoder

ordinal_cols = [ord_col1, ord_col2]
enc = OrdinalEncoder(categories=ordinal_cols, dtype=np.int64)
X_encoded = enc.fit_transform(X)
```

위 방법은 Ordinal 변수의 범주값을 Rank로 Mapping합니다.

예를 들어, 범주값의 Ordering이 A > B > C 이면 A를 0,  B를 1, C를 2로 Mapping합니다.

범주가 단순히 등차수열로 바뀌는 것이라 정보를 전부는 반영하지 못한다고 생각합니다만, 그래도 유용할 것 같습니다.



## (2) BinaryEncoder

위와 같이 범주가 integer로 Mapping이 되고 그것이 0부터 오름차순으로 1씩 증가한다면, 그리고 동시에 범주에 따른 극적인 효과를 One-hot Encoding처럼 되는 효과를 원한다면, 다른 방법이 있습니다.

Scikit-learn의 contrib 중 category encoders 라이브러리의 BinaryEncoder를 사용하면 됩니다.

BinaryEnoding은 아래의 Step으로 구성됩니다.

1. Categorical Column의 Order를 그대로 반영하여 Integer로 Mapping합니다
2. Integer를 다시 2진수로 Mapping합니다. 예를 들어 3은 11이 됩니다.
3. 2진수의 최대 자리수 만큼 변수를 생성합니다. 예를 들어 최대정수가 7이라면, 3은 011이 되고 변수는 3개가 생성됩니다. 3의 첫번째 변수의 값은 0, 두번째 변수의 값은 1, 세번째 변수의 값은 1이 됩니다.

아래와 같이 시각화를 할 수 있습니다.

![_config.yml]({{ site.baseurl }}/images/binary_encoding.png)

아래와 같이 사용할 수 있습니다.

```python
import category_encoders as ce

ordinal_cols = [ord_col1, ord_col2]
enc = ce.BinaryEncoder(cols=ordinal_cols)
X_encoded = enc.fit_transform(X)
```





## (3) OrdinalEncoder, BinaryEncoder Revisited

그러나 위와 같이 사용하는 경우, Ordinal Column의 order를 preserve를 하는 것인지 의심이 듭니다.

즉, ordering을 다시 자체적으로 하여 Encoding을 하는 의심이 듭니다.

이 의심이 헛된 의심이라는 증거를 현재까지 발견하지 못 했습니다.

따라서 Manual하게 하는 것이 좋아보입니다.

아래는 OrdinalEncoding을 Manual하게 수행하는 부분입니다.

```python
ordinal_cols = [ord_col1, ord_col2]

for col in ordinal_cols:
    mapping = {cat: i for i, cat in enumerate(X[col].cat.categories)}
    X[col] = X[col].map(lambda x: mapping[x])
```

이와 같이 BinaryEncoding도 Manual하게 수행해서 사용할 수 있습니다.



## (4) RIDIT scoring

RIDIT scoring은 개별 범주형 변수의 rank의 개념을 받아들이고 비율정보를 더함으로써 가능한 order의 정확한 정보를 보존하고자 하는 방법론입니다.

RIDIT는 아래의 Step으로 구성됩니다.

1. RIDIT Scoring을 적합시킬 때 사용할 Data를 준비합니다.
2. 개별 범주값마다의 전체에서 차지하는 비율을 구합니다.
3. 해당 범주값보다 Order가 낮은 범주값의 비율을 모두 더하고, 해당 범주값의 비율을 반을 더한 값이 RIDIT Score가 됩니다.

공식은 다음과 같습니다.
$$
p_{j} = Prob(x_{j}) \\
ridit_{j} = 0.5p_{j} + \sum_{k<j}{p_{k}}
$$
위에서 RIDIT score는 항상 0에서 1 사이의 값으로 형성이 됩니다.

RIDIT는 범주값의 Ordering 정보를 단순한 등차수열로서 사용하는 것이 아닌, 실제 범주값의 순위가 전체에서 몇 등인지를 더 정확하게 반영한다고 보면 좋을 것 같습니다.

아래와 같이 공식을 변형해서 -1과 1 사이의 값으로 사용하기도 합니다.
$$
p_{j} = Prob(x_{j}) \\
ridit_{j} = \sum_{k<j}{p_{k}} - \sum_{k>j}{p_{k}}
$$



# Reference

https://en.wikipedia.org/wiki/Ridit_scoring#cite_note-2

https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html 

https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OrdinalEncoder.html 

 http://contrib.scikit-learn.org/category_encoders/binary.html 

https://towardsdatascience.com/all-about-categorical-variable-encoding-305f3361fd02 