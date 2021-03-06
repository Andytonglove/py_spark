from itertools import islice
import findspark
findspark.init()

'''
数据集: 下载Adult数据集(http://archive.ics.uci.edu/ml/datasets/Adult)。
数据从美国1994年人口普查数据库抽取而来，可用来预测居民收入是否超过50K$/year。
该数据集类变量为年收入是否超过50k$，属性变量包含年龄、工种、学历、职业、人种等重要信息。

1. 从文件中导入数据,并转化为DataFrame。
2. 训练决策树模型,用于预测居民收入是否超过50K。
3. 对Test数据集进行验证,输出模型的准确率。
'''

# pyspark存在部分问题；因此将data的后部分数据作为训练集，test做验证集，运行
# 这里由于编码的部分原因，此决策树训练环境改用为py3.6

from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row, SQLContext
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# 字典类型
work_type = {'Private': 1,
             'Self-emp-not-inc': 2,
             'Self-emp-inc': 3,
             'Federal-gov': 4,
             'Local-gov': 5,
             'State-gov': 6,
             'Without-pay': 7,
             'Never-worked': 8,
             '?': -1}
education = {'Bachelors': 1,
             'Some-college': 2,
             '11th': 3,
             'HS-grad': 4,
             'Prof-school': 5,
             'Assoc-acdm': 6,
             'Assoc-voc': 7,
             '9th': 8,
             '7th-8th': 9,
             '12th': 10,
             'Masters': 11,
             '1st-4th': 12,
             '10th': 13,
             'Doctorate': 14,
             '5th-6th': 15,
             'Preschool': 16,
             '?': -1}
marital_status = {'Married-civ-spouse': 1,
                  'Divorced': 2,
                  'Never-married': 3,
                  'Separated': 4,
                  'Widowed': 5,
                  'Married-spouse-absent': 6,
                  'Married-AF-spouse': 7,
                  '?': -1}
occupation = {'Tech-support': 1,
              'Craft-repair': 2,
              'Other-service': 3,
              'Sales': 4,
              'Exec-managerial': 5,
              'Prof-specialty': 6,
              'Handlers-cleaners': 7,
              'Machine-op-inspct': 8,
              'Adm-clerical': 9,
              'Farming-fishing': 10,
              'Transport-moving': 11,
              'Priv-house-serv': 12,
              'Protective-serv': 13,
              'Armed-Forces': 14,
              '?': -1}
relationship = {'Wife': 1,
                'Own-child': 2,
                'Husband': 3,
                'Not-in-family': 4,
                'Other-relative': 5,
                'Unmarried': 6,
                '?': -1}
race = {'White': 1,
        'Asian-Pac-Islander': 2,
        'Amer-Indian-Eskimo': 3,
        'Other': 4,
        'Black': 5,
        '?': -1}
sex = {'Female': 1,
       'Male': 2,
       '?': -1}
native_country = {'United-States': 1,
                  'Cambodia': 2,
                  'England': 3,
                  'Puerto-Rico': 4,
                  'Canada': 5,
                  'Germany': 6,
                  'Outlying-US(Guam-USVI-etc)': 7,
                  'India': 8,
                  'Japan': 9,
                  'Greece': 10,
                  'South': 11,
                  'China': 12,
                  'Cuba': 13,
                  'Iran': 14,
                  'Honduras': 15,
                  'Philippines': 16,
                  'Italy': 17,
                  'Poland': 18,
                  'Jamaica': 19,
                  'Vietnam': 20,
                  'Mexico': 21,
                  'Portugal': 22,
                  'Ireland': 23,
                  'France': 24,
                  'Dominican-Republic': 25,
                  'Laos': 26,
                  'Ecuador': 27,
                  'Taiwan': 28,
                  'Haiti': 29,
                  'Columbia': 30,
                  'Hungary': 31,
                  'Guatemala': 32,
                  'Nicaragua': 33,
                  'Scotland': 34,
                  'Thailand': 35,
                  'Yugoslavia': 36,
                  'El-Salvador': 37,
                  'Trinadad&Tobago': 38,
                  'Peru': 39,
                  'Hong': 40,
                  'Holand-Netherlands': 41,
                  '?': -1}

# 对应处理函数
def f(x):
    rel = {
        'features': Vectors.dense(float(x[0]),
                                  float(work_type[x[1]]),
                                  float(x[2]),
                                  float(education[x[3]]),
                                  float(x[4]),
                                  float(marital_status[x[5]]),
                                  float(occupation[x[6]]),
                                  float(relationship[x[7]]),
                                  float(race[x[8]]),
                                  float(sex[x[9]]),
                                  float(x[10]),
                                  float(x[11]),
                                  float(x[12]),
                                  float(native_country[x[13]])
                                  ),
        'label': str(x[14])}
    return rel


# spark 初始化
conf = SparkConf().setMaster("local").setAppName("ml")
sc = SparkContext(conf=conf)  # 创建spark对象
# solve the question:AttributeError: 'PipelinedRDD' object has no attribute 'toDF'
sqlContext = SQLContext(sc)
# 这里data数据用于训练，由于数据量太大会crash，所以需要跳过一些行数，多次读取再综合
data = sc.textFile("adult/adult.data").mapPartitionsWithIndex(
    lambda i, test_iter: islice(test_iter, 1008, None) if i == 0 else test_iter).map(
        lambda line: line.split(', ')).map(
            lambda p: Row(**f(p))).toDF()
# 再读取开头被跳过的1008行，这个数字是反复测试的极限值
data_front = sc.textFile("adult/adult.data").map(
        lambda line: line.split(', ')).map(
            lambda p: Row(**f(p))).toDF().limit(1008)
data = data.union(data_front)  # 将数据合并，使用data的全部数据

labelIndexer = StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
featureIndexer = VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
labelConverter = IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
trainingData = data

# 这里adult.test里存在结尾50K.的错误数据，通过vscode去掉.全部替换为50K的正常数据
# test一共16000行，跳过无关内容的第一行，用于验证测试
testData = sc.textFile("adult/adult-new.test").mapPartitionsWithIndex(
    lambda i, test_iter: islice(test_iter, 1, None) if i == 0 else test_iter).map(
        lambda line: line.split(', ')).map(
            lambda p: Row(**f(p))).toDF()
dtClassifier = DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

# 流水线
dtPipeline = Pipeline().setStages([labelIndexer, featureIndexer, dtClassifier, labelConverter])
dtPipelineModel = dtPipeline.fit(trainingData)
dtPredictions = dtPipelineModel.transform(testData)
dtPredictions.select("predictedLabel", "label", "features").show(30)
evaluator = MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
# 输出精度
accuracy = evaluator.evaluate(dtPredictions)
print(accuracy)