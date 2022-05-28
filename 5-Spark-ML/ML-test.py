# coding=utf-8

# 1. 导入需要的包
from pyspark import SparkContext
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vector,Vectors
from pyspark.sql import Row
from pyspark.ml.feature import IndexToString,StringIndexer,VectorIndexer


# 2.第2步：读取文本文件，第一个map把每行的数据用“,”隔开，
# 比如在的数据集中，每行被分成了5部分，前4部分是鸢尾花的4个特征，最后一部分是鸢尾花的分类；
# 这里把特征存储在Vector中，创建一个Iris模式的RDD，然后转化成dataframe。
def f(x):
    rel = {}
    rel['features']=Vectors. \
    dense(float(x[0]),float(x[1]),float(x[2]),float(x[3]))
    rel['label'] = str(x[4])
    return rel
data = SparkContext.sparkContext. \
textFile("file:///usr/local/spark/iris.txt"). \
map(lambda line: line.split(',')). \
map(lambda p: Row(**f(p))). \
toDF()


# 第3步：进一步处理特征和标签，把数据集随机分成训练集和测试集，其中训练集占70%。
labelIndexer = StringIndexer(). \
setInputCol("label"). \
setOutputCol("indexedLabel"). \
fit(data)
featureIndexer = VectorIndexer(). \
setInputCol("features"). \
setOutputCol("indexedFeatures"). \
setMaxCategories(4). \
fit(data)
labelConverter = IndexToString(). \
setInputCol("prediction"). \
setOutputCol("predictedLabel"). \
setLabels(labelIndexer.labels)
trainingData, testData = data.randomSplit([0.7, 0.3])


# 第4步：创建决策树模型DecisionTreeClassifier，通过setter的方法来设置决策树的参数，
# 也可以用ParamMap来设置。这里仅需要设置特征列（FeaturesCol）和待预测列（LabelCol）。
# 具体可以设置的参数可以通过explainParams()来获取。
dtClassifier = DecisionTreeClassifier(). \
setLabelCol("indexedLabel"). \
setFeaturesCol("indexedFeature")


# 第5步：构建机器学习流水线（Pipeline），在训练数据集上调用fit()进行模型训练，
# 并在测试数据集上调用transform()方法进行预测。
dtPipeline = Pipeline(). \
setStages([labelIndexer, featureIndexer, dtClassifier, labelConverter])
dtPipelineModel = dtPipeline.fit(trainingData)
dtPredictions = dtPipelineModel.transform(testData)
dtPredictions.select("predictedLabel", "label", "features").show(20)

evaluator = MulticlassClassificationEvaluator(). \
setLabelCol("indexedLabel"). \
setPredictionCol("prediction")
dtAccuracy = evaluator.evaluate(dtPredictions)
print(dtAccuracy)  # 0.9726976552103888  #模型的预测准确率


# 第6步：可以通过调用DecisionTreeClassificationModel的toDebugString方法，查看训练的决策树模型结构。
treeModelClassifier = dtPipelineModel.stages[2]
print("Learned classification tree model:\n" + \
str(treeModelClassifier.toDebugString))

