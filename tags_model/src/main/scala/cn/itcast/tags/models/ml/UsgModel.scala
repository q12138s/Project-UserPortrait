package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelConfig, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 开发挖掘类型标签：用户购物性别USG模型
 */
class UsgModel extends AbstractModel("用户购物性别USG模型", ModelType.ML) {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = businessDF.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    import cn.itcast.tags.spark.hbase._

    //加载订单表数据，获取订单编号和会员ID
    val ordersDF: DataFrame = spark.read
      .option("zkHosts", ModelConfig.PROFILE_TABLE_ZK_HOSTS)
      .option("zkPort", ModelConfig.PROFILE_TABLE_ZK_PORT)
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "ordersn,memberid")
      .hbase
    //    ordersDF.printSchema()
    //    ordersDF.show(20,false)
    /*
    +----------------------+--------+
    |ordersn               |memberid|
    +----------------------+--------+
    |gome_792756751164275  |725     |
    |jd_14090106121770839  |301     |
     */

    // 2. 加载维度表的数据：颜色维度表和商品维度表
    val colorsDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
      .option("url", ModelConfig.MYSQL_JDBC_URL)
      .option("dbtable", "profile_tags.tbl_dim_colors")
      .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
      .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
      .load()
    //    colorsDF.printSchema()
    //    colorsDF.show(20,false)
    /*
    +---+----------+
    |id |color_name|
    +---+----------+
    |1  |香槟金色   |
    |2  |黑色       |
     */
    //构建颜色语句
    val colorWhen: Column = {
      var colWhen: Column = null
      colorsDF
        .as[(Int, String)] //将DataFrame转换为DataSet
        .rdd //转换为RDD
        .collectAsMap() //转换为Map
        .foreach {
          case (colorId, colorName) =>
            if (null == colWhen) {
              colWhen = when($"ogcolor".equalTo(colorName), colorId)
            } else {
              colWhen = colWhen.when($"ogcolor".equalTo(colorName), colorId)
            }
        }
      colWhen.otherwise(0).as("color")
    }

    val productsDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
      .option("url", ModelConfig.MYSQL_JDBC_URL)
      .option("dbtable", "profile_tags.tbl_dim_products")
      .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
      .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
      .load()
    //    productsDF.printSchema()
    //    productsDF.show(20,false)
    /*
    +---+------------+
    |id |product_name|
    +---+------------+
    |1  |4K电视        |
    |2  |Haier/海尔冰箱  |
     */
    //构建商品
    val productWhen: Column = {
      // 定义变量when语句
      var prodWhen: Column = null
      productsDF
        .as[(Int, String)] // 将DataFrame转换为Dataset
        .rdd // 转换为RDD
        .collectAsMap() // 转换为Map集合
        .foreach { case (productId, productName) =>
          if (null == prodWhen) {
            // 第一次构建WHEN条件，进行初始化
            prodWhen = when($"producttype".equalTo(productName), productId)
          } else {
            prodWhen = prodWhen.when($"producttype".equalTo(productName), productId)
          }
        }
      prodWhen.otherwise(0).as("product")
    }

    // 2.5. 根据运营规则标注的部分数据，
    // TODO：依据具体运行分析统计，认为标注购物商品性别
    val labelColumn: Column = {
      when($"ogcolor".equalTo("樱花粉")
        .or($"ogcolor".equalTo("白色"))
        .or($"ogcolor".equalTo("香槟色"))
        .or($"ogcolor".equalTo("香槟金"))
        .or($"productType".equalTo("料理机"))
        .or($"productType".equalTo("挂烫机"))
        .or($"productType".equalTo("吸尘器/除螨仪")), 1) //女
        .otherwise(0) //男
        .alias("label") //决策树预测label
    }

    //标注数据，按颜色和商品标注每个商品是男性还是女性买的
    val genderDF: DataFrame = businessDF
      .join(ordersDF, businessDF("cordersn") === ordersDF("ordersn"))
      .select(
        $"memberid".as("uid"),
        colorWhen, productWhen, labelColumn
      )
//    genderDF.printSchema()
//    genderDF.show(100,false)
    /*
    |uid|color|product|label|
    +---+-----+-------+-----+
    |653|11   |2      |0    |
    |653|1    |8      |0    |
     */

    // 4. 加载模型，模型存在就加载；不存在，使用数据集构建模型
    // 特征工程：提取特征值
//    val featuresDF: DataFrame = featuresTransform(genderDF)
//    val dtcModel: DecisionTreeClassificationModel = loadModel(featuresDF)

    //TODO：使用算法模型预测用户购买商品所属的性别（男、女），否则认为标注数据（要命的）
//    val predictionDF = dtcModel.transform(featuresDF)

    // 获取管道模型PipelineModel
//    val pipelineModel: PipelineModel = loadPipelineModel(genderDF)
//    val predictionDF = pipelineModel.transform(genderDF)

    val pipelineModel: PipelineModel = trainBestModel(genderDF)
    val predictionDF: DataFrame = pipelineModel.transform(genderDF)

    // 5. 打标签：按照用户ID分组，分别统计出各个商品中男性、女性占比，依据规则确定性别
    /*
      计算每个用户近半年（时间范围，依据具体业务而定）内所有订单中的男性商品超过60%则认定该用户为男，
        或近半年内所有订单中的女性品超过60%则认定该用户为女。
     */
    // 5.1. 计算USG值
    val usgDF: DataFrame = predictionDF
      .select(
        $"uid",
        when($"label" === 0, 1).otherwise(0).as("male"),
        when($"label" === 1, 1).otherwise(0).as("female")
      )
      .groupBy($"uid")
      .agg(
        count($"uid").as("total"),
        sum($"male").as("male_count"),
        sum($"female").as("female_count")
      )
//    usgDF.printSchema()
//    usgDF.show(50,false)
/*
|uid|total|male_count|female_count|
+---+-----+----------+------------+
|487|114  |87        |27          |
|433|104  |80        |24          |
|595|121  |78        |43          |
 */

    // 5.2. 获取属性标签的规则，转换为Map集合: Map[rule, tagId]
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)

    // 5.3. 自定义UDF函数，打标签
    val gender_to_tag: UserDefinedFunction = udf(
      (total: Long, maleCount: Int, femaleCount: Int) => {
        // 计算男性和女性占比
        val maleRate = maleCount / total
        val femaleRate = femaleCount / total
        if (maleRate >= 0.6) {
          ruleMap("0")
        } else if (femaleRate >= 0.6) {
          ruleMap("1")
        } else {
          ruleMap("-1")
        }
      }
    )
    // 5.4. 使用UDF函数，真正打标签
    val modelDF: DataFrame = usgDF
      .select(
        $"uid",
        gender_to_tag($"total", $"male_count", $"female_count").as("tagId")
      )
    modelDF.printSchema()
    modelDF.show(100,false)

    modelDF
  }

  /**
   * 针对数据集进行特征工程：特征提取、特征转换及特征选择
   * @param dataframe 数据集
   * @return 数据集，包含特征列features: Vector类型和标签列label
   */
  def featuresTransform(dataframe: DataFrame): DataFrame = {
    /*
    root
     |-- uid: string (nullable = true)
     |-- color: integer (nullable = false)
     |-- product: integer (nullable = false)
     |-- label: integer (nullable = false)
     */
    // a. 组合特征：color和product两列
    val assembler = new VectorAssembler()
      .setInputCols(Array("color", "product"))
      .setOutputCol("raw_features")
    val df1: DataFrame = assembler.transform(dataframe)

    // b. 类别特征索引化：告知决策树算法哪些时类别特征，经过索引化处理
    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMaxCategories(30) // 设置类别特征最大类别数目
      .fit(df1)
    val df2: DataFrame = indexer.transform(df1)

    // c. 返回特征数据
    df2
  }

  /**
   * 使用决策树分类算法训练模型，返回DecisionTreeClassificationModel模型
   * @return
   */
  def loadModel(dataframe: DataFrame): DecisionTreeClassificationModel = {
    // 1. 模型保存路径
    val modelPath: String = ModelConfig.MODEL_BASE_PATH +
      s"/${this.getClass.getSimpleName.stripSuffix("$")}"
    // 2. 判断路径是否存在，存在直接加载
    // 获取Hadoop 配置信息
    val hadoopConf = dataframe.sparkSession.sparkContext.hadoopConfiguration
    if(HdfsUtils.exists( hadoopConf,modelPath)){
      logWarning(s"正在加载<${modelPath}>模型..................")
      DecisionTreeClassificationModel.load(modelPath)
    }else {
      // i. 数据集划分：训练数据集和测试数据集
      val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2))
      trainingDF.persist(StorageLevel.MEMORY_AND_DISK).count()

      // ii. 使用决策树分类算法训练模型
      val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
        // 设置输入、输出列名称
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setPredictionCol("prediction")
        // 设置算法超参数
        .setImpurity("gini")
        .setMaxDepth(5) // 设置树的深度
        .setMaxBins(32) // 设置树的叶子数目，必须大于等于类别特征的类别个数
      val dtcModel: DecisionTreeClassificationModel = dtc.fit(trainingDF)
      println(dtcModel.toDebugString)

      // iii. . 模型评估，使用准确率（度）Accuracy评估
      val predictionDF: DataFrame = dtcModel.transform(testingDF)
      predictionDF.show(10, truncate = false)
      import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
      val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictionDF)
      println(s"accuracy = $accuracy")

      // iv. 保存模型
      logWarning(s"正在保存模型到<${modelPath}>..................")
      dtcModel.save(modelPath)

      // v. 返回模型
      dtcModel
    }
  }

  /**
   * 使用决策树分类算法训练模型，返回PipelineModel模型
   *
   * @return
   */
  def loadPipelineModel(dataframe: DataFrame): PipelineModel = {
    // 将数据集划分为训练集和测试集
    val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2))

    // a. 组合特征：color和product两列
    val assembler = new VectorAssembler()
      .setInputCols(Array("color", "product"))
      .setOutputCol("raw_features")

    // b. 类别特征索引化：告知决策树算法哪些时类别特征，经过索引化处理
    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMaxCategories(30) // 设置类别特征最大类别数目
    //	.fit(dataframe)

    // c. 使用决策树分类算法训练模型
    val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
      // 设置输入、输出列名称
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      // 设置算法超参数
      .setImpurity("gini")
      .setMaxDepth(5) // 设置树的深度
      .setMaxBins(32) // 设置树的叶子数目，必须大于等于类别特征的类别个数

    // 4. 构建Pipeline管道
    val pipeline: Pipeline = new Pipeline().setStages(
      Array(assembler, indexer, dtc)
    )

    // 5. 使用训练数据集训练管道获取模型
    val pipelineModel: PipelineModel = pipeline.fit(trainingDF)

    // 6. 模型评估
    val predictionDF: DataFrame = pipelineModel.transform(testingDF)
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictionDF)
    println(s"accuracy = $accuracy")

    // 7. 返回模型
    pipelineModel
  }

  /**
   * 调整算法超参数，找出最优模型
   * @param dataframe 数据集
   * @return
   */
  def trainBestModel(dataframe: DataFrame): PipelineModel = {
    // a. 组合特征：color和product两列
    val assembler = new VectorAssembler()
      .setInputCols(Array("color", "product"))
      .setOutputCol("raw_features")

    // b. 类别特征索引化：告知决策树算法哪些时类别特征，经过索引化处理
    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMaxCategories(30) // 设置类别特征最大类别数目

    // c. 使用决策树分类算法训练模型
    val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
      // 设置输入、输出列名称
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    // 4. 构建Pipeline管道
    val pipeline: Pipeline = new Pipeline().setStages(
      Array(assembler, indexer, dtc)
    )

    // 5. 构建网格参数Map集合，设置不同模型学习器的参数值
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(dtc.impurity, Array("gini", "entropy"))
      .addGrid(dtc.maxDepth, Array(5))
      .addGrid(dtc.maxBins, Array(32))
      .build()

    // 6. 创建模型评估器
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    // 7. 创建TrainValidationSplit
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline) // 设置模型学习器
      .setEvaluator(evaluator) // 设置模型评估器
      .setEstimatorParamMaps(paramGrid) // 模型学习器参数
      .setTrainRatio(0.8) // 训练数据集占比

    // 8. 使用数据集训练和验证模型
    val model: TrainValidationSplitModel = trainValidationSplit.fit(dataframe)

    // 9. 获取最佳模型
    model.bestModel.asInstanceOf[PipelineModel]
  }
}

object UsgModel {
  def main(args: Array[String]): Unit = {

    val modelDF = new UsgModel
    modelDF.executeModel(380L)
  }
}
