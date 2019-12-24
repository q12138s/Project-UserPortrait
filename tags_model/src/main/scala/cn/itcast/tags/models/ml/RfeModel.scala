package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelConfig, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel


/**
 * 开发标签模型（挖掘类型标签）：用户活跃度RFE模型
 *  Recency: 最近一次访问时间,用户最后一次访问距今时间
 *  Frequency: 访问频率,用户一段时间内访问的页面总次数,
 *  Engagements: 页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面
 * 浏览量、下载量、 视频播放数量等
 **/
class RfeModel extends AbstractModel("用户活跃度模型RFE",ModelType.ML) {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = businessDF.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //从业务数据中心计算RFE的值
    val rfeDF: DataFrame = businessDF
      .groupBy($"global_user_id")
      .agg(
        max($"log_time").as("last_time"),
        count($"loc_url").as("frequency"),
        countDistinct($"loc_url").as("engagements")
      )
      .select(
        $"global_user_id".as("uid"),
        datediff(
          date_sub(current_timestamp(), 100),
          $"last_time"
        ).as("recency"), $"frequency", $"engagements"
      )
    //    rfeDF.printSchema()
    //    rfeDF.show(50,false)
    /*
    +---+-------+---------+-----------+
|uid|recency|frequency|engagements|
+---+-------+---------+-----------+
|1  |29     |418      |270        |
|102|29     |415      |271        |
|107|29     |425      |281        |
|110|29     |357      |250        |
     */

    //依据RFE规则，给RFE打Score分数
    /*
			R：0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
			F：≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
			E：≥250=5分，200-249=4分，150-199=3分，149-50=2分，≤49=1分
		 */
    // rWhen表达式
    val rWhen = when(col("recency").between(1, 15), 5.0) //
      .when(col("recency").between(16, 30), 4.0) //
      .when(col("recency").between(31, 45), 3.0) //
      .when(col("recency").between(46, 60), 2.0) //
      .when(col("recency").geq(61), 1.0) //
    // fWhen表达式
    val fWhen = when(col("frequency").leq(99), 1.0) //
      .when(col("frequency").between(100, 199), 2.0) //
      .when(col("frequency").between(200, 299), 3.0) //
      .when(col("frequency").between(300, 399), 4.0) //
      .when(col("frequency").geq(400), 5.0) //
    // eWhen表达式
    val eWhen = when(col("engagements").lt(49), 1.0) //
      .when(col("engagements").between(50, 149), 2.0) //
      .when(col("engagements").between(150, 199), 3.0) //
      .when(col("engagements").between(200, 249), 4.0) //
      .when(col("engagements").geq(250), 5.0) //
    val rfeScoreDF: DataFrame = rfeDF.select(
      $"uid", rWhen.as("r_score"), //
      fWhen.as("f_score"), eWhen.as("e_score")
    )
    //    rfeScoreDF.printSchema()
    //    rfeScoreDF.show(50, truncate = false)
    /*
    |uid|r_score|f_score|e_score|
    +---+-------+-------+-------+
    |1  |4.0    |5.0    |5.0    |
    |102|4.0    |5.0    |5.0    |
    |107|4.0    |5.0    |5.0    |
     */

    //组合算法模型特征数据：features
    val rfeFeaturesDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "e_score"))
      .setOutputCol("features")
      .transform(rfeScoreDF)
    //    rfeFeaturesDF.printSchema()
    //    rfeFeaturesDF.show(20,false)

    //应用数据到算法中训练模型
    val kmeansModel: KMeansModel = loadModel(rfeFeaturesDF)

    //使用模型预测
    val predictionDF: DataFrame = kmeansModel.transform(rfeFeaturesDF)
//    predictionDF.printSchema()
//    predictionDF.show(50,false)
    /*
    +---+-------+-------+-------+-------------+----------+
    |uid|r_score|f_score|e_score|features     |prediction|
    +---+-------+-------+-------+-------------+----------+
    |1  |4.0    |5.0    |5.0    |[4.0,5.0,5.0]|0         |
    |102|4.0    |5.0    |5.0    |[4.0,5.0,5.0]|0         |
    |107|4.0    |5.0    |5.0    |[4.0,5.0,5.0]|0         |
     */

    //预测值和属性标签的规则rule关联，给每个用户打标签
    //获取KMeansModel中列簇中心点
    val clusterCenters: Array[linalg.Vector] = kmeansModel.clusterCenters

    //获取属性标签数据规则rule
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)

    //计算标签
    val clusterIndexTagMap: Map[Int, Long] = TagTools.convertIndexMap(clusterCenters,tagDF)

    //自定义UDF函数
    val index_to_tag: UserDefinedFunction = udf(
      (index: Int) => {
        clusterIndexTagMap(index)
      }
    )

    //打标签
    val modelDF: DataFrame = predictionDF
      .select(
        $"uid", // 用户ID
        index_to_tag($"prediction").as("tagId")
      )
    modelDF.show(100, truncate = false)

    modelDF
  }

  /**
   * 传递数据集（包含特征列features），使用KMeans算法训练模型
   * 当模型已经存在，直接加载模型，不存在在使用数据集训练模型
   *
   * @param dataframe 数据集
   * @return
   */
  def loadModel(dataframe: DataFrame): KMeansModel = {
    // 1. 模型保存路径
    val modelPath: String = ModelConfig.MODEL_BASE_PATH +
      s"/${this.getClass.getSimpleName.stripSuffix("$")}"
    // 2. 判断路径是否存在，存在直接加载
    // 获取Hadoop 配置信息
    val hadoopConf = dataframe.sparkSession.sparkContext.hadoopConfiguration
    if (HdfsUtils.exists(hadoopConf, modelPath)) {
      logWarning(s"正在加载<${modelPath}>模型..................")
      KMeansModel.load(modelPath)
    } else {
      // i. 数据集划分：训练数据集和测试数据集
      val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2))
      trainingDF.persist(StorageLevel.MEMORY_AND_DISK).count()
      // ii. 使用训练数据集训练模型
      logWarning(s"正在训练模型..................")
      val model: KMeansModel = new KMeans()
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setK(4) // 分为4个类簇
        .setMaxIter(10) // 设置最大的迭代次数
        .fit(trainingDF)
      // iii. 使用测试数据集评估模型
      val wssse: Double = model.computeCost(testingDF)
      println(s"WSSSE = $wssse")
      // iv. 保存模型
      logWarning(s"正在保存模型<$modelPath>..................")
      model.save(modelPath)
      // v. 返回模型
      model
    }

  }
}


  object RfeModel {
    def main(args: Array[String]): Unit = {
      val tagDF = new RfeModel
      tagDF.executeModel(369L)
    }
  }
