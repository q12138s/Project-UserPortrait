package cn.itcast.tags.models.rmd

import cn.itcast.tags.models.{AbstractModel, ModelConfig, ModelType}
import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 标签模型开发：品牌偏好BP模型
 * 使用基于隐语义模型的ALS推荐算法，构建用户购物偏好模型
 */
class BpModel extends AbstractModel("用户购物偏好BP模型", ModelType.ML) {
  /*
		使用ALS交替最小二乘法推荐算法构建推荐模型：
			将X矩阵（用户对物品的评分矩阵） = U（用户因子矩阵） * I（物品因子矩阵）
		数据：
			评分rating ->  从用户行为日志数据中获取浏览商品Item，统计浏览浏览/点击次数
				U_10001, I_101, 4（点击浏览次数）
				U_10001, I_102, 6（点击浏览次数）
				U_10001, I_104, 1（点击浏览次数）
				U_10002, I_101, 5（点击浏览次数）
				U_10002, I_101, 1（点击浏览次数）
			物品点击浏览次数作为评分rating：
				属于隐式反馈
		ALS模型：
			两个因子矩阵：U（用户因子矩阵） * I（物品因子矩阵）
		核心点：
			如何获取物品ItemId，从访问log_url中解析获取
				http://www.eshop.com/product/11771.html?ebi=ref-lst-1-17
	 */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = businessDF.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 1. 自定义UDF函数，从log_url中获取物品ID
    val url_to_product: UserDefinedFunction = udf(
      (url: String) => {
        var productId: String = null
        // 字符串截取方式：http://www.eshop.com/product/10781.html?ebi=ref-ixv5-5-main-1-7
        if (url.contains("/product/") && url.contains(".html")) {
          val startIndex: Int = url.indexOf("/product/")
          val endIndex: Int = url.indexOf(".html")
          if (endIndex > startIndex) {
            productId = url.substring(startIndex + 9, endIndex)
          }
        }
        productId
      }
    )

    // 判断获取ProductId，如果不转转换数字类型，直接过滤
    val validate_product_udf: UserDefinedFunction = udf(
      (productId: String) => {
        try {
          productId.toInt
          // 可以转换返回 true
          true
        } catch {
          case _: Exception => false
        }
      }
    )

    // 2. 构建用户对物品的评分（用户浏览物品的次数）
    val ratingsDF: DataFrame = businessDF
      .filter($"loc_url".isNotNull) // 获取loc_url不为null
      .select(
        $"global_user_id".as("userId"),
        url_to_product($"loc_url").as("productId")
      )
      // 过滤数据，productId为null
      .filter($"userId".isNotNull && $"productId".isNotNull && validate_product_udf($"productId"))
      // 按照用户ID和商品Id进行分组，统计每个用户点击每个商品的次数
      .groupBy($"userId", $"productId")
      .count()
      // 将数据转换为Double类型
      .select(
        $"userId".cast(DoubleType),
        $"productId".cast(DoubleType),
        $"count".as("rating").cast(DoubleType)
      )
    ratingsDF.printSchema()
    ratingsDF.show(100, truncate = false)
    /*
    +------+---------+------+
|userId|productId|rating|
+------+---------+------+
|530.0 |5543.0   |1.0   |
|498.0 |7147.0   |1.0   |
|192.0 |9303.0   |1.0   |
     */

    //加载模型，存在直接加载，不存在，训练模型并保存模型
    val alsModel: ALSModel = loadModel(ratingsDF)
    //使用推荐模型推荐
    // 给用户推荐商品: Top5
    val rmdItemsDF: DataFrame = alsModel.recommendForAllUsers(5)
//    rmdItemsDF.printSchema()
//    rmdItemsDF.show(10, truncate = false)
    // 4.2. 给物品推荐用户
    /*
      val rmdUsersDF: DataFrame = alsModel.recommendForAllItems(5)
      rmdUsersDF.printSchema()
      rmdUsersDF.show(10, truncate = false)
    */

    //存储推荐数据
    /*
    给用户推荐的商品存储到Hbase表中
     */
    val modelDF: DataFrame = rmdItemsDF
      .select(
        $"userId", //
        $"recommendations.productId".as("productIds"), //
        $"recommendations.rating".as("ratings") //
      )
      //将商品和评分列类型转换（Array数组类型 -》字符串类型）
      .select(
        $"userId".cast(StringType),
        concat_ws(",", $"productIds").as("productIds"),
        concat_ws(",", $"ratings").as("ratings")
      )
    modelDF.printSchema()
    modelDF.show(100,false)

    import cn.itcast.tags.spark.hbase._
    modelDF.write
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_rmd_items")
      .option("family", "info")
      .option("rowKeyColumn", "userId")
      .hbase



    null
  }

  def loadModel(dataframe: DataFrame): ALSModel = {

    // 1. 模型保存路径
    val modelPath: String = ModelConfig.MODEL_BASE_PATH +
      s"/${this.getClass.getSimpleName.stripSuffix("$")}"

    // 2. 判断路径是否存在，存在直接加载
    // 获取Hadoop 配置信息
    val hadoopConf = dataframe.sparkSession.sparkContext.hadoopConfiguration
    if (HdfsUtils.exists(hadoopConf, modelPath)) {
      logWarning(s"正在加载<${modelPath}>模型..................")
      ALSModel.load(modelPath)
    } else {
      // 模型路径不存在，训练模型
      logWarning(s"正在训练模型..................")
      // 由于ALS算法属于迭代算法，所以将数据集缓存
      dataframe.persist(StorageLevel.MEMORY_AND_DISK).count()

      // 3. 使用ALS算法构建模型
      // TODO: 此处实际项目中，单独训练模型，获取最佳模型，保存模型；当使用模型预测时，直接加载即可
      // 3.1 构建ALS算法实例对象
      val als: ALS = new ALS()
        .setUserCol("userId") //用户id列名称
        .setItemCol("productId") //商品ID列名城管
        .setRatingCol("rating") //用户对商品评分列名称
        .setPredictionCol("prediction")
        //算法超参数
        .setRank(10) //因子矩阵的维度Rank
        .setMaxIter(10) //交替最大迭代次数
        .setImplicitPrefs(true) //评分数据属于隐式反馈
        .setColdStartStrategy("drop") //表示ALS算法属于冷启动
        .setRegParam(1.0)
      // 3.2 使用数据训练模型
      val alsModel: ALSModel = als.fit(dataframe)

      // 3.3 模型评估
      val predictionDF: DataFrame = alsModel.transform(dataframe)
      predictionDF.printSchema()
      predictionDF.show(100, false)
      import org.apache.spark.ml.evaluation.RegressionEvaluator
      val evaluator: RegressionEvaluator = new RegressionEvaluator()
        .setLabelCol("rating")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse: Double = evaluator.evaluate(predictionDF)
      println(s"RMSE = $rmse")
      // 保存模型
      logWarning(s"正在保存模型<${modelPath}>..................")
      alsModel.save(modelPath)
      // 返回模型
      alsModel
    }
  }
}
object BpModel {
  def main(args: Array[String]): Unit = {
    val tagDF = new BpModel
    tagDF.executeModel(384L)

  }
}
