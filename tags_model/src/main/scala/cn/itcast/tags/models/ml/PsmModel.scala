package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelConfig, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

class PsmModel extends AbstractModel("消费敏感度PSM模型", ModelType.ML){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark = businessDF.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 1. 计算PSM值： psm = 优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比
    //ra: receivableAmount 应收金额
    val raColumn: Column = ($"orderamount" + $"couponcodevalue").as("ra")
    //da: discountAmount 优惠金额
    val daColumn: Column = $"couponcodevalue".as("da")
    //pa: practicalAmount 实收金额
    val paColumn: Column = $"orderamount".as("pa")
    //state: 订单状态，此订单是否是优惠订单
    val stateColumn: Column = when($"couponcodevalue" === 0.0, 0)
      .otherwise(1).as("state")

    //tdon 优惠订单数
    val tdonColumn: Column = sum($"state").as("tdon")
    //ton  总订单总数
    val tonColumn: Column = count($"state").as("ton")
    //tda 优惠总金额
    val tdaColumn: Column  = sum($"da").as("tda")
    //tra 应收总金额
    val traColumn: Column = sum($"ra").as( "tra")

    /*
			tdonr 优惠订单占比(优惠订单数 / 订单总数)
			tdar  优惠总金额占比(优惠总金额 / 订单总金额)
			adar  平均优惠金额占比(平均优惠金额 / 平均每单应收金额)
		*/
    val tdonrColumn: Column = ($"tdon" / $"ton").as("tdonr")
    val tdarColumn: Column = ($"tda" / $"tra").as("tdar")
    val adarColumn: Column = (
      ($"tda" / $"tdon") / ($"tra" / $"ton")
      ).as("adar")
    val psmColumn: Column = ($"tdonr" + $"tdar" + $"adar").as("psm")

    val psmDF: DataFrame = businessDF
      .select(
        $"memberid".as("uid"),
        daColumn, paColumn, raColumn, stateColumn
      )
      // 按照用户Id分组
      .groupBy($"uid")
      .agg(
        tdonColumn, tonColumn, tdaColumn, traColumn
      )
      // 计算优惠订单占比、优惠总金额占比和平均优惠金额占比
      .select($"uid", tdonrColumn, tdarColumn, adarColumn)
      // 计算PSM值
      .select($"*", psmColumn)
      // 返现，当没有优惠订单时，计算adar时为null（分母为0），表示此用户对价格敏感度很低，设置很小的值
      .select(
        $"*", // 获取所有列
        when($"psm".isNull, 0.000000000001)
          .otherwise($"psm").as("psm_score")
      )
//    psmDF.printSchema()
//    psmDF.show(100, truncate = false)

    // 2. 使用KMeans算法训练模型预测
    // 2.1. 组成特征值
    val psmFeaturesDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("psm_score"))
      .setOutputCol("features")
      .transform(psmDF)
    // 2.2. 加载模型，如果存在就加载获取；不存在就训练模型
    val kMeansModel: KMeansModel = loadModel(psmFeaturesDF)
    // 2.3. 模型预测
    val predictionDF: DataFrame = kMeansModel.transform(psmFeaturesDF)

    // 3. 构建标签
    // 3.1. 获取类簇中心点
    import org.apache.spark.ml.linalg
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    // 3.2. 结合属性标签规则rule和列簇中心点索引构建映射Map集合
    val clusterIndexTagMap: Map[Int, Long] = TagTools.convertIndexMap(clusterCenters, tagDF)
    // 3.3. 自定义UDF函数
    val index_to_tag = udf(
      (index: Int) => {
        clusterIndexTagMap(index)
      }
    )
    // 3.4 打标签
    val modelDF: DataFrame = predictionDF
      .select(
        $"uid", // 用户ID
        index_to_tag($"prediction").as("tagId")
      )
    modelDF.show(100, truncate = false)

    // 4. 返回标签数据
    modelDF
  }

  /**
   * 传递数据集（包含特征列features），使用KMeans算法训练模型
   *      当模型已经存在，直接加载模型，不存在在使用数据集训练模型
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
    if(HdfsUtils.exists( hadoopConf,modelPath)){
      logWarning(s"正在加载<${modelPath}>模型..................")
      KMeansModel.load(modelPath)
    }else{
      // i. 数据集划分：训练数据集和测试数据集
      val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2))
      trainingDF.persist(StorageLevel.MEMORY_AND_DISK).count()
      // ii. 使用训练数据集训练模型
      logWarning(s"正在训练模型..................")
      val model: KMeansModel = new KMeans()
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setK(5) // 分为5个类簇
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
object PsmModel{
  def main(args: Array[String]): Unit = {
    val tagDF = new PsmModel
    tagDF.executeModel(374L)
  }
}
