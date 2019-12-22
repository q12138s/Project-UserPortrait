package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}

/**
 * 标签模型开发，客户价值RFM模型
 */
class RfmModel extends AbstractModel("客户价值RFM模型",ModelType.ML){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // 1. 从订单数据获取字段值，计算每个用户RFM值
    val rfmDF: DataFrame = businessDF
      .groupBy($"memberid")
      .agg(
        max($"finishtime").as("finish_time"), //
        count($"ordersn").as("frequency"), //
        sum($"orderamount").as("monetary")
      )
      .select(
        $"memberid".as("uid"),
        datediff(current_timestamp(), from_unixtime($"finish_time")).as("recency"),
        $"frequency",
        round($"monetary", 2).as("monetary")
      )
//    rfmDF.printSchema()
//    rfmDF.show(50,false)
    // 2. 按照规则，给RFM值打分Score
    /*
			R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
			F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
			M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
		 */
    // 计算R表达式
    val rWhen: Column = when($"recency".between(1, 3), 5)
      .when($"recency".between(4, 6), 4)
      .when($"recency".between(7, 9), 3)
      .when($"recency".between(10, 15), 2)
      .when($"recency".geq(16), 1)
      .as("r_score")
    // 计算F表达式
    val fWhen: Column = when($"frequency".geq(200), 5)
      .when($"frequency".between(150, 199), 4)
      .when($"frequency".between(100, 149), 3)
      .when($"frequency".between(50, 99), 2)
      .when($"frequency".between(1, 49), 1)
      .as("f_score")
    // 计算M表达式
    val mWhen: Column = when($"monetary".geq(200000), 5)
      .when($"monetary".between(100000, 199999), 4)
      .when($"monetary".between(50000, 99999), 3)
      .when($"monetary".between(10000, 49999), 2)
      .when($"monetary".between(1, 9999), 1)
      .as("m_score")
    // 进行打分Score
    val rfmScoreDF: DataFrame = rfmDF.select($"uid", rWhen, fWhen, mWhen)
//    rfmScoreDF.printSchema()
//    rfmScoreDF.show(50, truncate = false)

    // 3. 将RFM数据使用KMeans算法聚类（K=7个）
    // 3.1. 将R\F\M值组成为特征features向量
    val rfmFeaturesDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
      .transform(rfmScoreDF)

    //训练模型
    val (wssse,kmeansModel, predictionDF) = trainModel(rfmFeaturesDF)

    // 3.3. 模型评估
//    val kmeansModel: KMeansModel = kmeans.fit(rfmFeaturesDF)
//    val wssse: Double = kmeansModel.computeCost(rfmFeaturesDF)
    println(s"wssse = $wssse")

    // 3.4. 使用模型预测
//    val predictionDF: DataFrame = kmeansModel.transform(rfmFeaturesDF)
//    predictionDF.printSchema()
//    predictionDF.show(20,false)
    /*
    |uid|r_score|f_score|m_score|features     |prediction|
    +---+-------+-------+-------+-------------+----------+
    |1  |1      |5      |5      |[1.0,5.0,5.0]|2         |
    |102|1      |3      |4      |[1.0,3.0,4.0]|1         |
    |107|1      |3      |4      |[1.0,3.0,4.0]|1         |
    |110|1      |2      |4      |[1.0,2.0,4.0]|3         |
     */

    // 4. 依据预测值和属性标签规则rule进行打标签
    // 4.1. 获取类簇中心点
    val clusterCenters: Array[linalg.Vector] = kmeansModel.clusterCenters
    val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters.zipWithIndex
      .map {
        case (vector, clusterIndex) =>
          (vector.toArray.sum, clusterIndex)
      }
      .sortBy(-_._1)
      .zipWithIndex
//      .foreach(println)

    // 4.2. 获取属性标签的规则rule
    val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)

    // 4.3. 遍历类簇中心点Array，匹配获取标签tagId
    val clusterIndexMap: Map[Int, Long] = clusterIndexArray.map {
      case ((_, clusterIndex), index) =>
        //根据index,获取tagId
        val tagId: Long = ruleMap(index.toString)
        (clusterIndex, tagId)
    }.toMap
    val clusterIndexMapBroadcast: Broadcast[Map[Int, Long]] = spark.sparkContext.broadcast(clusterIndexMap)

    // 4.4. 自定义UDF函数, 依据clusterIndex获取tagId
    val index_to_tag: UserDefinedFunction = udf(
      (index: Int) => {
        clusterIndexMapBroadcast.value(index)
      }
    )

    // 4.5. 匹配给没有用户打标签
    val modelDF: DataFrame = predictionDF.select(
      $"uid",
      index_to_tag($"prediction").as("tagId")
    )
    modelDF.printSchema()
    modelDF.show(50,false)

    // 5. 返回标签数据
    modelDF

  }

  /**
   * 最初使用RFM值直接使用和设置超参数训练模型
   * @param dataframe
   * @return
   */
  def trainRfmModel(dataframe: DataFrame): KMeansModel = {
    // 使用KMeans聚类
    val kmeans: KMeans = new KMeans()
      // 设置特征列名称
      .setFeaturesCol("features")
      // 设置预测列名称
      .setPredictionCol("prediction")
      // 设置算法的超参数， 默认值为20
      .setMaxIter(20)
      .setInitMode("k-means||")// 表示KMeans算法如何初始化K的类簇点
      // 设置聚类列簇个数
      .setK(7)
    val kmeansModel: KMeansModel = kmeans.fit(dataframe)
    // 返回模型
    kmeansModel
  }

  /**
   * 对特征数据进行归一化处理，训练模型
   * @param dataframe
   * @return
   */
  def trainModel(dataframe: DataFrame): (Double,KMeansModel, DataFrame) = {
    // 使用最小最大归一化处理特征数据
    val scaler: MinMaxScalerModel = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .fit(dataframe)
    val scalarDF: DataFrame = scaler.transform(dataframe)

    // 使用KMeans聚类
    val kmeans: KMeans = new KMeans()
      // 设置特征列名称
      .setFeaturesCol("scaledFeatures")
      // 设置预测列名称
      .setPredictionCol("prediction")
      // 设置算法的超参数， 默认值为20
      .setMaxIter(20)
      .setInitMode("k-means||")// 表示KMeans算法如何初始化K的类簇点
      // 设置聚类列簇个数
      .setK(7)
    val kmeansModel: KMeansModel = kmeans.fit(scalarDF)

    //模型评估指标WSSSE
    val wssse: Double = kmeansModel.computeCost(scalarDF)

    // 预测
    val predictionDF: DataFrame = kmeansModel.transform(scalarDF)

    // 返回预测的值
    (wssse,kmeansModel, predictionDF)
  }
}
object RfmModel{
  def main(args: Array[String]): Unit = {

    val tagDF = new RfmModel
    tagDF.executeModel(361L)

  }
}
