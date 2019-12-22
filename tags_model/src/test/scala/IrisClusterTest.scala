import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用KMeans算法对鸢尾花数据进行聚类操作
 */
object IrisClusterTest {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .getOrCreate()
      import org.apache.spark.sql.functions._
      import spark.implicits._

    //获取数据
    val irisDF: DataFrame = spark.read
      .format("libsvm")
      .load("E:\\bigdata_project\\profile-tags\\datas\\iris_kmeans.txt")
    irisDF.printSchema()
    irisDF.show(10,false)

    // 2. 构建KMeans算法-> 模型学习器实例对象
    val means: KMeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prec")
      .setK(3)
      .setMaxIter(20)
    // 3. 应用数据集训练模型, 获取转换器
    val KMeansModel: KMeansModel = means.fit(irisDF)
    //获取聚类的簇的中心点
    KMeansModel.clusterCenters.foreach(println)
    // 4. 模型评估
    val WSSSE: Double = KMeansModel.computeCost(irisDF)
    println(s"WSSSE = $WSSSE")
    // 5. 使用模型预测
    val predictionDF: DataFrame = KMeansModel.transform(irisDF)
    predictionDF.printSchema()
    predictionDF.show(150,false)
    predictionDF
        .groupBy($"label",$"prec")
        .count()
        .show(150,false)
    spark.stop()
  }

}
