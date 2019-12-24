package cn.itcast.tags.tools
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg
/**
 * 针对标签进行相关操作工具类
 */
object TagTools {
  /**
   * 将属性标签数据中规则rule与tagId转换为Map集合
   * @param ruleTagDF
   * @return
   */
  def convertMap(ruleTagDF: DataFrame): Map[String, Long] = {
    import ruleTagDF.sparkSession.implicits._
    ruleTagDF
      .filter($"level" === 5)
      .select($"rule", $"id".as("tagId"))
      .as[(String, Long)]
      .rdd
      .collectAsMap().toMap
  }

  /**
   * 依据业务数据中业务字段的值与规则集合Map，使用UDF函数，进行打标签
   * @param dataframe 业务数据
   * @param field 业务字段
   * @param ruleTagDF 标签数据
   * @return 标签数据
   */
  def ruleMatchTag(dataframe: DataFrame, field: String,
                   ruleTagDF: DataFrame): DataFrame = {
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._
    // 1. 获取属性标签数据，转换为Map集合
    val ruleMap: Map[String, Long] = convertMap(ruleTagDF)
    //采用广播变量方式将数据广播到Executor中使用
    val ruleMapBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(ruleMap)

    // 2. 自定义UDF函数
    val job_to_tag: UserDefinedFunction = udf(
      (job: String) => {
        ruleMapBroadcast.value(job)
      }
    )

    // 3. 打标签（使用UDF函数）
    val modelDF: DataFrame = dataframe
      .select($"id".as("uid"), job_to_tag(col(field)).as("tagId"))
    modelDF
  }

  /**
   * 将属性标签中规则rule拆分为范围（start，end）
   * @param ruleTagDF
   * @return
   */
  def convertTuple(ruleTagDF: DataFrame): DataFrame = {
    // 导入隐式转换和函数库
    import org.apache.spark.sql.functions._
    import ruleTagDF.sparkSession.implicits._
    // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.split("-").map(_.toInt)
        // 返回二元组
        (start, end)
      }
    )

    // 2. 获取属性标签数据，解析规则rule
    val ruleDF: DataFrame = ruleTagDF
      .filter($"level" === 5) // 5级标签
      .select(
        $"id".as("tagId"), //
        rule_to_tuple($"rule").as("rules") //
      )
      // 获取起始start和结束end
      .select(
        $"tagId", //
        $"rules._1".as("start"), //
        $"rules._2".as("end") //
      )
    //ruleDF.show(20, truncate = false)

    // 3. 返回标签规则
    ruleDF

  }

  /**
   * 将KMeans模型中类簇中心点索引对应到属性标签的标签ID
   * @param clusterCenters KMeans模型类簇中心点
   * @param ruleTagDF 属性标签数据
   * @return
   */
  def convertIndexMap(clusterCenters: Array[linalg.Vector],
                      ruleTagDF: DataFrame): Map[Int, Long] = {

    // 1. 获取属性标签（5级标签）数据，选择id,rule
    val rulesMap: Map[String, Long] = TagTools.convertMap(ruleTagDF)

    // 2. 获取聚类模型中簇中心及索引
    val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters
      .zipWithIndex
      .map{case(vector, centerIndex) => (centerIndex, vector.toArray.sum)}
      .sortBy{case(_, rfm) => - rfm}
      .zipWithIndex

    // 3. 聚类类簇关联属性标签数据rule，对应聚类类簇与标签tagId
    val indexTagMap: Map[Int, Long] = centerIndexArray
      .map{case((centerIndex, _), index) =>
        val tagId = rulesMap(index.toString)
        // （类簇中心点索引，属性标签TagId）
        (centerIndex, tagId)
      }
      .toMap

    // 3. 返回类簇索引及对应TagId的Map集合
    indexTagMap
  }

}
