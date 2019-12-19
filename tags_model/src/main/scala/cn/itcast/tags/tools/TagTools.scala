package cn.itcast.tags.tools
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

}
