package cn.itcast.tags.tools
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 构建画像标签数据工具类：加载（读取）、保存（写入）及合并画像标签
 */
object ProfileTools {
  /**
   * 从HBase表中加载画像标签数据
   * @param spark SparkSession实例对象
   * @return
   */
    def loadProfile(spark:SparkSession):DataFrame={
//      val profileDF: DataFrame = HBaseTools.read(
//        spark, "bigdata-cdh01.itcast.cn", "2181", //
//        "tbl_profile", "user", Seq("userId", "tagIds")
//      )
//      profileDF
      import cn.itcast.tags.spark.hbase._
      spark.read
//        .format("cn.itcast.tags.spark.hbase")
        .option("zkHosts", "bigdata-cdh01.itcast.cn")
        .option("zkPort", "2181")
        .option("hbaseTable", "tbl_profile")
        .option("family", "user")
        .option("selectFields", "userId,tagIds")
//        .load()
        .hbase
    }

  /**
   * 将画像标签数据保存到HBase表中
   * @param profileDF 画像标签数据
   */
  def saveProfile(profileDF: DataFrame): Unit = {
//    HBaseTools.write(
//      profileDF, "bigdata-cdh01.itcast.cn", "2181", //
//      "tbl_profile", "user", "userId"
//    )
    import cn.itcast.tags.spark.hbase._
    profileDF.write
      .mode(SaveMode.Overwrite)
//      .format("cn.itcast.tags.spark.hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_profile")
      .option("family", "user")
      .option("rowKeyColumn", "userId")
//      .save()
      .hbase
  }

  def mergeProfileTags(modelDF:DataFrame,profileDF:DataFrame):DataFrame = {
    import modelDF.sparkSession.implicits._
    // 4.2、合并用户标签数据，关联操作
    val mergeDF: DataFrame = modelDF
      .join(profileDF, modelDF("uid") === profileDF("userId"), "left")

    // 自定义UDF函数，真正合并标签, 判断tagIds是否有值，有值就合并；没值直接返回tagId
    val merge_tag_udf: UserDefinedFunction = udf(
      (tagId: Long, tagIds: String) => {
        tagIds
          .split(",")
          .map(_.toLong)
          .:+(tagId)
          .distinct
          .mkString(",")
      }
    )

    //合并标签数据
    val newProfileDF: DataFrame = mergeDF.select(
      $"uid".as("userId"),
      when($"tagIds".isNull, $"tagId")
        .otherwise(merge_tag_udf($"tagId", $"tagIds"))
        .as("tagIds")
    )
    newProfileDF
  }

}
