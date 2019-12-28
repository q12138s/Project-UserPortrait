package cn.itcast.tags.tools
import cn.itcast.tags.models.ModelConfig
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
      /*
      spark.read
//        .format("cn.itcast.tags.spark.hbase")
        .option("zkHosts", "bigdata-cdh01.itcast.cn")
        .option("zkPort", "2181")
        .option("hbaseTable", "tbl_profile")
        .option("family", "user")
        .option("selectFields", "userId,tagIds")
//        .load()
        .hbase
       */
      spark.read
//        .format("cn.itcast.tags.spark.hbase")
        .option("zkHosts", ModelConfig.PROFILE_TABLE_ZK_HOSTS)
        .option("zkPort", ModelConfig.PROFILE_TABLE_ZK_PORT)
        .option("hbaseTable", ModelConfig.PROFILE_TABLE_NAME)
        .option("family", ModelConfig.PROFILE_TABLE_FAMILY_USER)
        .option("selectFields", ModelConfig.PROFILE_TABLE_SELECT_FIELDS)
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
    /*
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
     */
    profileDF.write
      .mode(SaveMode.Overwrite)
//      .format("cn.itcast.tags.spark.hbase")
      .option("zkHosts", ModelConfig.PROFILE_TABLE_ZK_HOSTS)
      .option("zkPort", ModelConfig.PROFILE_TABLE_ZK_PORT)
      .option("hbaseTable", ModelConfig.PROFILE_TABLE_NAME)
      .option("family", ModelConfig.PROFILE_TABLE_FAMILY_USER)
      .option("rowKeyColumn", ModelConfig.PROFILE_TABLE_ROWKEY_COL)
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


  /**
   * 将每个标签模型计算用户标签与历史画像标签数据合并
   * @param modelDF 标签数据，字段为uid和tagId
   * @param profileDF 画像标签数据，字段为userId和tagIds
   * @param ids 标签所有ID
   * @return
   */
  def mergeProfileTags(modelDF: DataFrame, profileDF: DataFrame,
                       ids: Set[Long]): DataFrame = {
    import modelDF.sparkSession.implicits._
    // a. 依据用户ID关联标签数据
    val mergeDF: DataFrame = modelDF
      // 按照模型数据中userId与画像数据中rowKey关联
      .join(profileDF, modelDF("uid") === profileDF("userId"), "left")

    // b. 自定义UDF函数，合并已有标签与计算标签
    val merge_tags_udf = udf(
      (tagId: Long, tagIds: String) => {
        if(null != tagIds){
          // i. // 画像标签Set集合
          val tagIdsSet: Set[Long] = tagIds.split(",").map(_.toLong).toSet
          // ii. 交集
          val interset: Set[Long] = tagIdsSet & ids
          // iii. 合并新标签
          val newTagIds: Set[Long] = tagIdsSet -- interset + tagId
          // iv. 返回标签
          newTagIds.mkString(",")
        }else{
          tagId.toString
        }
      }
    )
    // c. 合并标签数据
    val newProfileDF: DataFrame = mergeDF.select(
      $"uid".as("userId"), //
      merge_tags_udf($"tagId", $"tagIds").as("tagIds")//
    )

    // d. 返回标签画像数据
    newProfileDF
  }

}
