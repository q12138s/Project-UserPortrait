package cn.itcast.tags.meta

import cn.itcast.tags.models.ModelConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * 加载业务数据工具类：
 *    解析业务标签规则rule，依据规则判断具体数据源，记载业务数据
 */
object MetaParse extends Logging{

  def parseRuleToParams(tagDF: DataFrame): Map[String, String] = {
    import tagDF.sparkSession.implicits._

    //获取业务标签规则rule
    val tagRule: String = tagDF
      // 依据level级别，获取业务标签数据
      .filter($"level" === 4)
      .select($"rule") // 获取规则rule
      .head()
      .getAs[String]("rule")
    logWarning(s"========= { $tagRule } ========")

    // 2、将规则解析为Map集合
    val ruleMap: Map[String, String] = tagRule
      .split("\n") // 使用换行符分割
      // 再次按照等号分割，转换为二元组
      .map{line =>
        val Array(attrKey, attrValue) = line.split("=")
        (attrKey, attrValue)
      }
      .toMap // 将数组转换为Map集合（由于数组中数据类型为二元组）
    logWarning(s"========== { ${ruleMap.mkString(", ")} } ==========")

    // 3. 业务数据源的元数据Map集合
    ruleMap
  }


  /**
   * 依据inType判断数据源，封装元数据Meta，加载业务数据
   * @param spark SparkSession实例对象
   * @param paramsMap 业务数据源参数集合
   * @return
   */
  def parseMetaToData(spark: SparkSession,
                      paramsMap: Map[String, String]): DataFrame = {
    // 1. 获取业务数据源类型
    val inType = paramsMap("inType").toLowerCase

    // 2. 依据数据源类型获取业务数据
    val businessDF = inType match {
      case "hbase" =>
        // 封装HBase数据源中元数据信息HBaseMeta
        val hbaseMeta = HBaseMeta.getHBaseMeta(paramsMap)
        // 从HBase表读取数据
        spark.read
          .format("cn.itcast.tags.spark.hbase")
          .option("zkHosts", hbaseMeta.zkHosts)
          .option("zkPort", hbaseMeta.zkPort)
          .option("hbaseTable", hbaseMeta.hbaseTable)
          .option("family", hbaseMeta.family)
          .option("selectFields", hbaseMeta.selectFieldNames)
          .load()
      case "mysql" =>
        // 将参数Map集合封装到MySQLMeta中
        val mysqlMeta: MySQLMeta = MySQLMeta.getMySQLMeta(paramsMap)
        // 加载业务数据：从MySQL数据库中
        spark.read
          .format("jdbc")
          .option("driver", mysqlMeta.driver)
          .option("url", mysqlMeta.url)
          .option("dbtable", mysqlMeta.sql)
          .option("user", mysqlMeta.user)
          .option("password", mysqlMeta.password)
          .load()
      case "hive" =>
        // 将参数Map集合封装到HiveMeta中
        val hiveMeta: HiveMeta = HiveMeta.getHiveMeta(paramsMap)
        // 加载业务数据：从Hive数据仓库, SparkSession构建实例时集成Hive
        spark.read
          .table(hiveMeta.hiveTable)
          .select(hiveMeta.selectFieldNames: _*)
      case "hdfs" =>
        // 将参数Map集合封装到HdfsMeta中
        val hdfsMeta: HdfsMeta = HdfsMeta.getHdfsMeta(paramsMap)
        // 加载业务数据：从HDFS文件系统读取
        spark.read
          .option("sep", hdfsMeta.sperator)
          .option("header", "true")
          .csv(hdfsMeta.inPath)
          .select(hdfsMeta.selectFieldNames: _*)
    }

    // 3. 返回加载的业务数据
    businessDF
  }
}
