package cn.itcast.tags.models

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.spark.sql._
import cn.itcast.tags.tools.ProfileTools
import cn.itcast.tags.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 所有标签模型的基类，必须继承此类，实现doTag方法，如何计算标签
 * @param modelName
 * @param modelType
 */
abstract class AbstractTagModel(modelName: String, modelType: ModelType) extends Logging{

  // 变量声明
  var spark: SparkSession = _

  // 1. 初始化：构建SparkSession实例对象
  def init(): Unit = {
    spark = SparkUtils.createSparkSession(this.getClass)
  }

  // 2. 准备标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
  def getTagData(tagId: Long): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
      .option("url", ModelConfig.MYSQL_JDBC_URL)
      .option("dbtable", ModelConfig.tagTable(tagId))
      .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
      .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
      .load()
  }

  // 3. 业务数据：依据业务标签规则rule，从数据源获取业务数据
  def getBusinessData(tagDF: DataFrame): DataFrame = {
    import tagDF.sparkSession.implicits._

    // 3.1. 4级标签规则rule
    val tagRule: String = tagDF
      .filter($"level" === 4)
      .head().getAs[String]("rule")
    logInfo(s"==== 业务标签数据规则: {$tagRule} ====")

    // 3.2. 解析标签规则，先按照换行\n符分割，再按照等号=分割
    /*
      inType=hbase
      zkHosts=bigdata-cdh01.itcast.cn
      zkPort=2181
      hbaseTable=tbl_tag_users
      family=detail
      selectFieldNames=id,gender
     */
    val ruleMap: Map[String, String] = tagRule
      .split("\n")
      .map{ line =>
        val Array(attrName, attrValue) = line.trim.split("=")
        (attrName, attrValue)
      }
      .toMap

    // 3.3. 依据标签规则中inType类型获取数据
    var businessDF: DataFrame = null
    if("hbase".equals(ruleMap("inType").toLowerCase())){
      // 规则数据封装到HBaseMeta中
      val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      // 依据条件到HBase中获取业务数据
      businessDF = spark.read
        .option("zkHosts", hbaseMeta.zkHosts)
        .option("zkPort", hbaseMeta.zkPort.toString)
        .option("hbaseTable", hbaseMeta.hbaseTable)
        .option("family", hbaseMeta.family)
        .option("selectFields", hbaseMeta.selectFieldNames)
          .option("whereField",hbaseMeta.whereFieldNames)
        .hbase
    }else{
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    // 3.4. 返回数据
    businessDF
  }

  // 4. 构建标签：依据业务数据和属性标签数据建立标签
  def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

  // 5. 标签合并与保存：读取用户标签数据，进行合并操作，最后保存
  def mergeAndSaveTag(modelDF: DataFrame): Unit = {
    val spark = modelDF.sparkSession
    // 5.1. 从HBase表中读取户画像标签表数据: userId、tagIds
    val profileDF: DataFrame = ProfileTools.loadProfile(spark)
    // 5.2. 将用户标签数据合并
    val newProfileDF = ProfileTools.mergeProfileTags(modelDF, profileDF)
    // 5.3. 保存HBase表中
    ProfileTools.saveProfile(newProfileDF)
  }

  // 6. 关闭资源：应用结束，关闭会话实例对象
  def close(): Unit = {
    if(null != spark) spark.stop()
  }

  // 规定标签模型执行流程顺序
  def executeModel(tagId: Long): Unit ={
    // a. 初始化
    init()
    try{
      // b. 获取标签数据
      val tagDF: DataFrame = getTagData(tagId)
      //basicTagDF.show()
      tagDF.persist(StorageLevel.MEMORY_AND_DISK)
      tagDF.count()

      // c. 获取业务数据
      val businessDF: DataFrame = getBusinessData(tagDF)
      //businessDF.show()

      // d. 计算标签
      val modelDF: DataFrame = doTag(businessDF, tagDF)
      //modelDF.show()

      // e. 合并标签与保存
//      mergeAndSaveTag(modelDF)

//      Thread.sleep(10000000)

      tagDF.unpersist()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      close()
    }
  }

}
