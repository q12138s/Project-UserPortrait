package cn.itcast.tags.models.basic

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.{HBaseTools, ProfileTools}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 标签基类，各个标签模型继承此类，实现其中打标签方法doTag
 */
trait BasicModel extends Logging {

  //变量声明
  var spark: SparkSession = _

  //1.初始化：构建SparkSession实例对象
  def init(): Unit = {
    // a. 创建SparkConf,设置应用相关配置
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      // 设置序列化为：Kryo
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put]))
      // 设置Shuffle分区数目
      .set("spark.sql.shuffle.partitions", "4")
    // b. 建造者模式创建SparkSession会话实例对象
    spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
  }

  //2.准备标签数据：依据标签ID从Mysql数据库表tbl_basic_tag获取标签数据
  def getTagData(tagId: Long): DataFrame = {
    // 依据tagId查询标签数据SQL
    val tagTable: String =
      s"""
			  |(
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE id = $tagId
			  |UNION
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE pid = $tagId
			  |ORDER BY `level` ASC, `id` ASC
			  |) AS basic_tag
			  |""".stripMargin
    // 获取标签数据
    val tagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/profile_tags?" +
        "useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    // 返回标签数据
    tagDF
  }

  //3.业务数据：依据业务标签规则rule，从数据源获取业务数据
  def getBusinessData(tagDF: DataFrame): DataFrame = {
    import tagDF.sparkSession.implicits._

    // 1、获取业务标签规则rule
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
      .map { line =>
        val Array(attrKey, attrValue) = line.split("=")
        (attrKey, attrValue)
      }
      .toMap // 将数组转换为Map集合（由于数组中数据类型为二元组）
    logWarning(s"========== { ${ruleMap.mkString(", ")} } ==========")

    // 3、依据标签规则中inType判断从哪个数据源获取业务数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase)) {
      // 2.3.1、封装HBase数据源中元数据信息HBaseMeta
      val hbaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      // 2.3.2、从HBase表读取数据
      businessDF = HBaseTools.read(
        spark, hbaseMeta.zkHosts, hbaseMeta.zkPort, //
        hbaseMeta.hbaseTable, hbaseMeta.family, //
        hbaseMeta.selectFieldNames.split(",")
      )
    } else {
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }

    // 4、返回业务数据
    businessDF
  }

  // 4. 构建标签：依据业务数据和属性标签数据建立标签
  def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

  // 5. 标签合并与保存：读取用户标签数据，进行合并操作，最后保存
  def mergeAndSaveTag(modelDF: DataFrame): Unit = {
    import modelDF.sparkSession.implicits._
    //TODO：调用profileTools
    //加载数据
    val profileDF: DataFrame = ProfileTools.loadProfile(spark)
    //合并数据
    val newProfileDF: DataFrame = ProfileTools.mergeProfileTags(modelDF, profileDF)
    //输出数据
    ProfileTools.saveProfile(newProfileDF)
  }

  // 6. 关闭资源：应用结束，关闭会话实例对象
  def close(): Unit = {
    if (null != spark) spark.stop()
  }

  // 规定标签模型执行流程顺序
  def executeModel(tagId: Long): Unit = {
    // a. 初始化
    init()
    try {
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
      mergeAndSaveTag(modelDF)
      tagDF.unpersist()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close()
    }
  }

}