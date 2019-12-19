package cn.itcast.tags.models.rule


import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 用户职业标签
 */
object JobModel extends Logging {

  def main(args: Array[String]): Unit = {
      /*
      321 职业
      322 学生 1
      323 公务员 2
      324 军人 3
      325 警察 4
      326 职业是教师 5
      327 白领 6
      */
    // TODO: 1. 创建SparkSession实例对象
    val spark: SparkSession = {
      // 1.a. 创建SparkConf 设置应用信息
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[4]")
        .set("spark.sql.shuffle.partitions", "4")
        // 由于从HBase表读写数据，设置序列化
        .set("spark.serializer",
          "org.apache.spark.serializer.KryoSerializer")
        // 设置与Hive集成: 读取Hive元数据MetaStore服务
        .set("hive.metastore.uris","thrift://bigdata-cdh01.itcast.cn:9083")
        // 设置数据仓库目录
        .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .registerKryoClasses(
          Array(classOf[ImmutableBytesWritable],
            classOf[Result], classOf[Put])
        )
      // 1.b. 建造者模式构建SparkSession对象
      val session = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
      // 1.c. 返回会话实例对象
      session
    }
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 1、依据TagId，从MySQL读取标签数据
    val tagTable: String =
      """
			  |(
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE id = 321
			  |UNION
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE pid = 321
			  |ORDER BY `level` ASC, `id` ASC
			  |) AS basic_tag
			  |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/profile_tags?" +
        "useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
//    |321|职业  |inType=hbase|5    |
//    |322|学生  |1  |5    |
//    basicTagDF.printSchema()
//    basicTagDF.show(50,false)

    // 2、解析业务标签rule，读取业务数据（从HBase表）
    // 2.1、获取业务标签规则rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .select($"rule")
      .head()
      .getAs[String]("rule")
    logWarning(s"========= { $tagRule } ========")

    // 2.2、将规则解析为Map集合
    val ruleMap: Map[String, String] = tagRule
      .split("\n")
      .map {
        line =>
          val Array(attrKey, attrValue): Array[String] = line.split("=")
          (attrKey, attrValue)
      }.toMap
    logWarning(s"========== { ${ruleMap.mkString(", ")} } ==========")

    // 2.3、依据标签规则中inType判断从哪个数据源获取业务数据
    var businessDF: DataFrame=null
    if ("hbase".equals(ruleMap("inType").toLowerCase)){
      // 2.3.1、封装HBase数据源中元数据信息HBaseMeta
        val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      // 2.3.2、从HBase表读取数据
      businessDF = HBaseTools.read(spark, hbaseMeta.zkHosts, hbaseMeta.zkPort, hbaseMeta.hbaseTable,
        hbaseMeta.family, hbaseMeta.selectFieldNames.split(","))
    }else{
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    /*
    root
 |-- id: string (nullable = true)
 |-- job: string (nullable = true)
       +---+---+
      |id |job|
      +---+---+
      |1  |3  |
      |10 |5  |
     */
//  businessDF.printSchema()
//    businessDF.show(50,false)
    // 3、业务数据结合属性标签数据，构建标签
    // 3.1、将属性标签数据存储至Map集合
    val attrTagMap: Map[String, Long] = basicTagDF
      .filter($"level" === 5)
      .select($"rule", $"id".as("tagId"))
      .as[(String, Long)]
      .rdd
      .collectAsMap()
      .toMap
    /*
    4 325
    5 326
    6 327
    1 322
    2 323
    3 324
     */
//    for((x,y) <- attrTagMap) println(s"$x $y")
    // 采用广播变量方式将数据广播到Executor中使用
    val attrTagMapBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(attrTagMap)
    // 3.2、自定义UDF函数，传递: job -> tagId
    val job_to_tag: UserDefinedFunction = udf(
      (job: String) => {
        attrTagMapBroadcast.value(job)
      }
    )
    // 3.3、使用UDF函数，进行数据打标签
    val modelDF: DataFrame = businessDF
      .select($"id".as("uid"), job_to_tag($"job").as("tagId"))
    /*
    root
 |-- uid: string (nullable = true)
 |-- tagId: long (nullable = true)
     +---+-----+
    |uid|tagId|
    +---+-----+
    |1  |324  |
    |10 |326  |
     */
//    modelDF.printSchema()
//    modelDF.show(50,false)

    // 4、合并画像标签数据，存储HBase表中
    // 4.1、从HBase表中获取画像表中用户标签数据
    val profileDF: DataFrame = HBaseTools.read(
      spark, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", Seq("userId", "tagIds")
    )
    /*
    root
 |-- userId: string (nullable = true)
 |-- tagIds: string (nullable = true)
     +------+------+
    |userId|tagIds|
    +------+------+
    |1     |320   |
    |10    |320   |
    |100   |320   |
     */
//    profileDF.printSchema()
//    profileDF.show(50,false)
    // 4.2、合并用户标签数据，关联操作
    val mergeDF: DataFrame = modelDF
      .join(profileDF, modelDF("uid") === profileDF("userId"), "left")
    /*
        +---+-----+------+------+
    |uid|tagId|userId|tagIds|
    +---+-----+------+------+
    |1  |324  |1     |320   |
    |102|322  |102   |320   |
    |107|322  |107   |319   |
     */
//    mergeDF.printSchema()
//    mergeDF.show(50,false)
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
    val newProfileDF: DataFrame = mergeDF.select(
      $"uid".as("userId"),
      when($"tagIds".isNull, $"tagId")
        .otherwise(merge_tag_udf($"tagId", $"tagIds"))
        .as("tagIds")
    )
    newProfileDF.printSchema()
    newProfileDF.show(50,false)
    // 4.3、保存最新用户标签数据至HBase表中
    HBaseTools.write(
      newProfileDF, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", "userId" //
    )


    spark.stop()
  }

}
