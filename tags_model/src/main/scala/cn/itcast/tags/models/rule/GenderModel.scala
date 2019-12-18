package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 用户性别标签
 */
object GenderModel extends Logging {

  def main(args: Array[String]): Unit = {

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

    //TODO:2.从Mysql数据库读取标签数据（基础标签表：tbl_basic_tag），依据业务标签ID读取
    val tagTable: String =
      """
			  |(
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE id = 318
			  |UNION
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE pid = 318
			  |ORDER BY `level` ASC, `id` ASC
			  |) AS basic_tag
			  |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url",
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
          "useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    /*
root
 |-- id: long (nullable = false)
 |-- name: string (nullable = true)
 |-- rule: string (nullable = true)
 |-- level: integer (nullable = true)
 */
    //    basicTagDF.printSchema()
    //    basicTagDF.show(false)
    basicTagDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      .count() //表示触发缓存


    //TODO:3.依据业务标签规则获取业务数据，比如到Hbase数据库读取表的数据
    //获取业务标签规则rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .select($"rule") // 获取规则rule
      .head()
      .getAs[String]("rule")
    logWarning(s"===============$tagRule=============")

    //将规则解析为Map集合
    val ruleMap: Map[String, String] = tagRule
      .split("\n")
      // 再次按照等号分割，转换为二元组
      .map { line =>
        val Array(attrKey, attrValue) = line.split("=")
        (attrKey, attrValue)
      }
      .toMap // 将数组转换为Map集合（由于数组中数据类型为二元组）
    logWarning(s"=============={${ruleMap.mkString(", ")}}================")

    //依据标签规则中inType判断从哪个数据源获取业务数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase)) {
      //封装hbase数据源中元数据信息HbaseMeta
      val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      //从hbase表中读取数据
      businessDF = HBaseTools.read(spark, hbaseMeta.zkHosts, hbaseMeta.zkPort, hbaseMeta.hbaseTable,
        hbaseMeta.family, hbaseMeta.selectFieldNames.split(","))
    } else {
      //如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    /*
			root
			 |-- id: string (nullable = true)
			 |-- gender: string (nullable = true)
 */
    //    businessDF.printSchema()
    //    businessDF.show(20,false)

    //TODO:4.业务数据和属性标签结合，构建标签：规则匹配型标签-》RULE，MATCH
    //获取属性标签的数据
    val attrTagDF: DataFrame = basicTagDF
      // 依据标签级别level：5，获取属性标签数据
      .filter($"level" === 5)
      .select($"id".as("tagId"), $"rule")
    /*
  root
   |-- tagId: long (nullable = false)
   |-- rule: string (nullable = true)
 */
    //    attrTagDF.printSchema()
    //    attrTagDF.show(false)
    //依据字段关联业务数据和属性标签数据
    val modelDF: DataFrame = businessDF
      .join(attrTagDF, businessDF("gender") === attrTagDF("rule"), "left")
      .select(
        $"id".as("uid"), $"tagId".cast(StringType)
      )
    //    modelDF.show(false)
    basicTagDF.unpersist()
    //TODO:5.将标签数据存储到HBase表中：用户画像标签表-》tbl_profile
    //从hbase表中获取画像表中用户标签数据
    val profileDF: DataFrame = HBaseTools.read(
      spark, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", Seq("userId", "tagIds")
    )
    /*
    root
     |-- userId: string (nullable = true)
     |-- tagIds: string (nullable = true)
     */
    //    profileDF.printSchema()
    //    profileDF.show()
    //合并用户标签数据，关联操作
    val mergeDF: DataFrame = modelDF
      .join(profileDF, modelDF("uid") === profileDF("userId"), "left")
    /*
        root
     |-- uid: string (nullable = true)
     |-- tagId: string (nullable = true)
     |-- userId: string (nullable = true)
     |-- tagIds: string (nullable = true)
     */
    /*
  +------+-----+------+------+
  |uid|tagId|userId|tagIds|
  +------+-----+------+------+
  |1     |320  |null  |100,320  |  =>  100,320
  |102   |320  |null  |null  |
  |107   |319  |null  |null  |
 */
    //    mergeDF.printSchema()
    //    mergeDF.show()
    //自定义UDF函数，真正合并标签, 判断tagIds是否有值，有值就合并；没值直接返回tagId
    val merge_tag: UserDefinedFunction = udf(
      (tagId: String, tagIds: String) => {
        tagIds
          .split(",") // 按照逗号分割
          .:+(tagId) // 添加新的标签
          .distinct
          .mkString(",")
      }
    )
    val newProfileDF: DataFrame = mergeDF.select(
      $"uid".as("userId"),
      when($"tagIds".isNull, $"tagId")
        .otherwise(merge_tag($"tagId", $"tagIds"))
        .as("tagIds")
    )
    newProfileDF.printSchema()
    newProfileDF.show(50, false)
    //保存最新用户标签数据至hbase
    HBaseTools.write(
      newProfileDF, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", "userId" //
    )

    //应用结束，关闭资源
    spark.stop()
  }

}

