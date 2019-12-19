import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HBaseSQLTest {

  def main(args: Array[String]): Unit = {
    import cn.itcast.tags.spark.hbase._
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._

    /*
      zkHosts=bigdata-cdh01.itcast.cn
      zkPort=2181
      hbaseTable=tbl_tag_users
      family=detail
      selectFieldNames=id,gender
     */
    // 读取数据
    val df: DataFrame = spark.read
//      .format("cn.itcast.tags.spark.hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_users")
      .option("family", "detail")
      .option("selectFields", "id,gender")
//      .load()
        .hbase
    df.printSchema()
    df.show(100, truncate = false)

    // 保存数据
//    df.write
//      .mode(SaveMode.Overwrite)
//      .format("cn.itcast.tags.spark.hbase")
//      .option("zkHosts", "bigdata-cdh01.itcast.cn")
//      .option("zkPort", "2181")
//      .option("hbaseTable", "xxx_users")
//      .option("family", "info")
//      .option("rowKeyColumn", "id")
//      .save()


    spark.close()

  }

}
