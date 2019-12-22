import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object HBaseSQLFilterTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 读取HBase表数据，不设置条件
    import cn.itcast.tags.spark.sql._
    val ordersDF: DataFrame = spark.read
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "id,memberid,orderamount")
      .hbase
    //ordersDF.printSchema()
    //ordersDF.show(50, truncate = false)
    println(s"count = ${ordersDF.count()}")
    // 读取HBase表数据，设置条件: 2019-09-01
    import cn.itcast.tags.spark.sql._
    val dataframe: DataFrame = spark.read
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "id,memberid,orderamount")
      .option("whereFields", "modified[lt]2019-09-01")
      .hbase
    dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    dataframe.show(50, truncate = false)
    println(s"count = ${dataframe.count()}")
    spark.stop()
  }

}
