package cn.itcast.tags.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 工具类，Hbase读写功能
 */
object HBaseTools extends Logging {
  /**
   * 依据指定表名称、列簇及列名称从HBase表中读取数据
   *
   * @spark SparkSession 实例对象
   * @param zks    Zookerper 集群地址
   * @param port   Zookeeper端口号
   * @param table  HBase表的名称
   * @param family 列簇名称
   * @param fields 列名称
   * @return
   */
  def read(spark: SparkSession, zks: String, port: String,
           table: String, family: String, fields: Seq[String]): DataFrame = {

    //1.设置hbase依赖zk相关配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zks)
    conf.set("hbase.zookeeper.property.clientPort", port)
    //2.设置表的名称
    conf.set(TableInputFormat.INPUT_TABLE, table)
    //3.设置读取列簇和列
    val scan = new Scan()
    //设置列簇
    val cfBytes: Array[Byte] = Bytes.toBytes(family)
    scan.addFamily(cfBytes)
    //设置列
    fields.foreach(field => {
      scan.addColumn(cfBytes, Bytes.toBytes(field))
    })
    //TODO:设置scan对象，需要将scan对象转换为字符串
    conf.set(
      TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )
    //4.从hbase表中读取数据
    /*
          conf: Configuration = hadoopConfiguration,
          fClass: Class[F],
          kClass: Class[K],
          vClass: Class[V]
     */
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    logWarning(s"从HBase表[$table]读取数据条目数：${datasRDD.count()} ...................")
    //5.转换RDD为DataFrame
    val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
      // 依据获取字段名称获取对应的值，放在Seq中
      val values: Seq[String] = fields.map { filed =>
        //依据列名获取值
        Bytes.toString(result.getValue(cfBytes, Bytes.toBytes(filed)))
      }
      Row.fromSeq(values)
    }
    //6.自定义schema
    var rowSchema: StructType = new StructType()
    fields.foreach { field =>
      rowSchema = rowSchema.add(field, StringType, nullable = true)
    }
    //7.构建DataFra并返回
    spark.createDataFrame(rowsRDD, rowSchema)

  }



  /**
   * 将DataFrame数据保存到HBase表中
   *
   * @param dataframe 数据集DataFrame
   * @param zks Zk地址
   * @param port 端口号
   * @param table 表的名称
   * @param family 列簇名称
   * @param rowKeyColumn RowKey字段名称
   */
  def write(dataframe: DataFrame, zks: String, port: String,
            table: String, family: String, rowKeyColumn: String): Unit = {

    // 1. 设置HBase依赖Zookeeper相关配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zks)
    conf.set("hbase.zookeeper.property.clientPort", port)
    // 2. 数据写入表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    // 3. 数据转换为RDD
    val columnNames: Array[String] = dataframe.columns// 获取DataFrame中所有列名称
    val cfBytes: Array[Byte] = Bytes.toBytes(family)
    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = dataframe.rdd.map {
      row =>
        // a. 获取RowKey的值
        val rowKeyValue: String = row.getAs[String](rowKeyColumn)
        // b. 构建Put对象
        val put = new Put(Bytes.toBytes(rowKeyValue))
        // c. 添加列: 依据列的名称获取值，添加在put中
        columnNames.foreach {
          columnName =>
            val columnValue: String = row.getAs[String](columnName)
            put.addColumn(cfBytes, Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
        }
        (new ImmutableBytesWritable(put.getRow), put)
    }
    // 4. 调用TableOutputFormat保存数据到HBase表中
    datasRDD
      .saveAsNewAPIHadoopFile(
        s"datas/hbase/output-${table}-${System.currentTimeMillis()}",
        classOf[ImmutableBytesWritable], //
        classOf[Put], //
        classOf[TableOutputFormat[ImmutableBytesWritable]], //
        conf
      )
  }


}
