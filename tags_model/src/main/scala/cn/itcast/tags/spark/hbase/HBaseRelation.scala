package cn.itcast.tags.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType

/**
 * 实现SparkSQL提供外部数据源接口，实现加载和保存数据到HBase表中
 */
class HBaseRelation(context: SQLContext, userSchema: StructType, params: Map[String, String]) extends BaseRelation with TableScan
  with InsertableRelation with Serializable {

  // 连接HBase数据库的属性名称
  val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
  val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
  val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
  val HBASE_ZK_PORT_VALUE: String = "zkPort"
  val HBASE_TABLE: String = "hbaseTable"
  val HBASE_TABLE_FAMILY: String = "family"
  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

  override def sqlContext: SQLContext = context

  override def schema: StructType = userSchema

  override def buildScan(): RDD[Row] = {
    // 1. 读取配置信息，加载HBaseClient配置（主要ZK地址和端口号）
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
    // 2. 设置表的名称
    conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))
    // 3. 设置读取列簇和列
    val scan: Scan = new Scan() // 获取扫描器对象Scan
    val cfBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    // 设置列簇
    scan.addFamily(cfBytes)
    // 设置列
    val fields = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
    fields.foreach { field =>
      scan.addColumn(cfBytes, Bytes.toBytes(field))
    }
    // 设置Scan对象：需要将Scan对象转换为字符串
    conf.set(
      TableInputFormat.SCAN, //
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) //
    )
    // 4. 从HBase表读取数据
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext
      .newAPIHadoopRDD(
        conf, //
        classOf[TableInputFormat], //
        classOf[ImmutableBytesWritable], //
        classOf[Result]
      )
    // 5. 转换RDD[Row]
    val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
      // 依据获取字段名称获取对应的值，放在Seq中
      val values: Seq[String] = fields.map { field =>
        // 如何依据列名称获取值
        Bytes.toString(result.getValue(cfBytes, Bytes.toBytes(field)))
      }
      // 返回Row对象
      Row.fromSeq(values)
    }
    // 6. 返回读取的数据
    rowsRDD
  }

  /**
   * 将数据集DataFrame进行保存，设置是否覆盖
   *
   * @param data      数据集
   * @param overwrite 是否覆写
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // 1. 设置HBase依赖Zookeeper相关配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
    // 2. 数据写入表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE))
    // 3. 转换数据为RDD[(ImmutableBytesWritable, Put)]
    val columnNames: Array[String] = data.columns // 获取DataFrame中所有列名称
    val cfBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map { row =>
      // a. 获取RowKey的值
      val rowKeyValue = row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME))
      // b. 构建Put对象
      val put = new Put(Bytes.toBytes(rowKeyValue))
      // c. 添加列: 依据列的名称获取值，添加在put中
      columnNames.foreach { columnName =>
        val columnValue = row.getAs[String](columnName)
        put.addColumn(
          cfBytes, Bytes.toBytes(columnName), Bytes.toBytes(columnValue)
        )
      }
      // d. 返回二元组
      (new ImmutableBytesWritable(put.getRow), put)
    }
    // 4. 调用TableOutputFormat保存数据到HBase表中
    datasRDD.saveAsNewAPIHadoopFile(
      s"datas/hbase/output-${params(HBASE_TABLE)}-${System.currentTimeMillis()}", //
      classOf[ImmutableBytesWritable], //
      classOf[Put], //
      classOf[TableOutputFormat[ImmutableBytesWritable]], //
      conf
    )
  }
}
