package cn.itcast.tags.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object sql{
  /**
   * Adds a method, `hbase`, to DataFrameReader that allows you to read HBase tables using
   * the DataFrameReader.
   */
  implicit class HBaseDataFrameReader(reader: DataFrameReader) {
    def hbase: DataFrame = reader.format("cn.itcast.tags.spark.sql").load
  }

  /**
   * Adds a method, `hbase`, to DataFrameWriter that allows writes to HBase using
   * the DataFileWriter
   */
  implicit class HBaseDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def hbase = writer.format("cn.itcast.tags.spark.sql").save
  }
}
