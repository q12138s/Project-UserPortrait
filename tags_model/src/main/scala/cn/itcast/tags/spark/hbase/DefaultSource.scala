package cn.itcast.tags.spark.hbase

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class DefaultSource extends RelationProvider with CreatableRelationProvider
  with DataSourceRegister with Serializable {

  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"

  // 调用API时，简短名称
  override def shortName(): String = "hbase"
  /**
   * 提供BaseRelation对象，用于读取HBase表的数据
   * @param sqlContext SQLContext实例对象
   * @param parameters 参数信息
   * @return
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // 1. 定义读取数据的Schema信息
    val schema: StructType = StructType(
      parameters(HBASE_TABLE_SELECT_FIELDS)
        .split(SPERATOR)
        .map{field =>
          StructField(field, StringType, nullable = true)
        }
    )
    // 2. 创建HBaseRelation对象
    val relation = new HBaseRelation(sqlContext, schema, parameters)

    // 3. 返回对象
    relation
  }
  /**
   * 提供BaseRelation对象，用于将数据保存到HBase表中
   * @param sqlContext SQLContext实例对象
   * @param mode 保存模式
   * @param parameters 参数信息
   * @param data 数据集
   * @return
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    // 1. 创建HBaseRelation对象
    val relation = new HBaseRelation(sqlContext, data.schema, parameters)

    // 2. 插入数据
    relation.insert(data, overwrite = true)

    // 3. 返回对象
    relation
  }

}
