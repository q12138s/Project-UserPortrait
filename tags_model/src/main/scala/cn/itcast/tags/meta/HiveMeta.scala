package cn.itcast.tags.meta

import org.apache.spark.sql.Column

/**
 *Hive 元数据解析存储，具体数据字段格式如下所示：
 *inType=hive
 *hiveTable=tags_dat.tbl_logs
 *selectFieldNames=global_user_id,loc_url,log_time
 *## 分区字段及数据范围
 *whereFieldNames=log_time#day#30
 */
case class HiveMeta(
                     hiveTable: String,
                     selectFieldNames: Array[Column],
                     whereFieldNames: String
                   )

object HiveMeta{

  /**
   * 将Map集合数据解析到HiveMeta中
   * @param ruleMap map集合
   * @return
   */
  def getHiveMeta(ruleMap: Map[String, String]): HiveMeta = {

    // 将选取的字段转换为Column对象
    import org.apache.spark.sql.functions.col
    val columns: Array[Column] = ruleMap("selectFieldNames")
      // 按照分隔符分割
      .split(",")
      // 将字符串转换为Column对象
      .map{field => col(field)}

    // 创建HiveMeta对象
    HiveMeta(
      ruleMap("hiveTable"),
      columns,
      null
    )

  }

}