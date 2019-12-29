package cn.itcast.tags.meta

import org.apache.spark.sql.Column

/**
 * HDFS 元数据解析存储，具体数据字段格式如下所示：
 * inType=hdfs
 * inPath=/apps/datas/tbl_tag_logs.tsv
 * sperator=\t
 * selectFieldNames=global_user_id,loc_url,log_time
 */
case class HdfsMeta(
                     inPath: String,
                     sperator: String,
                     selectFieldNames: Array[Column]
                   )

object HdfsMeta{

  /**
   * 将Map集合数据解析到HdfsMeta中
   * @param ruleMap map集合
   * @return
   */
  def getHdfsMeta(ruleMap: Map[String, String]): HdfsMeta = {
    // 将选取的字段转换为Column对象
    import org.apache.spark.sql.functions.col
    val columns: Array[Column] = ruleMap("selectFieldNames")
      // 按照分隔符分割
      .split(",")
      // 将字符串转换为Column对象
      .map{field => col(field)}

    // 构建HdfsMeta对象
    HdfsMeta(
      ruleMap("inPath"),
      ruleMap("sperator"),
      columns
    )
  }

}