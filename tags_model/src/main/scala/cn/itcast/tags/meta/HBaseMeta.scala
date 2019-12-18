package cn.itcast.tags.meta

/**
 * hbase元数据解析存储
 */

case class HBaseMeta(
                      inType: String,
                      zkHosts: String,
                      zkPort: String,
                      hbaseTable: String,
                      family: String,
                      selectFieldNames: String
                    )

object HBaseMeta {
  /**
   * 将Map集合数据解析到HBaseMeta中
   *
   * @param ruleMap map集合
   * @return
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      ruleMap("inType"), //
      ruleMap("zkHosts"), //
      ruleMap("zkPort"), //
      ruleMap("hbaseTable"), //
      ruleMap("family"), //
      ruleMap("selectFieldNames") //
    )
  }
}
