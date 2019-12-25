package cn.itcast.tags.meta

import cn.itcast.tags.utils.DateUtils

/**
 * hbase元数据解析存储
 * * inType=hbase
 * * zkHosts=bigdata-cdh01.itcast.cn
 * * zkPort=2181
 * * hbaseTable=tbl_tag_users
 * * family=detail
 * * selectFieldNames=id,gender
 * * whereFieldNames=modified#day#30
 */

case class HBaseMeta(
                      inType: String,
                      zkHosts: String,
                      zkPort: String,
                      hbaseTable: String,
                      family: String,
                      selectFieldNames: String,
                      whereFieldNames : String
                    )

object HBaseMeta {
  /**
   * 将Map集合数据解析到HBaseMeta中
   *
   * @param ruleMap map集合
   * @return
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {

    //从规则中获取where字段值：modified#day#30
    val whereFieldNames: String = ruleMap.getOrElse("whereFieldNames",null)

    //解析条件构建Condition
    val whereField : String = if (null != whereFieldNames) {
      /*
				modified#day#30
					|
				modified[le]2019-12-24,modified[ge]2019-11-25
			 */
      //字符串分割
      val Array(field,unit,amount): Array[String] = whereFieldNames.split("#")
      //获取今天时间日期和昨天
      val nowDate: String = DateUtils.getNow
      val yesterdayDate: String = DateUtils.dateSub(nowDate,-1)
      //依据单位unit，获取最早的时间日期
      val days: Int = unit match {
        case "day" => - amount.toInt
        case "month" => - (30 * amount.toInt)
        case "year" => - (365 * amount.toInt)
      }
      val ageDate: String = DateUtils.dateSub(nowDate,days)
      //拼接字符串，组合条件
      s"$field[le]$yesterdayDate,$field[ge]$ageDate"
    }else null


      HBaseMeta(
      ruleMap("inType"), //
      ruleMap("zkHosts"), //
      ruleMap("zkPort"), //
      ruleMap("hbaseTable"), //
      ruleMap("family"), //
      ruleMap("selectFieldNames") ,
        whereField
    )
  }
}
