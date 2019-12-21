package cn.itcast.tags.models.rule

import cn.itcast.tags.models.basic.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * 用户国籍标签开发
 */
class NationalityModel extends BasicModel {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
//    依据业务数据中业务字段的值与规则集合Map，使用UDF函数，进行打标签
    //TODO:调用TagTools
    val modelDF: DataFrame = TagTools.ruleMatchTag(businessDF, "nationality", tagDF)
    modelDF.printSchema()
    modelDF.show(50,false)
    modelDF
  }
}

object NationalityModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new NationalityModel
    tagModel.executeModel(332L)
  }
}
