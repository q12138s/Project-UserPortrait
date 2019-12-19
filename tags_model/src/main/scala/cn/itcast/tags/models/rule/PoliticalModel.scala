package cn.itcast.tags.models.rule

import cn.itcast.tags.models.basic.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：政治面貌标签模型
 */
class PoliticalModel extends BasicModel{

  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
      val modelDF: DataFrame = TagTools.ruleMatchTag(businessDF,"politicalface",tagDF)
    modelDF.printSchema()
    modelDF.show(50,false)
    modelDF
  }
}

object PoliticalModel{
  def main(args: Array[String]): Unit = {

    val tagModel = new PoliticalModel
    tagModel.executeModel(328L)
  }
}
