package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, AbstractTagModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class JobTagModel extends AbstractTagModel("职业标签", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    // a. 使用业务字段：job与属性标签规则rule匹配，获取标签Id（tagId）
    val modelDF: DataFrame = TagTools.ruleMatchTag(businessDF, "job", tagDF)

    // b. 返回用户标签数据
    modelDF.show(50,false)
    modelDF
  }
}

object JobTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new JobTagModel()
    tagModel.executeModel(321L)
  }
}
