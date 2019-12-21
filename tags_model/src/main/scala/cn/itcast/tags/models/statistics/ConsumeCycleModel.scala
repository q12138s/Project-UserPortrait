package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.basic.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class ConsumeCycleModel extends BasicModel{
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    // 导入隐式转换
    import businessDF.sparkSession.implicits._
    // 导入函数库
    import org.apache.spark.sql.functions._
    // 1. 获取属性标签数据，解析规则rule
    val attrTagDF: DataFrame = TagTools.convertTuple(tagDF)
//    attrTagDF.show(10,false)

    // 2. 订单数据按照会员ID：memberid分组，获取最近一次订单完成时间：finishtime
    val daysDF: DataFrame = businessDF
      // 2.1. 分组，获取最新订单时间，并转换格式
      .groupBy($"memberid")
      .agg(
        from_unixtime(max($"finishtime")).as("finish_time")
      )
      // 2.2. 计算用户最新订单距今天数
      .select(
        $"memberid".as("uid"),
        datediff(current_timestamp(), $"finish_time").as("consumer_days")
      )
    // 3. 关联属性标签数据和消费天数数据，加上判断条件，进行打标签
    val modelDF: DataFrame = daysDF
      .join(attrTagDF)
      .where(
        daysDF("consumer_days").between(attrTagDF("start"), attrTagDF("end"))
      )
      .select(
        $"uid", $"tagId"
      )
//    modelDF.show(20,false)

    modelDF
  }
}
object ConsumeCycleModel{
  def main(args: Array[String]): Unit = {

    val modelDF = new ConsumeCycleModel
    modelDF.executeModel(347L)

  }
}
