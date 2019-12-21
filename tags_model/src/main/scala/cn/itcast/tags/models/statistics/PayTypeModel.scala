package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.basic.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

class PayTypeModel extends BasicModel{
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    // 导入隐式转换和函数库
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    // 1. 订单数据中获取每个用户最多支付方式
    val paymentDF: DataFrame = businessDF
      .groupBy($"memberid", $"paymentcode")
      .count()
      // 获取每个会员支付方式最多，使用开窗排序函数ROW_NUMBER
      .withColumn(
        "rnk",
        row_number().over(
          Window.partitionBy($"memberid").orderBy($"count".desc)
        )
      )
      .where($"rnk".equalTo(1))
      .select(
        $"memberid".as("id"),
        $"paymentcode"
      )
//    paymentDF.show(20,false)

    // 2. 计算标签，规则匹配
    val modelDF: DataFrame = TagTools.ruleMatchTag(paymentDF,"paymentcode",tagDF)

//    modelDF.show(20,false)
    modelDF
  }
}
object PayTypeModel{
  def main(args: Array[String]): Unit = {
    val modelDF = new PayTypeModel
    modelDF.executeModel(356L)
  }
}
