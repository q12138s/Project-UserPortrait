package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.basic.BasicModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

/**
 * 标签模型开发
 */
class AgeRangeModel extends BasicModel{
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    // 导入隐式转换
    import businessDF.sparkSession.implicits._
    // 导入函数库
    import org.apache.spark.sql.functions._
    // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
    val rule_to_tuple = udf(
      (rule:String)=>{
        val Array(start,end) = rule.split("-").map(_.toInt)
        (start,end)
      }
    )
    // 2. 获取属性标签数据，解析规则rule
    val attrTagDF: DataFrame = tagDF
      .filter($"level" === 5)
      .select(
        $"id".as("tagId"),
        rule_to_tuple($"rule").as("rules")
      )
      .select(
        $"tagId",
        $"rules._1".as("start"),
        $"rules._2".as("end")
      )
//    attrTagDF.printSchema()
//    attrTagDF.show(20,false)

    // 3. 业务数据与标签规则关联JOIN，比较范围
    val birthdayDF: DataFrame = businessDF
      .select(
        $"id".as("uid"),
        regexp_replace($"birthday", "-", "").cast(IntegerType)
          .as("bronDate")
      )
//    birthdayDF.show(20,false)
    // 3.2. 关联属性规则，设置条件
    val modelDF: DataFrame = birthdayDF
      .join(attrTagDF)
      .where(
        birthdayDF("bronDate").between(attrTagDF("start"), attrTagDF("end"))
      )
      .select(
        $"uid", $"tagId"
      )
//    modelDF.show(20,false)
    modelDF
  }
}

object AgeRangeModel{
  def main(args: Array[String]): Unit = {

    val modelDF = new AgeRangeModel
    modelDF.executeModel(338L)
  }
}
