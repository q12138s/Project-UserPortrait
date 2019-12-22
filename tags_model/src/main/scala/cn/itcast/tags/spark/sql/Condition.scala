package cn.itcast.tags.spark.sql

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp

import scala.util.matching.Regex

object Condition {

//正则表达式
  val FULL_REGEX:Regex = "(.*?)\\[(.*?)\\](.*+)".r

    def parseCondition(whereField : String):Condition={
      //使用正则表达式，或者分割字符串
        val optionMatch: Option[Regex.Match] = FULL_REGEX.findFirstMatchIn(whereField)
      //获取匹配regex.Match对象
      val matchValue: Regex.Match = optionMatch.get
      //获取比较操作符，转换为CompareFilter对象
      val compare: CompareOp = matchValue.group(2).toLowerCase match {
        case "eq" => CompareOp.EQUAL
        case "ne" => CompareOp.NOT_EQUAL
        case "gt" => CompareOp.GREATER
        case "lt" => CompareOp.LESS
        case "ge" => CompareOp.GREATER_OR_EQUAL
        case "le" => CompareOp.LESS_OR_EQUAL
      }

      Condition(
        matchValue.group(1),
        compare,
        matchValue.group(3)
      )
    }

}

/**
 * 封装where cause 语句至Condition对象中
 * @param field
 * @param compare
 * @param value
 */
case class Condition(
                    field:String,
                    compare:CompareOp,
                    value:String
                    )
