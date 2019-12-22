package cn.itcast.tags.utils
import java.util.{Calendar, Date}
import org.apache.commons.lang3.time.FastDateFormat
/**
 * 日期工具类
 */
object DateUtils {

  // 格式：年－月－日 小时：分钟：秒
  val FORMAT_ONE: String = "yyyy-MM-dd HH:mm:ss"
  // 格式：年－月－日
  val LONG_DATE_FORMAT: String = "yyyy-MM-dd"
  /**
   * 获得当前日期字符串，格式"yyyy-MM-dd HH:mm:ss"
   * @return
   */
  def getNow: String = {
    // 获取当前Calendar对象
    val today: Calendar = Calendar.getInstance()
    // 返回字符串
    dateToString(today.getTime, FORMAT_ONE)
  }
  /**
   * 依据日期时间增加或减少多少天
   * @param dateStr 日期字符串
   * @param amount 天数，可以为正数，也可以为负数
   * @return
   */
  def dateSub(dateStr: String, amount: Int): String = {
    // a. 将日期字符串转换为Date类型
    val date: Date = stringToDate(dateStr, FORMAT_ONE)
    // b. 获取Calendar对象
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    // c. 设置天数（增加或减少）
    calendar.add(Calendar.DAY_OF_YEAR, amount)
    // d. 转换Date为字符串
    dateToString(calendar.getTime, FORMAT_ONE)
  }
  /**
   * 把日期转换为字符串
   * @param date 日期
   * @param format 格式
   * @return
   */
  def dateToString(date: Date, format: String): String = {
    // a. 构建FastDateFormat对象
    val formatter: FastDateFormat =
      FastDateFormat.getInstance(format)
    // b. 转换格式
    formatter.format(date)
  }
  /**
   * 把符合日期格式的字符串转换为日期类型
   * @param dateStr 日期格式字符串
   * @param format 格式
   * @return
   */
  def stringToDate(dateStr: String, format: String): Date = {
    // a. 构建FastDateFormat对象
    val formatter: FastDateFormat =
      FastDateFormat.getInstance(format)
    // b. 转换格式
    formatter.parse(dateStr)
  }

  def main(args: Array[String]): Unit = {

    val nowDate: String = getNow
    println(nowDate)
    // 一周前日期
    val weekDate = dateSub(nowDate, -7)
    println(weekDate)
    // 一周后日期
    val weekAfterDate = dateSub(nowDate, 7)
    println(weekAfterDate)

  }
}
