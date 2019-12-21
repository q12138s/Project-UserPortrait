package cn.itcast.tags.utils
import java.util
import java.util.Map

import cn.itcast.tags.models.ModelConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {
  /**
   * 加载Spark Application默认配置文件，设置到SparkConf中
   * @param resource 资源配置文件名称，默认值为：spark.properties
   * @return SparkConf对象
   */
  def loadConf(resource: String): SparkConf ={
    //创建SparkConf对象
    val sparkConf = new SparkConf()

    //判断是否是本地模式，如果是，设置local【4】
    if(ModelConfig.APP_IS_LOCAL){
      sparkConf.setMaster(ModelConfig.APP_SPARK_MASTER)
    }

    //加载应用配置
    val config: Config = ConfigFactory.load(resource)
    // 获取所有配置
    val entrySet: util.Set[util.Map.Entry[String, ConfigValue]] = config.entrySet()
    // 导入隐式转换，将Java集合转换为Scala集合，方便遍历
    import scala.collection.JavaConverters._
    entrySet.asScala.foreach{entry =>
      val entryValue: ConfigValue = entry.getValue
      if(resource.equals(entryValue.origin().resource())){
        val key = entry.getKey
        val value = entryValue.unwrapped().toString
        //println(s"key = $key, value = $value")
        // 设置属性
        sparkConf.set(key, value)
      }
    }
    // d. 返回对象
    sparkConf
  }

  /**
   * 构建SparkSession实例对象，如果是本地模式，设置master
   * @return
   */
  def createSparkSession(clazz: Class[_]): SparkSession = {
    // 1. 创建SparkConf对象
    val sparkConf: SparkConf = loadConf("spark.properties")

    // 2. 创建SparkBuilder对象
    var builder: SparkSession.Builder = SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .config(sparkConf)

    // 3. 判断是否与Hive集成
    if(ModelConfig.APP_IS_HIVE){
      builder = builder
        .config("hive.metastore.uris", ModelConfig.APP_HIVE_META_STORE_URL)
        .enableHiveSupport()
    }

    // 4. 获取SparkSession对
    builder.getOrCreate()
  }

}
