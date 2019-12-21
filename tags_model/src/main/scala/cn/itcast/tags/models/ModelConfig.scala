package cn.itcast.tags.models

import com.typesafe.config.{Config, ConfigFactory}

/**
 * 读取配置文件信息config.properties，获取属性值
 */
object ModelConfig {

  // 读取属性配置文件
  val config: Config = ConfigFactory.load("config.properties")

  // Spark Application Local Mode
  val APP_IS_LOCAL: Boolean = config.getBoolean("app.is.local")
  val APP_SPARK_MASTER: String = config.getString("app.spark.master")

  // Spark Application With Hive
  val APP_IS_HIVE: Boolean = config.getBoolean("app.is.hive")
  val APP_HIVE_META_STORE_URL: String = config.getString("app.hive.metastore.uris")

  // Model Config
  val MODEL_BASE_PATH: String  = config.getString("tag.model.base.path")

  // MySQL Config
  val MYSQL_JDBC_DRIVER: String  = config.getString("mysql.jdbc.driver")
  val MYSQL_JDBC_URL: String  = config.getString("mysql.jdbc.url")
  val MYSQL_JDBC_USERNAME: String  = config.getString("mysql.jdbc.username")
  val MYSQL_JDBC_PASSWORD: String  = config.getString("mysql.jdbc.password")

  // Basic Tag table config
  def tagTable(tagId: Long): String = {
    s"""
		   |(
		   |SELECT `id`,
		   |       `name`,
		   |       `rule`,
		   |       `level`
		   |FROM `profile_tags`.`tbl_basic_tag`
		   |WHERE id = $tagId
		   |UNION
		   |SELECT `id`,
		   |       `name`,
		   |       `rule`,
		   |       `level`
		   |FROM `profile_tags`.`tbl_basic_tag`
		   |WHERE pid = $tagId
		   |ORDER BY `level` ASC, `id` ASC
		   |) AS basic_tag
		   |""".stripMargin
  }

  // Profile table Config
  val PROFILE_TABLE_ZK_HOSTS: String  = config.getString("profile.hbase.zk.hosts")
  val PROFILE_TABLE_ZK_PORT: String  = config.getString("profile.hbase.zk.port")
  val PROFILE_TABLE_ZK_ZNODE: String  = config.getString("profile.hbase.zk.znode")
  val PROFILE_TABLE_NAME: String  = config.getString("profile.hbase.table.name")
  val PROFILE_TABLE_FAMILY_USER: String  = config.getString("profile.hbase.table.family.user")
  val PROFILE_TABLE_FAMILY_ITEM: String  = config.getString("profile.hbase.table.family.item")
  val PROFILE_TABLE_COMMON_COL: String  = config.getString("profile.hbase.table.family.common.col")
  // 作为RowKey列名称
  val PROFILE_TABLE_ROWKEY_COL: String = config.getString("profile.hbase.table.rowkey.col")
  val PROFILE_TABLE_SELECT_FIELDS: String = config.getString("profile.hbase.table.select.fields")


  // HDFS Config
  val DEFAULT_FS: String  = config.getString("fs.defaultFS")
  val FS_USER: String  = config.getString("fs.user")

  // Solr Config
  val SOLR_CLOUD_MODEL: String  = config.getString("solr.cloud.model")
  val SOLR_ADDR: String = config.getString("solr.addr")
  val SOLR_PROFILE_FIELDS: String  = config.getString("solr.profile.fields")
  val SOLR_PROFILE_FAMILY: String  = config.getString("solr.profile.family")

}
