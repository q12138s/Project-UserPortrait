package cn.itcast.tags.up

import com.typesafe.config.ConfigFactory
import pureconfig.ConfigSource

case class Config
(
	model: Model,
	hadoop: Hadoop,
	mysql: MySQL,
	oozie: Oozie
)

case class Model
(
	user: String,
	app: String,
	path: Path
)

case class Path
(
	jars: String,
	modelBase: String
)

case class Hadoop
(
	nameNode: String,
	resourceManager: String
)

case class MySQL
(
	url: String,
	driver: String,
	tagTable: String,
	modelTable: String
)

case class Oozie
(
	url: String,
	params: Map[String, String]
)

object ConfigHolder {

	import pureconfig._
	import pureconfig.generic.auto._

	private val configTool = ConfigFactory.load("up")
	val config: Config = ConfigSource.fromConfig(configTool).load[Config].right.get
	val model: Model = config.model
	val hadoop: Hadoop = config.hadoop
	val oozie: Oozie = config.oozie
	val mysql: MySQL = config.mysql
}
