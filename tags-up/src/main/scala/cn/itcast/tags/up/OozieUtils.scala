package cn.itcast.tags.up

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.oozie.client.OozieClient

/**
 * Oozie Java Client 提交Workflow时封装参数CaseClass
 *
 * @param modelId 模型ID
 * @param mainClass 运行主类
 * @param jarPath JAR包路径
 * @param sparkOptions Spark Application运行参数
 * @param start 运行开始时间
 * @param end 运行结束时间
 */
case class OozieParam(
	                     modelId: Long,
	                     mainClass: String,
	                     jarPath: String,
	                     sparkOptions: String,
	                     start: String,
	                     end: String
                     )


object OozieUtils {
	val classLoader: ClassLoader = getClass.getClassLoader

	/**
	 * Properties 包含各种配置
	 * OozieParam 外部传进来的参数
	 * 作用: 生成配置, 有些配置无法写死, 所以外部传入
	 */
	def genProperties(param: OozieParam): Properties = {
		val props = new Properties()

		val params: Map[String, String] = ConfigHolder.oozie.params
		for (entry <- params) {
			props.setProperty(entry._1, entry._2)
		}

		val appPath = ConfigHolder.hadoop.nameNode + genAppPath(param.modelId)
		props.setProperty("appPath", appPath)

		props.setProperty("mainClass", param.mainClass)
		props.setProperty("jarPath", param.jarPath) // 要处理

		if (StringUtils.isNotBlank(param.sparkOptions)) props.setProperty("sparkOptions", param.sparkOptions)
		props.setProperty("start", param.start)
		props.setProperty("end", param.end)
		props.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath)

		props
	}

	/**
	 * 上传配置
	 *
	 * @param modelId 因为要上传到 家目录, 所以要传入 id 生成家目录
	 */
	def uploadConfig(modelId: Long): Unit = {
		val workflowFile = classLoader.getResource("oozie/workflow.xml").getPath
		val coordinatorFile = classLoader.getResource("oozie/coordinator.xml").getPath

		val path = genAppPath(modelId)
		HDFSUtils.getInstance().mkdir(path)
		HDFSUtils.getInstance().copyFromFile(workflowFile, path + "/workflow.xml")
		HDFSUtils.getInstance().copyFromFile(coordinatorFile, path + "/coordinator.xml")
	}

	def genAppPath(modelId: Long): String = {
		ConfigHolder.model.path.modelBase + "/tag_" + modelId
	}

	def store(modelId: Long, prop: Properties): Unit = {
		val appPath = genAppPath(modelId)
		prop.store(HDFSUtils.getInstance().createFile(appPath + "/job.properties"), "")
	}

	def start(prop: Properties): Unit = {
		// 构建OozieClient客户端
		val oozie = new OozieClient(ConfigHolder.oozie.url)
		println(prop)

		// 运行WorkFlow
		val jobId = oozie.run(prop)
		println(jobId)
	}

}

