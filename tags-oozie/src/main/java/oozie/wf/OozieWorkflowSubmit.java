package oozie.wf;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

/**
 * 调用Oozie Java API提交执行WorkFlow工作流
 */
public class OozieWorkflowSubmit {

	public static void main(String[] args) throws OozieClientException, InterruptedException {

		// 1. 构建OozieClient实例对象
		OozieClient oozieClient = new OozieClient("http://bigdata-cdh01.itcast.cn:11000/oozie/");

		// 2. 构建Properties实例对象，设置属性值
		Properties jobConf = oozieClient.createConfiguration();

		// TODO： 动态加载属性配置文件的数据 -> job.properties，设置值
		// 2.1. 系统参数设置
		jobConf.setProperty("oozie.use.system.libpath", "true") ;
		jobConf.setProperty("user.name", "root") ;
		jobConf.setProperty("oozie.libpath",
			"hdfs://bigdata-cdh01.itcast.cn:8020/user/root/share/lib/lib_20190723215106/spark2") ;
		// 2.2. 必要参数信息
		jobConf.setProperty("nameNode", "hdfs://bigdata-cdh01.itcast.cn:8020") ;
		jobConf.setProperty("jobTracker", "bigdata-cdh01.itcast.cn:8032") ;
		jobConf.setProperty("queueName", "default") ;
		// 2.3. 应用提交运行yarn参数
		jobConf.setProperty("master", "yarn") ;
		jobConf.setProperty("mode", "cluster") ;
		jobConf.setProperty("sparkOptions",
			" --driver-memory 512m " +
				"--executor-memory 512m " +
				"--num-executors 1 " +
				"--executor-cores 1 " +
				"--conf spark.yarn.historyServer.address=http://bigdata-cdh01.itcast.cn:18080 " +
				"--conf spark.eventLog.enabled=true " +
				"--conf spark.eventLog.dir=hdfs://bigdata-cdh01.itcast.cn:8020/spark/eventLogs " +
				"--conf spark.yarn.jars=hdfs://bigdata-cdh01.itcast.cn:8020/spark/jars/*"
		) ;
		jobConf.setProperty("mainClass", "org.apache.spark.examples.SparkPi") ;
		jobConf.setProperty("appName", "SparkExamplePi") ;
		jobConf.setProperty("jarPath",
			"hdfs://bigdata-cdh01.itcast.cn:8020/user/root/oozie_works/wf_yarn_pi/lib/spark-examples_2.11-2.2.0.jar") ;
		jobConf.setProperty("appParam", "10") ;
		// 2.4. Oozie Workflow 参数
		jobConf.setProperty(OozieClient.APP_PATH,
			"hdfs://bigdata-cdh01.itcast.cn:8020/user/root/oozie_works/wf_yarn_pi/workflow.xml") ;

		// 3. 提交执行WorkFlow作业
		String jobId = oozieClient.run(jobConf) ;
		System.out.println("Job Id = " + jobId);

		// 4. 依据JobID获取转态信息
		while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
			System.out.println("Workflow job running ...");
			Thread.sleep(10 * 1000);
		}
		System.out.println("Workflow job completed ...");
	}
}
