package cn.fraudmetrix.spark.metrics

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.uber.profiling.Reporter
import com.uber.profiling.util.JsonUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkEnv
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.diagnosis.utils.NetUtils
import org.slf4j.LoggerFactory
/**
  * @author futao
  * create at 2018/11/17
  */
class HdfsOutputReporter extends Reporter{
	
	private val logger = LoggerFactory.getLogger(classOf[HdfsOutputReporter])
	
	private var appIdPath: String = null
	
	def getAppIdPath: String = appIdPath
	
	def setAppIdPath(appIdPath: String): Unit = this.appIdPath = appIdPath
	
	private val inited = new AtomicBoolean(false)
	
	private val serverIsReady = new AtomicBoolean(false)
	
	private var fileOutputStream: FSDataOutputStream = null
	
	private var metricsCount = 0L
	
	private var executorIdPath: String = null
	
	private def createAppIdPathIfNecessary(conf: Configuration, appId: String): Unit ={
		try {
			val fs = FileSystem.get(conf)
			val path = new Path(s"$appIdPath/$appId")
			if (!fs.exists(path)) {
				fs.mkdirs(path, new FsPermission("777"))
			}
		} catch {
			case e: Exception => logger.error(s"create path $appIdPath/$appId error: ${e.getMessage}")
		}
	}
	
	private def init(conf: Configuration, appId: String): Unit ={
		conf.set("dfs.support.append", "true")
		createAppIdPathIfNecessary(conf, appId)
		val fs = FileSystem.get(conf)
		val path = new Path(executorIdPath)
		fs.mkdirs(path, new FsPermission("777"))
		fs.setPermission(path, new FsPermission("777"))

		val filePath = new Path(s"$executorIdPath/metrics")
		IOUtils.closeQuietly(fs.create(filePath))
		fs.setPermission(filePath, new FsPermission("755"))
		logger.info(s"create file $filePath")
		fileOutputStream = fs.append(filePath)
	}
	
	override def report(profilerName: String, metrics: util.Map[String, AnyRef]): Unit = {
		try {
			if (!inited.get()) {
				val sparkEnv = SparkEnv.get
				if (null != sparkEnv) {
					val appId = SparkEnv.get.conf.getAppId
					val executorId = SparkEnv.get.executorId
					executorIdPath = s"$appIdPath/$appId/executor_$executorId"
					
					init(SparkHadoopUtil.get.conf, appId)
					inited.getAndSet(true)
				}
			}
			
			if (inited.get() && !serverIsReady.get()) {
				if (ReporterServer.ready.get()) {
					val address = NetUtils.getEthernetAddress
					val port = ReporterServer.port.get()
					
					logger.info(s"server running on $address:$port")
					
					val path = new Path(s"$executorIdPath/server")
					val fs = FileSystem.get(SparkHadoopUtil.get.conf)
					val out = fs.create(path)
					fs.setPermission(path, new FsPermission("777"))
					IOUtils.write(s"$address:$port", out)
					IOUtils.closeQuietly(out)
					
					serverIsReady.getAndSet(true)
				}
			}
			
			if (null != fileOutputStream) {
				fileOutputStream.writeBytes(JsonUtils.serialize(metrics) + "\n")
				metricsCount += 1
				if (serverIsReady.get()) {
					ReporterServer.updateInfo(metrics)
				}
				
				if (metricsCount % 100 == 0) {
					val file = s"$executorIdPath/metrics"
					val filePath = new Path(file)
					fileOutputStream.close()
					val fs = FileSystem.get(SparkHadoopUtil.get.conf)
					fileOutputStream = fs.append(filePath)
					logger.info(s"flush 100 records metrics into file $file")
				}
			}
		} catch {
			case e: Exception => logger.error("report metrics error ", e)
		}
	}

	override def close(): Unit = {
		val path = new Path(s"$executorIdPath/server")
		val fs = FileSystem.get(SparkHadoopUtil.get.conf)
		if (fs.exists(path)) {
			fs.delete(path, true)
		}
		if (null != fileOutputStream) {
			fileOutputStream.close()
		}
	}
}
