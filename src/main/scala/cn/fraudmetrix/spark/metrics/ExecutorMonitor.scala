package cn.fraudmetrix.spark.metrics

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.diagnosis.data.{AppEvent, ExecutorMonitorInfo}
import org.apache.spark.diagnosis.utils.{HttpClient, MetricsSinkFactory, MetricsUtils}
import org.apache.spark.internal.Logging

import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * @author futao
  * create at 2018/11/19
  */

class HeapMemoryInfo {
	
	var minMemory = 0xFFFFFFFFFFFL
	
	var maxMemory = 0L
	
	var totalMemory = 0L
	
	var count = 0

	override def toString: String = {
		s"min heap memory: ${MetricsUtils.convertUnit(minMemory)}; " +
		s"average heap memory: ${if (count > 0) MetricsUtils.convertUnit(totalMemory/count) else MetricsUtils.convertUnit(totalMemory)}; " +
		s"max heap memory: ${MetricsUtils.convertUnit(maxMemory)} "
	}
}

object ExecutorMonitor extends Logging {
	
	val executorToHost = new mutable.HashMap[String, String]()
	
	val executorToMemory = new mutable.HashMap[String, HeapMemoryInfo]()
	
	val executorLostCount = new mutable.HashMap[String, Int]()
	
	val executorNotifyCount = new mutable.HashMap[String, Int]()
	
	var appIdPath: String = _
	
	var conf: Configuration = _
	
	var executorMemory: Long = _
	
	private def updateExecutorServerHost(): Unit = {
		if (StringUtils.isNotEmpty(appIdPath)) {
			val fs = FileSystem.get(conf)
			for (status <- fs.listStatus(new Path(appIdPath))) {
				val name = status.getPath.getName
				if (StringUtils.startsWith(name, "executor_")) {
					val executorId = StringUtils.substringAfter(name, "executor_")
					if (!executorToHost.contains(executorId)) {
						val serverFile = new Path(s"${status.getPath}/server")
						if (fs.exists(serverFile)) {
							val input = fs.open(serverFile)
							val lines = IOUtils.readLines(input)
							if (lines.size() > 0) {
								val hostPort = lines.get(0)
								executorToHost(executorId) = hostPort
								executorLostCount(executorId) = 0
								executorNotifyCount(executorId) = 0
								logInfo(s"executor $executorId running server on $hostPort")
							}
							IOUtils.closeQuietly(input)
						}
					}
				}
			}
		}
	}
	
	def updateExecutorJvmInfo(executorId: String): Unit = {
		val hostPort = executorToHost(executorId)
		val url = s"http://$hostPort/executor"
		val response = HttpClient.sendGet(url)
		logDebug(s"executor $executorId jvm info: $response")
		if (StringUtils.isNotEmpty(response)) {
			val json = parse(response)
			val map = json.values.asInstanceOf[Map[String, _]]
			val heapMemory = map("heapMemoryTotalUsed").asInstanceOf[Double].toLong
			if (!executorToMemory.contains(executorId)) {
				val heapMemoryInfo = new HeapMemoryInfo
				executorToMemory(executorId) = heapMemoryInfo
			}
			val heapMemoryInfo = executorToMemory(executorId)
			if (heapMemory > heapMemoryInfo.maxMemory) {
				heapMemoryInfo.maxMemory = heapMemory
			}
			if (heapMemory < heapMemoryInfo.minMemory) {
				heapMemoryInfo.minMemory = heapMemory
			}
			heapMemoryInfo.totalMemory += heapMemory
			heapMemoryInfo.count += 1
			
			if (heapMemory * 1.0 / executorMemory > 0.9) {
				if (executorNotifyCount(executorId) == 0) {
					val message = s"WARN: executor_$executorId max memory is ${MetricsUtils.convertUnit(executorMemory)}, current heap memory is ${MetricsUtils.convertUnit(heapMemory)}"
					MetricsSinkFactory.printLog(
						ExecutorMonitorInfo("", "WARN", AppEvent.Event.EXECUTOR_INFO, message)
					)
				}
				executorNotifyCount(executorId) += 1
				if (executorNotifyCount(executorId) >= 10) {
					executorNotifyCount(executorId) = 0
				}
			}
		} else {
			logError(s"can not get connect of executor $executorId")
			executorLostCount(executorId) += 1
			if (executorLostCount(executorId) >= 3) {
				logError(s"lost executor_$executorId 3 times, jvm info: ${executorToMemory(executorId)}")
				val path = new Path(s"$appIdPath/executor_$executorId/server")
				val fs = FileSystem.get(conf)
				if (fs.exists(path)) {
					fs.delete(path, true)
				}
				executorToHost.remove(executorId)
			}
		}
	}
	
	def startMonitor(): Unit = {

		val executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
			override def newThread(r: Runnable): Thread = {
				val t = new Thread(r, "monitor")
				t.setDaemon(true)
				if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
				t
			}
		})

		executor.scheduleAtFixedRate(new Runnable {
			override def run(): Unit = {
				try {
					updateExecutorServerHost()
					for (executorId <- executorToHost.keySet) {
						updateExecutorJvmInfo(executorId)
					}
				} catch {
					case e:Exception =>
						logError(s"update executor metrics error ", e)
				}
			}
		}, 10, 10, TimeUnit.SECONDS)
	}
}
