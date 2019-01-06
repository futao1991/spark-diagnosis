package org.apache.spark.diagnosis.listener

import cn.fraudmetrix.spark.metrics.ExecutorMonitor
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.diagnosis.data.StageMetricsData
import org.apache.spark.diagnosis.heuristic._
import org.apache.spark.diagnosis.schedule.JobGroupScheduleFactory
import org.apache.spark.diagnosis.status.InternalStatusUtils
import org.apache.spark.diagnosis.utils.{MetricsSinkFactory, MetricsUtils}
import org.apache.spark.{SparkContext, TaskFailedReason}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

/**
  * @author futao
  * create at 2018/9/10
  */
class SparkAppListener extends SparkListener with Logging {

	val yarnClient: YarnClient = YarnClient.createYarnClient()
	
	var appId: String = _
	
	var startTime = 0L
	
	override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

		var break = false
		var sparkContext: SparkContext = null
		var count = 0
		while (!break) {
			if (SparkContext.getActive.nonEmpty) {
				sparkContext = SparkContext.getActive.get
				break = true
			} else {
				count += 1
				if (count >= 20) {
					break = true
				}
				Thread.sleep(500)
			}
		}
		
		if (sparkContext == null || InternalStatusUtils.getAppStatusStore(sparkContext) == null) {
			throw new RuntimeException("can not get appStatusStore of sparkContext")
		}
		MetricsSinkFactory.initMetricsShowAdapter(sparkContext.conf)
		StageHeuristic.prometheusUrl = sparkContext.getConf.get("spark.diagnosis.prometheus.url", "")
		JobGroupScheduleFactory.addMonitor(applicationStart.appId)
		applicationStart.appId.foreach(appId = _)
		startTime = System.currentTimeMillis()

		ExecutorMonitor.conf = sparkContext.hadoopConfiguration
		ExecutorMonitor.executorMemory = sparkContext.getConf.getSizeAsBytes("spark.executor.memory", "1073741824")
		val appIdPath = sparkContext.getConf.get("spark.diagnosis.jvm.path", "")
		if (StringUtils.isNotEmpty(appIdPath)) {
			try {
				val path = s"$appIdPath/$appId"
				val fs = FileSystem.get(sparkContext.hadoopConfiguration)
				fs.mkdirs(new Path(path), new FsPermission("777"))
				fs.setPermission(new Path(path), new FsPermission("777"))
				ExecutorMonitor.appIdPath = path
				ExecutorMonitor.startMonitor()
			} catch {
				case e:Exception => logError(s"create path $appIdPath/$appId error: ${e.getMessage}")
			}
		}
		
		val sparkConfig = MetricsUtils.getSparkConfig(sparkContext)
		MetricsSinkFactory.metricsSink.showMetrics(ResultDetail(ResultLevel.INFO, MetricsInfo.ApplicationInfo, sparkConfig))
		
		yarnClient.init(sparkContext.hadoopConfiguration)
		yarnClient.start()
	}

	override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
		if (StringUtils.isNotEmpty(appId)) {
			val applicationReport = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId))
			val resourceUsageReport = applicationReport.getApplicationResourceUsageReport
			val memorySeconds = resourceUsageReport.getMemorySeconds
			val vCoreSeconds = resourceUsageReport.getVcoreSeconds
			val time = System.currentTimeMillis() - startTime
			val message = s"$appId end, elapsed time: ${MetricsUtils.convertTimeUnit(time)}, memorySeconds: $memorySeconds, vCoreSeconds: $vCoreSeconds"
			MetricsSinkFactory.metricsSink.showMetrics(ResultDetail(ResultLevel.INFO, MetricsInfo.ApplicationInfo, message))
			
			var seq = Seq[String]()
			for (executorId <- ExecutorMonitor.executorToMemory.keySet) {
				seq = seq :+ s"executorId_$executorId: ${ExecutorMonitor.executorToMemory(executorId)}"
			}
			val executorJvmInfo = s"""executor jvm info: ${seq.mkString("\n")}"""
			MetricsSinkFactory.metricsSink.showMetrics(ResultDetail(ResultLevel.INFO, MetricsInfo.ExecutorInfo, executorJvmInfo))
		}
	}

	override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
		
	}

	override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
		
	}

	override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
		val stageId = stageSubmitted.stageInfo.stageId
		StageMetricsData.stageInfoList(stageId) = stageSubmitted.stageInfo
		StageMetricsData.stageTaskNumMap(stageId) = 0
		StageHeuristic.evaluateShuffleRead(stageId, stageSubmitted.stageInfo.name)
	}

	override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
		val stageId = stageCompleted.stageInfo.stageId
		val stageName = stageCompleted.stageInfo.name
		val time = stageCompleted.stageInfo.completionTime.getOrElse(0L) - stageCompleted.stageInfo.submissionTime.getOrElse(0L)
		val taskNums = stageCompleted.stageInfo.numTasks
		
		StageHeuristic.evaluate(stageId)
		StageHeuristic.evaluateTaskMetricDistributions(stageId, stageCompleted.stageInfo.attemptId)
		if (stageCompleted.stageInfo.failureReason.isEmpty) {
			StageMetricsData.stageInfoList.get(stageId).foreach { stageInfo =>
				val inputBytes = MetricsUtils.convertUnit(stageInfo.taskMetrics.inputMetrics.bytesRead)
				val outputBytes = MetricsUtils.convertUnit(stageInfo.taskMetrics.outputMetrics.bytesWritten)
				val shuffleReadBytes = MetricsUtils.convertUnit(stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead)
				val shuffleWriteBytes = MetricsUtils.convertUnit(stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten)

				val message =
					s"""stage $stageId ($stageName) complete, total $taskNums task, time: ${MetricsUtils.convertTimeUnit(time)}
					   |input: $inputBytes, output: $outputBytes, shuffleRead: $shuffleReadBytes, shuffleWrite: $shuffleWriteBytes""".stripMargin
				MetricsSinkFactory.metricsSink.showMetrics(ResultDetail(ResultLevel.INFO, MetricsInfo.StageDataInfo, message))
			}
		}
	}

	override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
		StageMetricsData.stageTaskNumMap.synchronized(StageMetricsData.stageTaskNumMap(taskEnd.stageId) += 1)
		if (!taskEnd.taskInfo.successful) {
			val taskId = taskEnd.taskInfo.taskId
			taskEnd.reason match {
				case _: TaskFailedReason =>
					StageMetricsData.taskFailedMap(taskId) = taskEnd
					TaskHeuristic.evaluate(taskId)
				case _ =>
			}
		}
	}

	override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
		StageMetricsData.executorMap(executorAdded.executorId) = executorAdded.executorInfo.executorHost
	}

	override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
		val executorId = executorRemoved.executorId
		StageMetricsData.executorMap.remove(executorId)
		if (ExecutorMonitor.executorToMemory.contains(executorId)) {
			val message = s"executor_$executorId jvm info: ${ExecutorMonitor.executorToMemory(executorId)}"
			MetricsSinkFactory.metricsSink.showMetrics(ResultDetail(ResultLevel.INFO, MetricsInfo.ExecutorInfo, message))
		}
	}
}
