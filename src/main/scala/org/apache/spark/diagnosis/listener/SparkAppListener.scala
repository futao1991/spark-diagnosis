package org.apache.spark.diagnosis.listener

import cn.fraudmetrix.spark.metrics.ExecutorMonitor
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.diagnosis.data._
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

		//spark metrics config
		StageHeuristic.prometheusUrl = sparkContext.getConf.get("spark.diagnosis.prometheus.url", "")
        MetricsSinkFactory.metaServerUrl = sparkContext.getConf.get("spark.diagnosis.metaServer.url", "")
		MetricsSinkFactory.graphiteHost = sparkContext.getConf.get("spark.diagnosis.graphite.host", "")
		MetricsSinkFactory.graphitePort = sparkContext.getConf.getInt("spark.diagnosis.graphite.port", 0)
		MetricsSinkFactory.sinkType = sparkContext.getConf.get("spark.diagnosis.sink.type", "metaServer")
		MetricsSinkFactory.initSink()

		JobGroupScheduleFactory.addMonitor(applicationStart.appId)
		applicationStart.appId.foreach(appId = _)
		startTime = System.currentTimeMillis()

		ExecutorMonitor.conf = sparkContext.hadoopConfiguration
		ExecutorMonitor.executorMemory = sparkContext.getConf.getSizeAsBytes("spark.executor.memory", "1073741824")
		val appIdPath = sparkContext.getConf.get("spark.diagnosis.jvm.path", "")
		ExecutorMonitor.startMonitor()
		if (StringUtils.isNotEmpty(appIdPath)) {
			try {
				val path = s"$appIdPath/$appId"
                ExecutorMonitor.appIdPath = path
				val fs = FileSystem.get(sparkContext.hadoopConfiguration)
				fs.mkdirs(new Path(path), new FsPermission("777"))
				fs.setPermission(new Path(path), new FsPermission("777"))
			} catch {
				case e:Exception => logError(s"create path $appIdPath/$appId error: ${e.getMessage}")
			}
		}
		
		val sparkConfig = MetricsUtils.getSparkConfig(sparkContext)
		yarnClient.init(sparkContext.hadoopConfiguration)
		yarnClient.start()

		InternalStatusUtils.getAllExecutors.foreach(tuple =>{
			println(s"executor ${tuple._1} running on ${tuple._2}")
			StageMetricsData.executorMap(tuple._1) = tuple._2
		})

        val applicationReport = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId))
        val submitTime = applicationReport.getStartTime
        val user = applicationReport.getUser
        val queue = applicationReport.getQueue

        val appStartEvent = AppStartEvent(appId, "INFO", AppEvent.Event.APP_START,
            submitTime, startTime, user, queue, sparkConfig)
        MetricsSinkFactory.printLog(appStartEvent)
		MetricsSinkFactory.showMetrics(appStartEvent)
	}

	override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
		if (StringUtils.isNotEmpty(appId)) {
			val applicationReport = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId))
			val resourceUsageReport = applicationReport.getApplicationResourceUsageReport
			val memorySeconds = resourceUsageReport.getMemorySeconds
			val vCoreSeconds = resourceUsageReport.getVcoreSeconds
			val time = System.currentTimeMillis() - startTime

            val appEndEvent = AppEndEvent(appId, "INFO", AppEvent.Event.APP_END, time, memorySeconds, vCoreSeconds)
            MetricsSinkFactory.printLog(appEndEvent)
            MetricsSinkFactory.showMetrics(appEndEvent)

            for (executorId <- StageMetricsData.executorMap.keySet) {
                val executorRemovedEvent = getExecutorEvent(executorId)
                if (null != executorRemovedEvent) {
                    MetricsSinkFactory.printLog(executorRemovedEvent)
                    MetricsSinkFactory.showMetrics(executorRemovedEvent)
                }
            }
			MetricsSinkFactory.close()
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
				val inputBytes = stageInfo.taskMetrics.inputMetrics.bytesRead
				val outputBytes = stageInfo.taskMetrics.outputMetrics.bytesWritten
				val shuffleReadBytes = stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead
				val shuffleWriteBytes = stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten

                val stageCompletedEvent = StageCompletedEvent(appId, "INFO", AppEvent.Event.STAGE_COMPLETED, stageId, time,
                    taskNums, inputBytes, outputBytes, shuffleReadBytes, shuffleWriteBytes)

                MetricsSinkFactory.printLog(stageCompletedEvent)
                MetricsSinkFactory.showMetrics(stageCompletedEvent)
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
					TaskHeuristic.evaluate(appId, taskId)
				case _ =>
			}
		}
	}

	override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
		StageMetricsData.executorMap(executorAdded.executorId) = executorAdded.executorInfo.executorHost
	}

    private def getExecutorEvent(executorId: String): ExecutorRemovedEvent = {
        val executor = InternalStatusUtils.getExecutor(executorId)
        if (null != executor) {
            val host = executor.hostPort
            val startTime = executor.addTime.getTime
            val elapseTime = System.currentTimeMillis() - startTime
            val completedTasks = executor.completedTasks
            val failedTasks = executor.failedTasks
            val inputBytes = executor.totalInputBytes
            val shuffleReadBytes = executor.totalShuffleRead

            var maxMemory = 0L
            var averageHeapMemory = 0L

            if (ExecutorMonitor.executorToMemory.contains(executorId)) {
                val executorMemory = ExecutorMonitor.executorToMemory(executorId)
                maxMemory = executorMemory.maxMemory
                averageHeapMemory = if (executorMemory.count >0) executorMemory.totalMemory / executorMemory.count
                else executorMemory.totalMemory
            }

            return ExecutorRemovedEvent(appId, "INFO", AppEvent.Event.EXECUTOR_REMOVED, executorId, host, elapseTime,
                completedTasks, failedTasks, inputBytes, shuffleReadBytes, averageHeapMemory, maxMemory)
        }
        null
    }

	override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
		val executorId = executorRemoved.executorId
		StageMetricsData.executorMap.remove(executorId)

        val executorRemovedEvent = getExecutorEvent(executorId)
        if (null != executorRemovedEvent) {
            MetricsSinkFactory.printLog(executorRemovedEvent)
            MetricsSinkFactory.showMetrics(executorRemovedEvent)
        }
	}
}
