package org.apache.spark.diagnosis.heuristic

import java.util
import java.util.Date

import com.codahale.metrics.{ExponentiallyDecayingReservoir, Histogram}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.diagnosis.status.InternalStatusUtils
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1
import org.apache.spark.storage.RDDInfo
import org.apache.spark.diagnosis.data.{AppEvent, StageDiagnosisInfo, StageMetricsData}
import org.apache.spark.diagnosis.utils.{MetricsSinkFactory, MetricsUtils, PrometheusUtils}

import scala.collection.mutable

/**
  * @author futao
  * create at 2018/9/10
  */
object StageHeuristic extends Logging {

	val stageRddInfoMap = new mutable.HashMap[Int, RDDInfo]()
	
	var prometheusUrl: String = _

	def evaluate(stageId: Int, onlyCheckRunning: Boolean = false): Unit = {

		StageMetricsData.stageInfoList.get(stageId).foreach { stageInfo =>
			val stageAttemptId = stageInfo.attemptId
			val stageName = stageInfo.name
			val taskList = InternalStatusUtils.getTaskList(stageId, stageAttemptId)
			evaluateTaskDistribute(stageId, stageName, taskList, onlyCheckRunning)
		}
	}

	/**
	  * 评估task的运行时间分布
	  */
	private def evaluateTaskDistribute(stageId: Int, stageName: String, taskList: Seq[v1.TaskData], onlyCheckRunning: Boolean = false): Unit ={
		var minTime = 0L
		var mediumTime = 0L
		var maxTime = 0L
		var maxTimeTaskId = -1L
		var maxTimeTaskExecutorId = ""
		var maxTimeTaskHost = ""
		var maxTimeTaskIsRunning = false

		var minInputRecord = 0L
		var mediumInputRecord = 0L
		var maxInputRecord = 0L
		var maxInputRecordTaskId = -1L
		var maxInputRecordTaskIsRunning = false

		var minShuffleReadRecord = 0L
		var mediumShuffleReadRecord = 0L
		var maxShuffleReadRecord = 0L
		var maxShuffleReadRecordTaskId = -1L
		var maxShuffleReadRecordTaskIsRunning = false

		var minShuffleWriteRecord = 0L
		var mediumShuffleWriteRecord = 0L
		var maxShuffleWriteRecord = 0L
		var maxShuffleWriteRecordTaskId = -1L
		var maxShuffleWriteRecordTaskIsRunning = false

		var maxTaskGCTime = 0L
		var maxTaskGCIDRUNTIME = 0L
		var maxTaskGCId = -1L
		var maxTaskGCIsRunning = false

		var inputBytes = 0L
		var shuffleReadBytes = 0L

		val currentTime = new Date().getTime
		val taskTimeHistogram = new Histogram(new ExponentiallyDecayingReservoir())
		val taskReadHistogram = new Histogram(new ExponentiallyDecayingReservoir())
		val taskShuffleReadHistogram = new Histogram(new ExponentiallyDecayingReservoir())
		val taskShuffleWriteHistogram = new Histogram(new ExponentiallyDecayingReservoir())
		taskList.foreach { taskData =>
				
			/**
			  * task运行时间
			  */
			val time = if (StringUtils.equals(taskData.status, "RUNNING")) {
				currentTime - taskData.launchTime.getTime
			} else {
				taskData.duration.getOrElse(0L)
			}
			if (time > maxTime) {
				maxTime = time
				maxTimeTaskId = taskData.taskId
				maxTimeTaskExecutorId = taskData.executorId
				maxTimeTaskHost = taskData.host
				maxTimeTaskIsRunning = StringUtils.equals(taskData.status, "RUNNING")
			}
			taskTimeHistogram.update(time)

			/**
			  * task input 数据
			  */
			val recordsRead = taskData.taskMetrics.map(_.inputMetrics.recordsRead).getOrElse(0L)
			if (StringUtils.equals(taskData.status, "SUCCESS") ||
					(!StringUtils.equals(taskData.status, "FAILED") && time > 10000)) {
				if (recordsRead > maxInputRecord) {
					maxInputRecord = recordsRead
					maxInputRecordTaskId = taskData.taskId
					maxInputRecordTaskIsRunning = StringUtils.equals(taskData.status, "RUNNING")
				}
				taskReadHistogram.update(recordsRead)
			}
			if (!StringUtils.equals(taskData.status, "FAILED")) {
				inputBytes += taskData.taskMetrics.map(_.inputMetrics.bytesRead).getOrElse(0L)
			}

			/**
			  * task shuffle read数据
			  */
			val shuffleRead = taskData.taskMetrics.map(_.shuffleReadMetrics.recordsRead).getOrElse(0L)
			if (shuffleRead > maxShuffleReadRecord) {
				maxShuffleReadRecord = shuffleRead
				maxShuffleReadRecordTaskId = taskData.taskId
				maxShuffleReadRecordTaskIsRunning = StringUtils.equals(taskData.status, "RUNNING")
			}
			if (!StringUtils.equals(taskData.status, "FAILED")) {
				shuffleReadBytes += taskData.taskMetrics.map(metric => metric.shuffleReadMetrics.localBytesRead + metric.shuffleReadMetrics.remoteBytesRead).getOrElse(0L)
			}
			taskShuffleReadHistogram.update(shuffleRead)

			/**
			  * task shuffle write数据
			  */
			val shuffleWrite = taskData.taskMetrics.map(_.shuffleWriteMetrics.recordsWritten).getOrElse(0L)
			if (shuffleWrite > maxShuffleWriteRecord) {
				maxShuffleWriteRecord = shuffleWrite
				maxShuffleWriteRecordTaskId = taskData.taskId
				maxShuffleWriteRecordTaskIsRunning = StringUtils.equals(taskData.status, "RUNNING")
			}
			taskShuffleWriteHistogram.update(shuffleWrite)

			val taskGcTime = taskData.taskMetrics.map(_.jvmGcTime).getOrElse(0L)
			if (taskGcTime > 1000 && time > 10000) {
				if (taskGcTime > maxTaskGCTime) {
					maxTaskGCTime = taskGcTime
					maxTaskGCIDRUNTIME = time
					maxTaskGCId = taskData.taskId
					maxTaskGCIsRunning = StringUtils.equals(taskData.status, "RUNNING")
				}
			}
		}

		minTime = taskTimeHistogram.getSnapshot.getMin
		mediumTime = taskTimeHistogram.getSnapshot.getMedian.toLong
		maxTime =  taskTimeHistogram.getSnapshot.getMax

		minInputRecord = taskReadHistogram.getSnapshot.getMin
		mediumInputRecord = taskReadHistogram.getSnapshot.getMean.toLong
		maxInputRecord = taskReadHistogram.getSnapshot.getMax
		val seventyInputRecord = taskReadHistogram.getSnapshot.get75thPercentile.toLong

		minShuffleReadRecord = taskShuffleReadHistogram.getSnapshot.getMin
		mediumShuffleReadRecord = taskShuffleReadHistogram.getSnapshot.getMedian.toLong
		maxShuffleReadRecord = taskShuffleReadHistogram.getSnapshot.getMax
		val seventyShuffleReadRecord = taskShuffleReadHistogram.getSnapshot.get75thPercentile.toLong

		minShuffleWriteRecord = taskShuffleWriteHistogram.getSnapshot.getMin
		mediumShuffleWriteRecord = taskShuffleWriteHistogram.getSnapshot.getMedian.toLong
		maxShuffleWriteRecord = taskShuffleWriteHistogram.getSnapshot.getMax
		val seventyShuffleWriteRecord = taskShuffleWriteHistogram.getSnapshot.get75thPercentile.toLong

		StageMetricsData.stageRunningData(stageId) = (inputBytes, shuffleReadBytes)

		var needToCheckPrometheus = false
		/**
		  * 判断是否存在长尾task
		  */
		val timeGap = maxTime - mediumTime
		val timeGapRatio = if (mediumTime == 0) 1 else timeGap * 1.0 / mediumTime
		if (timeGap > 20000 && timeGapRatio > HeuristicConf.STAGE_TASK_DISTRIBUTION_GAP) {
			if ((onlyCheckRunning && maxTimeTaskIsRunning) || !onlyCheckRunning) {
				needToCheckPrometheus = true
				val message = s"longTail task occurred in stage $stageId ($stageName), max time: $maxTime ms, taskId: $maxTimeTaskId"
				if (HeuristicConf.LongTailTaskNotifyCount == 0) {
					MetricsSinkFactory.getLogMetricsSink.showMetrics(
                        StageDiagnosisInfo("", "WARN", AppEvent.Event.STAGE_DIAGNOSIS, message)
                    )
				}
				HeuristicConf.LongTailTaskNotifyCount += 1
				if (HeuristicConf.LongTailTaskNotifyCount >= 20) HeuristicConf.LongTailTaskNotifyCount = 0
			}
		}

		/**
		  * 判断是否存在shuffle read数据倾斜
		  */
		val shuffleRecordGap = maxShuffleReadRecord - mediumShuffleReadRecord
		val shuffleRecordGapRatio = if (mediumShuffleReadRecord == 0) 1 else shuffleRecordGap * 1.0 / mediumShuffleReadRecord
		val seventyShuffleRecordGap = maxShuffleReadRecord - seventyShuffleReadRecord
		val seventyShuffleRecordGapRatio = if (seventyShuffleReadRecord == 0) 1 else seventyShuffleRecordGap * 1.0 / seventyShuffleReadRecord

		if (shuffleRecordGap > 2000 && shuffleRecordGapRatio > HeuristicConf.STAGE_TASK_SHUFFLE_SKEW_GAP &&
				seventyShuffleRecordGapRatio > HeuristicConf.STAGE_TASK_SHUFFLE_SKEW_GAP / 1.5) {
			if ((onlyCheckRunning && maxShuffleReadRecordTaskIsRunning) || !onlyCheckRunning) {
				if (!stageRddInfoMap.contains(stageId)) {
					getStageRDDs(stageId).foreach(rddInfo => stageRddInfoMap(stageId) = rddInfo)
				}
				if (maxShuffleReadRecordTaskId == maxTimeTaskId) {
					needToCheckPrometheus = false
				}

				var message = s"shuffle read skew occurred in stage $stageId ($stageName), max shuffle read records: $maxShuffleReadRecord, taskId: $maxShuffleReadRecordTaskId"
				val rddInfo = stageRddInfoMap.get(stageId)
				if (rddInfo.isDefined) {
					message = s"$message, site: ${rddInfo.get.callSite}"
				}
				if (HeuristicConf.DataSkewNotifyCount == 0) {
					StageMetricsData.stageSkewFlag(stageId) = message
					MetricsSinkFactory.getLogMetricsSink.showMetrics(
						StageDiagnosisInfo("", "WARN", AppEvent.Event.STAGE_DIAGNOSIS, message))
				}
				HeuristicConf.DataSkewNotifyCount += 1
				if (HeuristicConf.DataSkewNotifyCount >= 10) HeuristicConf.DataSkewNotifyCount = 0
			}
		}

		/**
		  * 判断是否存在shuffle write数据倾斜
		  */
		val shuffleWriteRecordGap = maxShuffleWriteRecord - mediumShuffleWriteRecord
		val shuffleWriteRecordGapRatio = if (mediumShuffleWriteRecord == 0) 1 else shuffleWriteRecordGap * 1.0 / mediumShuffleWriteRecord
		val seventyShuffleWriteRecordGap = maxShuffleWriteRecord - seventyShuffleWriteRecord
		val seventyShuffleWriteRecordGapRatio = if (seventyShuffleWriteRecord == 0) 1 else seventyShuffleWriteRecordGap * 1.0 / seventyShuffleWriteRecord

		if (shuffleWriteRecordGap > 2000 && shuffleWriteRecordGapRatio > HeuristicConf.STAGE_TASK_SHUFFLE_SKEW_GAP &&
				seventyShuffleWriteRecordGapRatio > HeuristicConf.STAGE_TASK_SHUFFLE_SKEW_GAP / 1.5) {
			if ((onlyCheckRunning && maxShuffleWriteRecordTaskIsRunning) || !onlyCheckRunning) {
				if (!stageRddInfoMap.contains(stageId)) {
					getStageRDDs(stageId).foreach(rddInfo => stageRddInfoMap(stageId) = rddInfo)
				}
				if (maxShuffleWriteRecordTaskId == maxTimeTaskId) {
					needToCheckPrometheus = false
				}

				var message = s"shuffle write skew occurred in stage $stageId ($stageName), max shuffle write records: $maxShuffleWriteRecord, taskId: $maxShuffleWriteRecordTaskId"
				val rddInfo = stageRddInfoMap.get(stageId)
				if (rddInfo.isDefined) {
					message = s"$message, site: ${rddInfo.get.callSite}"
				}
				if (HeuristicConf.DataSkewNotifyCount == 0) {
					MetricsSinkFactory.getLogMetricsSink.showMetrics(
                        StageDiagnosisInfo("", "WARN", AppEvent.Event.STAGE_DIAGNOSIS, message)
                    )
				}
				HeuristicConf.DataSkewNotifyCount += 1
				if (HeuristicConf.DataSkewNotifyCount >= 10) HeuristicConf.DataSkewNotifyCount = 0
			}
		}

		/**
		  * 判断是否存在输入数据倾斜
		  */
		val inputRecordGap = maxInputRecord - mediumInputRecord
		val inputRecordGapRatio = if (mediumInputRecord == 0) 1 else inputRecordGap * 1.0 / mediumInputRecord
		val seventyInputRecordGap = maxInputRecord - seventyInputRecord
		val seventyInputRecordGapRatio = if (seventyInputRecord==0) 1 else seventyInputRecordGap * 1.0 / seventyInputRecord

		if (inputRecordGap > 5000 && inputRecordGapRatio > HeuristicConf.STAGE_TASK_INPUT_SKEW_GAP &&
				seventyInputRecordGapRatio > HeuristicConf.STAGE_TASK_INPUT_SKEW_GAP / 1.5) {
			if ((onlyCheckRunning && maxInputRecordTaskIsRunning) || !onlyCheckRunning) {
				if (maxInputRecordTaskId == maxTimeTaskId) {
					needToCheckPrometheus = false
				}

				val message = s"input skew occurred in stage $stageId ($stageName), max input records: $maxInputRecord, taskId: $maxInputRecordTaskId"
				if (HeuristicConf.DataSkewNotifyCount == 0) {
					StageMetricsData.stageSkewFlag(stageId) = message
					MetricsSinkFactory.getLogMetricsSink.showMetrics(
                        StageDiagnosisInfo("", "WARN", AppEvent.Event.STAGE_DIAGNOSIS, message)
                    )
				}
				HeuristicConf.DataSkewNotifyCount += 1
				if (HeuristicConf.DataSkewNotifyCount >= 10) HeuristicConf.DataSkewNotifyCount = 0
			}
		}

		/**
		  * 判断是否存在gc频繁的task
		  */
		if (maxTaskGCTime > HeuristicConf.STAGE_TASK_GCTIME_GAP && (maxTaskGCTime * 1.0)/maxTaskGCIDRUNTIME > HeuristicConf.STAGE_TASK_GCRATIO_GAP) {
			if ((onlyCheckRunning && maxTaskGCIsRunning) || !onlyCheckRunning) {
				val message = s"task $stageId.$maxTaskGCId GC overhead: running time:$maxTaskGCIDRUNTIME ms, gc time:$maxTaskGCTime ms"
				if (maxTaskGCId == maxTimeTaskId) {
					needToCheckPrometheus = false
				}

				if (HeuristicConf.GcOverHeadNotifyCount == 0) {
					MetricsSinkFactory.getLogMetricsSink.showMetrics(
                        StageDiagnosisInfo("", "WARN", AppEvent.Event.STAGE_DIAGNOSIS, message)
                    )
				}
				HeuristicConf.GcOverHeadNotifyCount += 1
				if (HeuristicConf.GcOverHeadNotifyCount >= 10) HeuristicConf.GcOverHeadNotifyCount = 0
			}
		}

		/**
		  * 调用Prometheus检测任务所在节点的状态
		  */
		if (needToCheckPrometheus && StringUtils.isNotEmpty(maxTimeTaskHost)) {
			if (StringUtils.isNotEmpty(prometheusUrl)) {
				val loadAverage = PrometheusUtils.getLoadAverageByHostName(prometheusUrl, maxTimeTaskHost)
				val cpuUsage = PrometheusUtils.getCpuUsageByHostName(prometheusUrl, maxTimeTaskHost)
				
				if (HeuristicConf.PrometheusNotifyCount == 0) {
					val message =
						s"""task $maxTimeTaskId running on $maxTimeTaskHost,
						   | Load 1m: ${loadAverage._1}, Load 5m: ${loadAverage._2}, Load 15m: ${loadAverage._3}
						   | CPU usage: $cpuUsage
						 """.stripMargin
					MetricsSinkFactory.getLogMetricsSink.showMetrics(
                        StageDiagnosisInfo("", "INFO", AppEvent.Event.STAGE_DIAGNOSIS, message)
                    )
				}
				HeuristicConf.PrometheusNotifyCount += 1
				if (HeuristicConf.PrometheusNotifyCount >= 30) HeuristicConf.PrometheusNotifyCount = 0
			}
		}
	}

	def evaluateTaskMetricDistributions(stageId: Int, stageAttemptId: Int): Unit ={
		val taskSummary = InternalStatusUtils.getTaskSummary(stageId, stageAttemptId)
		taskSummary.foreach { distributions =>
			var executorComputeTime = 0.0
			distributions.executorRunTime.foreach(time => executorComputeTime += time)
			var schedulerDelay = 0.0
			distributions.schedulerDelay.foreach(time => schedulerDelay += time)

			if ((schedulerDelay * 1.0) / executorComputeTime > HeuristicConf.STAGE_TASK_SCHEDULER_DELAY_GAP) {
				val medianTime = distributions.executorRunTime(2)
				val medianDelay = distributions.schedulerDelay(2)
				val message = s"stage $stageId average task time: $medianTime ms, average delay $medianDelay ms"
				MetricsSinkFactory.getLogMetricsSink.showMetrics(
                    StageDiagnosisInfo("", "WARN", AppEvent.Event.STAGE_DIAGNOSIS, message)
                )
			}
		}
	}

	def evaluateShuffleRead(stageId: Int, stageName: String): Unit ={
		val stageInfo = StageMetricsData.stageInfoList(stageId)
		var parentShuffleWritten = 0L
		stageInfo.parentIds.foreach(stageId =>{
			val bytes = StageMetricsData.stageInfoList.get(stageId).map(_.taskMetrics.shuffleWriteMetrics.bytesWritten).getOrElse(0L)
			parentShuffleWritten += bytes
		})
		if (parentShuffleWritten > 0) {
			val taskNums = stageInfo.numTasks
			val message = s"stage $stageId ($stageName) prepare to shuffle read ${MetricsUtils.convertUnit(parentShuffleWritten)}, total tasks: $taskNums"
			MetricsSinkFactory.getLogMetricsSink.showMetrics(
                StageDiagnosisInfo("", "INFO", AppEvent.Event.STAGE_DIAGNOSIS, message)
            )
		}
	}

	def getStageRDDs(stageId: Int): Option[RDDInfo] ={

		def checkContains(rddLists: util.List[Int], parentIds: Seq[Int]): Boolean = {
			for (id <- parentIds) {
				if (!rddLists.contains(id)) return false
			}
			true
		}

		StageMetricsData.stageInfoList.get(stageId).map(stageInfo => {
			val rddInfos = stageInfo.rddInfos

			val rddLists = new util.ArrayList[Int]()
			val rddInfoLists = new mutable.HashMap[Int, RDDInfo]()
			for (rdd <- rddInfos) {
				rddLists.add(rdd.id)
				rddInfoLists(rdd.id) = rdd
			}

			val map = new mutable.HashMap[Int, Seq[Int]]()
			var leafRddId = -1
			for (rdd <- rddInfos) {
				if (checkContains(rddLists, rdd.parentIds)) {
					map(rdd.id) = rdd.parentIds
				} else {
					map(rdd.id) = Seq[Int]()
				}
				if (rdd.id > leafRddId) leafRddId = rdd.id
			}
			var break = false
			var searchRdd = leafRddId
			while (!break) {
				val parentIds = map(searchRdd)
				if (parentIds.length != 1) {
					break = true
				} else {
					searchRdd = parentIds.head
				}
			}
			rddInfoLists(searchRdd)
		})
	}
}
