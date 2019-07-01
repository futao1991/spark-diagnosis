package org.apache.spark.diagnosis.heuristic

import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskFailedReason
import org.apache.spark.diagnosis.data._
import org.apache.spark.diagnosis.errordiagnosis.FailedReasonChecker
import org.apache.spark.diagnosis.status.InternalStatusUtils
import org.apache.spark.diagnosis.utils.{MetricsSinkFactory, MetricsUtils}
import org.apache.spark.internal.Logging

/**
  * @author futao
  * create at 2018/9/10
  */
object TaskHeuristic extends Logging {

	def evaluate(appId:String, taskId: Long): Unit = {
		StageMetricsData.taskFailedMap.get(taskId).foreach { taskEnd =>
            val stageId = taskEnd.stageId
			val taskId = taskEnd.taskInfo.taskId
			val executorId = taskEnd.taskInfo.executorId
			val reason = taskEnd.reason.asInstanceOf[TaskFailedReason]
			logError(s"task_$taskId failed, reason: ${reason.toErrorString}, running on executor $executorId")

            val taskFailedEvent = TaskFailedEvent(appId, "ERROR", AppEvent.Event.TASK_FAILED, stageId, taskId, executorId, reason.toErrorString)
            MetricsSinkFactory.printLog(taskFailedEvent)
            MetricsSinkFactory.showMetrics(taskFailedEvent)

            checkPossibleReason(taskId, reason.toErrorString, executorId)
		}
	}

	private def checkPossibleReason(taskId: Long, failedReason: String, executorId: String): Unit ={
		if (StringUtils.contains(failedReason, "Exit code is 143")) {
			val reason = checkReasonForOOM(taskId)
			val message =
				s"""executor $executorId failed due to OOM, possible reasons:
				   |1) data skew 2)executor memory too low 3)reading too large data\n $reason
				 """.stripMargin
			MetricsSinkFactory.printLog(
                TaskDiagnosisInfo("", "ERROR", AppEvent.Event.TASK_DIAGNOSIS, message)
            )
		} else {
			val reasonDiagnosis = FailedReasonChecker.checkFailedReason(failedReason)
			var reasonInfo = s"task $taskId failed: $failedReason "
			if (StringUtils.isNotEmpty(reasonDiagnosis)) {
				reasonInfo += reasonDiagnosis
			}
			MetricsSinkFactory.printLog(
                TaskDiagnosisInfo("", "ERROR", AppEvent.Event.TASK_DIAGNOSIS, reasonInfo)
            )
		}
	}


	private def checkReasonForOOM(taskId: Long): String ={
		val taskEnd = StageMetricsData.taskFailedMap(taskId)
		val stageId = taskEnd.stageId

		val reason = new StringBuilder
		val executorMemory = InternalStatusUtils.sc.conf.getSizeAsBytes("spark.executor.memory", "1073741824")
		reason.append(s"current executor memory: ${MetricsUtils.convertUnit(executorMemory)}\n")
		StageMetricsData.stageRunningData.get(stageId).foreach { dateBytes =>
			if (dateBytes._1 > 0) {
				reason.append(s"stage_$stageId input data: ${MetricsUtils.convertUnit(dateBytes._1)}\n")
			}
			if (dateBytes._2 > 0) {
				reason.append(s"stage_$stageId shuffle read data: ${MetricsUtils.convertUnit(dateBytes._2)}\n")
			}
			StageMetricsData.stageSkewFlag.get(stageId).foreach(msg => reason.append(msg))
		}
		reason.toString()
	}
}
