package org.apache.spark.diagnosis.heuristic

/**
  * @author futao
  * create at 2018/9/10
  */
class ResultDetail(val level:ResultLevel, val name: MetricsInfo, val message: String) {

}

object ResultDetail {

	def apply(level:ResultLevel, name: MetricsInfo, message: String) = new ResultDetail(level, name, message)
}

sealed class ResultLevel(level: String) {
	override def toString: String = level
}

object ResultLevel {

	object INFO extends ResultLevel("info")

	object WARN extends ResultLevel("warn")

	object ERROR extends ResultLevel("error")
}

sealed class MetricsInfo(name: String) {
	override def toString: String = name
}

object MetricsInfo {

	object StageDataInfo extends MetricsInfo("stageInfo")

	object LongTailTask extends MetricsInfo("longTailTask")

	var LongTailTaskNotifyCount = 0

	object DataSkew extends MetricsInfo("dataSkew")

	var DataSkewNotifyCount = 0

	object GcOverHead extends MetricsInfo("GCOverhead")

	var GcOverHeadNotifyCount = 0
	
	var PrometheusNotifyCount = 0

	object ScheduleDelay extends MetricsInfo("scheduleDelay")

	object TaskFailed extends MetricsInfo("TaskFailed")
	
	object NodeStatus extends MetricsInfo("nodeStatus")
	
	object ExecutorInfo extends MetricsInfo("executorInfo")
	
	object ApplicationInfo extends MetricsInfo("ApplicationInfo")
}