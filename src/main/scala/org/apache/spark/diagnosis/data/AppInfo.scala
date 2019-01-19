package org.apache.spark.diagnosis.data

import java.util
import scala.collection.JavaConversions._
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.spark.diagnosis.utils.MetricsUtils

object AppEvent {

    object Event extends Enumeration {
        val APP_START, APP_END, STAGE_COMPLETED, EXECUTOR_REMOVED, TASK_FAILED,
        STAGE_DIAGNOSIS, TASK_DIAGNOSIS, EXECUTOR_INFO = Value
    }
    type Event = Event.Value
}

sealed abstract class AppInfo(appId: String, level: String, val event: AppEvent.Event) {

    def toJSONInfo: JSONObject = null

    def toConsoleInfo: String = null
}

case class AppStartEvent(
     appId: String,
     level: String,
     override val event: AppEvent.Event,
     submitTime: Long,
     startTime: Long,
     user: String,
     queue: String,
     configuration: util.Map[String, Any]) extends AppInfo(appId, level, event) {

    override def toJSONInfo: JSONObject = {
        val json = new JSONObject
        json.put("appId", appId)
        json.put("submitTime", submitTime)
        json.put("startTime", startTime)
        json.put("user", user)
        json.put("queue", queue)
        json.put("configuration", configuration)

        json
    }

    override def toConsoleInfo: String = {
        var seq = Seq[String]()
        seq = seq :+ s"$level: appId: $appId, configuration:"
        for (keyValue <- configuration) {
            val key = keyValue._1
            var value = keyValue._2
            if (StringUtils.containsIgnoreCase(key, "memory")) {
                value = MetricsUtils.convertUnit(value.toString.toLong)
            }
            seq = seq :+ s"$key: $value"
        }
        seq.mkString("\n")
    }
}

case class AppEndEvent(
     appId: String,
     level: String,
     override val event: AppEvent.Event,
     elapseTime: Long,
     memorySeconds: Long,
     vCoreSeconds: Long) extends AppInfo(appId, level, event) {

    override def toJSONInfo: JSONObject = {
        val json = new JSONObject
        json.put("appId", appId)
        json.put("elapseTime", elapseTime)
        json.put("memorySeconds", memorySeconds)
        json.put("vCoreSeconds", vCoreSeconds)

        json
    }

    override def toConsoleInfo: String = {
        s"""$level: $appId end, elapsed time: ${MetricsUtils.convertTimeUnit(elapseTime)}
           | memorySeconds: $memorySeconds, vCoreSeconds: $vCoreSeconds
         """.stripMargin
    }
}

case class StageCompletedEvent(
     appId: String,
     level: String,
     override val event: AppEvent.Event,
     stageId: Int,
     elapseTime: Long,
     totalTask: Int,
     inputBytes: Long,
     outputBytes: Long,
     shuffleReadBytes: Long,
     shuffleWriteBytes: Long) extends AppInfo(appId, level, event) {

    override def toJSONInfo: JSONObject = {
        val json = new JSONObject()
        json.put("appId", appId)
        json.put("elapseTime", elapseTime)
        json.put("stageId", stageId)
        json.put("totalTask", totalTask)
        json.put("inputBytes", inputBytes)
        json.put("outputBytes", outputBytes)
        json.put("shuffleReadBytes", shuffleReadBytes)
        json.put("shuffleWriteBytes", shuffleWriteBytes)

        json
    }

    override def toConsoleInfo: String = {
        s"""$level: stage $stageId complete, time: ${MetricsUtils.convertTimeUnit(elapseTime)}, totalTask: $totalTask,
           input: ${MetricsUtils.convertUnit(inputBytes)},
           output: ${MetricsUtils.convertUnit(outputBytes)},
           shuffleRead: ${MetricsUtils.convertUnit(shuffleReadBytes)},
           shuffleWrite: ${MetricsUtils.convertUnit(shuffleWriteBytes)},
         """.stripMargin
    }
}

case class TaskFailedEvent(
     appId: String,
     level: String,
     override val event: AppEvent.Event,
     stageId: Int,
     taskId: Long,
     executorId: String,
     failedReason: String) extends AppInfo(appId, level, event) {

    override def toJSONInfo: JSONObject = {
        val json = new JSONObject
        json.put("appId", appId)
        json.put("stageId", stageId)
        json.put("taskId", taskId)
        json.put("executorId", executorId)
        json.put("failedReason", failedReason)

        json
    }

    override def toConsoleInfo: String = {
        s"""$level: task $stageId.$taskId failed, reason: $failedReason, running on executor $executorId
         """.stripMargin
    }
}

case class ExecutorRemovedEvent(
     appId: String,
     level: String,
     override val event: AppEvent.Event,
     executorId: String,
     host: String,
     elapseTime: Long,
     completedTasks: Int,
     failedTask: Int,
     inputBytes: Long,
     shuffleReadBytes: Long,
     averageHeapMemory: Long,
     maxHeapMemory: Long) extends AppInfo(appId, level, event) {

    override def toJSONInfo: JSONObject = {
        val json = new JSONObject()
        json.put("appId", appId)
        json.put("executorId", executorId)
        json.put("host", host)
        json.put("elapseTime", elapseTime)
        json.put("completedTasks", completedTasks)
        json.put("failedTask", failedTask)
        json.put("inputBytes", inputBytes)
        json.put("shuffleReadBytes", shuffleReadBytes)
        json.put("averageHeapMemory", averageHeapMemory)
        json.put("maxHeapMemory", maxHeapMemory)

        json
    }

    override def toConsoleInfo: String = {
        s"""$level: executor_$executorId, host: $host, completedTasks: $completedTasks, failedTask: $failedTask,
            inputBytes: ${MetricsUtils.convertUnit(inputBytes)},
            shuffleReadBytes: ${MetricsUtils.convertUnit(shuffleReadBytes)},
            average heap memory: ${MetricsUtils.convertUnit(averageHeapMemory)},
            max heap memory: ${MetricsUtils.convertUnit(maxHeapMemory)}
         """.stripMargin
    }
}

case class StageDiagnosisInfo(
        appId: String,
        level: String,
        override val event: AppEvent.Event,
        diagnosisInfo: String) extends AppInfo(appId, level, event) {
    override def toConsoleInfo: String = {
        s"""$level: $diagnosisInfo"""
    }
}

case class TaskDiagnosisInfo(
       appId: String,
       level: String,
       override val event: AppEvent.Event,
       diagnosisInfo: String) extends AppInfo(appId, level, event) {
    override def toConsoleInfo: String = {
        s"""$level: $diagnosisInfo"""
    }
}

case class ExecutorMonitorInfo(
        appId: String,
        level: String,
        override val event: AppEvent.Event,
        info: String) extends AppInfo(appId, level, event) {
    override def toConsoleInfo: String = {
        s"""$level: $info"""
    }
}
