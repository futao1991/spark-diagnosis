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

sealed abstract class AppInfo(val appId: String, level: String, val event: AppEvent.Event) {

    def toJSONInfo: JSONObject = null

    def toConsoleInfo: String = null

    def toGraphiteInfo: String = null
}

case class AppStartEvent(
     override val appId: String,
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

    override def toGraphiteInfo: String = {
        val lines = new StringBuilder
        val prefix = s"${appId}_$event"
        val timestamp = System.currentTimeMillis() / 1000L
        lines.append(s"${prefix}_submitTime").append(" ").append(submitTime)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_startTime").append(" ")
                .append(startTime).append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_user").append(" ")
                .append(user).append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_queue").append(" ")
                .append(queue).append(" ").append(timestamp).append("\n")
        lines.toString()
    }
}

case class AppEndEvent(
     override val appId: String,
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

    override def toGraphiteInfo: String = {
        val lines = new StringBuilder
        val prefix = s"${appId}_$event"
        val timestamp = System.currentTimeMillis() / 1000L
        lines.append(s"${prefix}_elapseTime").append(" ").append(elapseTime)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_memorySeconds").append(" ").append(memorySeconds)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_vCoreSeconds").append(" ").append(vCoreSeconds)
                .append(" ").append(timestamp).append("\n")
        lines.toString()
    }
}

case class StageCompletedEvent(
     override val appId: String,
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

    override def toGraphiteInfo: String = {
        val lines = new StringBuilder
        val prefix = s"${appId}_${event}_stage$stageId"
        val timestamp = System.currentTimeMillis() / 1000L
        lines.append(s"${prefix}_totalTask").append(" ").append(totalTask)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_inputBytes").append(" ").append(inputBytes)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_outputBytes").append(" ").append(outputBytes)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_shuffleReadBytes").append(" ").append(shuffleReadBytes)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_shuffleWriteBytes").append(" ").append(shuffleWriteBytes)
                .append(" ").append(timestamp).append("\n")
        lines.toString()
    }
}

case class TaskFailedEvent(
     override val appId: String,
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

    override def toGraphiteInfo: String = {
        val lines = new StringBuilder
        val prefix = s"${appId}_${event}_stage${stageId}_$taskId"
        val timestamp = System.currentTimeMillis() / 1000L
        lines.append(s"${prefix}_executorId").append(" ").append(executorId)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_failedReason").append(" ").append(failedReason)
                .append(" ").append(timestamp).append("\n")
        lines.toString()
    }
}

case class ExecutorRemovedEvent(
      override val appId: String,
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

    override def toGraphiteInfo: String = {
        val lines = new StringBuilder
        val prefix = s"${appId}_${event}_executor$executorId"
        val timestamp = System.currentTimeMillis() / 1000L
        lines.append(s"${prefix}_host").append(" ").append(host)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_elapseTime").append(" ").append(elapseTime)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_completedTasks").append(" ").append(completedTasks)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_failedTask").append(" ").append(failedTask)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_inputBytes").append(" ").append(inputBytes)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_shuffleReadBytes").append(" ").append(shuffleReadBytes)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_averageHeapMemory").append(" ").append(averageHeapMemory)
                .append(" ").append(timestamp).append("\n")
        lines.append(s"${prefix}_maxHeapMemory").append(" ").append(maxHeapMemory)
                .append(" ").append(timestamp).append("\n")
        lines.toString()
    }
}

case class StageDiagnosisInfo(
        override val appId: String,
        level: String,
        override val event: AppEvent.Event,
        diagnosisInfo: String) extends AppInfo(appId, level, event) {
    override def toConsoleInfo: String = {
        s"""$level: $diagnosisInfo"""
    }
}

case class TaskDiagnosisInfo(
       override val appId: String,
       level: String,
       override val event: AppEvent.Event,
       diagnosisInfo: String) extends AppInfo(appId, level, event) {
    override def toConsoleInfo: String = {
        s"""$level: $diagnosisInfo"""
    }
}

case class ExecutorMonitorInfo(
        override val appId: String,
        level: String,
        override val event: AppEvent.Event,
        info: String) extends AppInfo(appId, level, event) {
    override def toConsoleInfo: String = {
        s"""$level: $info"""
    }
}
