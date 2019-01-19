package org.apache.spark.diagnosis.status

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.util.ThreadStackTrace

/**
  * @author futao
  * create at 2018/9/10
  */
object InternalStatusUtils {

	private var appStatusStore: AppStatusStore = _

	var sc: SparkContext = _

	def getAppStatusStore(sparkContext: SparkContext): AppStatusStore = {
		if (appStatusStore == null) {
			val clazz = classOf[SparkContext]
			val field = clazz.getDeclaredField("_statusStore")
			field.setAccessible(true)
			appStatusStore = field.get(sparkContext).asInstanceOf[AppStatusStore]
			sc = sparkContext
		}
		appStatusStore
	}

	def getTaskSummary(stageId: Int, stageAttemptId: Int): Option[v1.TaskMetricDistributions] ={
		val array = Array(0, 0.25, 0.5, 0.75, 1.0)
		appStatusStore.taskSummary(stageId, stageAttemptId, array)
	}

	def getTaskList(stageId: Int, stageAttemptId: Int): Seq[v1.TaskData] = {
		appStatusStore.taskList(stageId, stageAttemptId, 0, Int.MaxValue, v1.TaskSorting.DECREASING_RUNTIME)
	}

	def getExecutorThread(executorId: String, taskId: Long): String ={
		val clazz = classOf[SparkContext]
		val method = clazz.getDeclaredMethod("getExecutorThreadDump", classOf[String])
		method.setAccessible(true)
		val stackTraceArray = method.invoke(sc, executorId).asInstanceOf[Option[Array[ThreadStackTrace]]]
		stackTraceArray.map( array => {
			array.filter(stack => stack.threadName.equals(s"Executor task launch worker for task $taskId")).map(_.stackTrace)
		}).getOrElse(Array[String]()).mkString(";")
	}
	
	def getExecutor(executorId: String): v1.ExecutorSummary ={
		try {
			appStatusStore.executorSummary(executorId)
		} catch {
			case _: Exception => null
		}
	}
}
