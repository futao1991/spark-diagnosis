package org.apache.spark.diagnosis.schedule

import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.diagnosis.data.StageMetricsData
import org.apache.spark.diagnosis.heuristic.StageHeuristic
import org.apache.spark.internal.Logging

class JobGroupMonitor(appId: Option[String]) extends Runnable with Logging {

	val running = new AtomicBoolean(true)

	def stopMonitor: Boolean = running.getAndSet(false)

	override def run(): Unit = {
		logInfo(s"start to monitor jobs under application {$appId}...")
		
		try {
			while (running.get()) {
				for (stageId <- StageMetricsData.stageInfoList.keySet) {
					val stageInfo = StageMetricsData.stageInfoList(stageId)
					//该stage处于运行状态中，并且已经完成一半的task
					if (stageInfo.submissionTime.isDefined && stageInfo.completionTime.isEmpty) {
						val finishTask = StageMetricsData.stageTaskNumMap.getOrElse(stageId, 0)
						if (finishTask * 1.0 / stageInfo.numTasks >= 0.5) {
							StageHeuristic.evaluate(stageId, onlyCheckRunning = true)
						}
					}
				}
				Thread.sleep(3000)
			}
		} catch {
			case e: Exception => logError(s"monitor jobGroup $appId error ", e)
		}
		logInfo(s"monitor jobs under application {$appId} finished.")
	}
}


/**
  * @author futao
  * create at 2018/9/10
  */
object JobGroupScheduleFactory {

	private val scheduledService: ExecutorService = Executors.newCachedThreadPool()
	
	def addMonitor(appId: Option[String]): Unit ={
		val monitor = new JobGroupMonitor(appId)
		scheduledService.submit(monitor)
	}
}
