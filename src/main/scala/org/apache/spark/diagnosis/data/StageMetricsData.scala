package org.apache.spark.diagnosis.data

import org.apache.spark.scheduler.{SparkListenerTaskEnd, StageInfo}
import scala.collection.mutable

/**
  * @author futao
  * create at 2018/9/10
  * 存储stage的采集信息
  */
object StageMetricsData {

	/**
	  * stage信息列表
	  */
	val stageInfoList = new mutable.HashMap[Int, StageInfo]()

	/**
	  * 当前stage已经完成的task数量
	  */
	val stageTaskNumMap = new mutable.HashMap[Int, Int]()

	/**
	  * stage数据倾斜标记
	  */
	val stageSkewFlag = new mutable.HashMap[Int, String]()

	/**
	  * stage运行中的数据(input和shuffle read)
	  */
	val stageRunningData = new mutable.HashMap[Int, (Long, Long)]()

	/**
	  * executor列表
	  */
	val executorMap = new mutable.HashMap[String, String]()

	/**
	  * 失败的task列表
	  */
	val taskFailedMap = new mutable.HashMap[Long, SparkListenerTaskEnd]()

	def clearCache(stageId: Int): Unit ={
		stageInfoList.remove(stageId)
		stageTaskNumMap.remove(stageId)
		stageSkewFlag.remove(stageId)
		stageRunningData.remove(stageId)
	}
}
