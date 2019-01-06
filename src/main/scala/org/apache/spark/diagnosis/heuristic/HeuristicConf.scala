package org.apache.spark.diagnosis.heuristic

/**
  * @author futao
  * create at 2018/9/10
  * 诊断的阈值
  */
object HeuristicConf {

	/**
	  * 判断stage中的task时间分布
	  */
	val STAGE_TASK_DISTRIBUTION_GAP = 0.6

	/**
	  * 判断stage中的task shuffle read 分布
	  */
	val STAGE_TASK_SHUFFLE_SKEW_GAP = 0.4

	/**
	  * 判断stage中的task input read 分布
	  */
	val STAGE_TASK_INPUT_SKEW_GAP = 0.6

	/**
	  * stage中的gc time阈值
	  */
	val STAGE_TASK_GCTIME_GAP = 20000

	/**
	  * stage中的gc ratio阈值
	  */
	val STAGE_TASK_GCRATIO_GAP = 0.7

	/**
	  * stage中的task scheduler delay 阈值
	  */
	val STAGE_TASK_SCHEDULER_DELAY_GAP = 0.2
}
