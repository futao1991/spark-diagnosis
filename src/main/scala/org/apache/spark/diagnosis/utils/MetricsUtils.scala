package org.apache.spark.diagnosis.utils

import java.text.DecimalFormat
import java.util
import org.apache.spark.SparkContext

/**
  * @author futao
  * create at 2018/9/10
  */
object MetricsUtils {

	private val df = new DecimalFormat("#.00")
	
	def convertUnit(memory:Long): String ={
		if (memory < 1024 ) s"$memory B"
		else if (memory < 1024 * 1024) s"${df.format(memory/1024.0)} KB"
		else if (memory < 1024 * 1024 * 1024) s"${df.format(memory/(1024.0*1024))} MB"
		else if (memory < 1024L * 1024 * 1024 * 1024) s"${df.format(memory/(1024.0*1024*1024))} GB"
		else s"${df.format(memory/(1024.0*1024*1024*1024))} TB"
	}
	
	def convertTimeUnit(time: Long): String = {
		
		if (time < 1000) {
			return s"$time ms"
		}
		
		val ss = 1000
		val mi = ss * 60
		val hh = mi * 60
		val dd = hh * 24
		
		val day = time / dd
		val hour = (time - day * dd) / hh
		val minute = (time - day * dd - hour * hh) / mi
		val second = (time - day * dd - hour * hh - minute * mi) / ss

		val sb = new StringBuffer()
		if (day > 0) {
			sb.append(day + " day ")
		}
		if (hour > 0) {
			sb.append(hour + " hour ")
		}
		if (minute > 0) {
			sb.append(minute + " mins ")
		}
		if (second > 0) {
			sb.append(second + " secs ")
		}
		sb.toString
	}
	
	
	def getSparkConfig(sparkContext: SparkContext): util.Map[String, Any] ={
		val conf = sparkContext.getConf
		val driverMemory = conf.getSizeAsBytes("spark.driver.memory", "1073741824")
		val executorMemory = conf.getSizeAsBytes("spark.executor.memory", "1073741824")
		val executorCores = conf.getInt("spark.executor.cores", 1)
		val dynamicAllocationEnable = conf.getBoolean("spark.dynamicAllocation.enabled", false)
		val maxDynamicExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", 5)
		val executorInstances = conf.getInt("spark.executor.instances", 5)
		val shufflePartitions = conf.getInt("spark.sql.shuffle.partitions", 200)

		val map = new util.HashMap[String, Any]()
		map.put("driverMemory", driverMemory)
		map.put("executorMemory", executorMemory)
		map.put("executorCores", executorCores)
		map.put("shufflePartitions", shufflePartitions)

        if (dynamicAllocationEnable) {
			map.put("maxDynamicExecutors", maxDynamicExecutors)
        } else {
			map.put("executorInstances", executorInstances)
        }
		map
	}
}
