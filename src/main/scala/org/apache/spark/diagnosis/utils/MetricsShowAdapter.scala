package org.apache.spark.diagnosis.utils

import java.sql.Timestamp

import org.apache.spark.diagnosis.heuristic.{ResultDetail, ResultLevel}
import org.apache.spark.SparkConf

/**
  * @author futao
  * create at 2018/9/10
  */
trait MetricsSinkAdapter {
	
	def showMetrics(resultDetail: ResultDetail)
}


class StdOutMetricsShow extends MetricsSinkAdapter {
	
	def info(level:String, message: String): Unit ={
		val time = new Timestamp(System.currentTimeMillis)
		println(s"$time $level: $message")
	}
	
	override def showMetrics(resultDetail: ResultDetail): Unit = {
		resultDetail.level match {
			case ResultLevel.INFO => info("INFO", resultDetail.message)
			case ResultLevel.WARN => info("WARN", resultDetail.message)
			case ResultLevel.ERROR => info("ERROR", resultDetail.message)
		}
	}
}

object MetricsSinkFactory {
	
	var metricsSink: MetricsSinkAdapter = _
	
	def initMetricsShowAdapter(conf: SparkConf): Unit = {
		if (metricsSink == null) {
			val adapter = conf.get("spark.metrics.adapter", "stdout")
			adapter match {
				case "stdout" => metricsSink = new StdOutMetricsShow
				case _ => throw new RuntimeException("unsupported metrics adapter")
			}
		}
	}
}