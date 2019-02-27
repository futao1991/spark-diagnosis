package org.apache.spark.diagnosis.utils

import java.sql.Timestamp
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.diagnosis.data.AppInfo

/**
  * @author futao
  * create at 2018/9/10
  */
trait MetricsSinkAdapter {
	def showMetrics(appInfo: AppInfo)
}

class StdOutMetricsShow extends MetricsSinkAdapter {
	
    override def showMetrics(appInfo: AppInfo): Unit ={
        val time = new Timestamp(System.currentTimeMillis)
        println(s"$time ${appInfo.toConsoleInfo}")
	}
}

class MetaServerMetrics(metaServerUrl: String) extends MetricsSinkAdapter {

    override def showMetrics(appInfo: AppInfo): Unit = {
        if (StringUtils.isNotEmpty(metaServerUrl)) {
            val url = s"http://$metaServerUrl/metaserver/eventserver/v1/send"
            val map = new util.HashMap[String, AnyRef]()
            map.put("channel", s"CHANNEL@SPARK@${appInfo.event}")
            map.put("event", appInfo.toJSONInfo)
            HttpClient.sendPost(url, map)
        }
    }
}

object MetricsSinkFactory {

    var metaServerUrl: String = _

	var logMetricsSink: MetricsSinkAdapter = _

    var metaServerMetricsSink: MetricsSinkAdapter = _

    def getLogMetricsSink: MetricsSinkAdapter = {
        if (null == logMetricsSink) {
            logMetricsSink = new StdOutMetricsShow
        }
        logMetricsSink
    }

    def getMetaServerMetricsSink: MetricsSinkAdapter = {
        if (null == metaServerMetricsSink) {
            metaServerMetricsSink = new MetaServerMetrics(metaServerUrl)
        }
        metaServerMetricsSink
    }
}