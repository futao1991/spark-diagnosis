package org.apache.spark.diagnosis.utils

import java.io.OutputStreamWriter
import java.net.Socket
import java.sql.Timestamp
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.IOUtils
import org.apache.spark.diagnosis.data.AppInfo
import org.slf4j.LoggerFactory

/**
  * @author futao
  * create at 2018/9/10
  */
trait MetricsSinkAdapter {

	def showMetrics(appInfo: AppInfo)

    def close()
}

class StdOutMetricsShow extends MetricsSinkAdapter {
	
    override def showMetrics(appInfo: AppInfo): Unit ={
        val time = new Timestamp(System.currentTimeMillis)
        println(s"$time ${appInfo.toConsoleInfo}")
	}

    override def close(): Unit = {

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

    override def close(): Unit = {

    }
}

class GraphiteMetrics(host:String, port:Int) extends MetricsSinkAdapter {

    private val logger = LoggerFactory.getLogger(this.getClass)

    private val socket = new Socket(host, port)

    private val writer = new OutputStreamWriter(socket.getOutputStream)

    override def showMetrics(appInfo: AppInfo): Unit = {
        try {
            if (null != appInfo.toGraphiteInfo) {
                writer.write(appInfo.toGraphiteInfo)
                writer.flush()
            }
        } catch {
            case e: Exception =>
                logger.error(s"send message to graphite error", e)
        }
    }

    override def close(): Unit = {
        IOUtils.closeStream(writer)
    }
}


object MetricsSinkFactory {

    private val logger = LoggerFactory.getLogger(this.getClass)

    var metaServerUrl: String = _

    var graphiteHost: String = _

    var graphitePort: Int = 0

	var logMetricsSink: MetricsSinkAdapter = _

    var sinkType: String = _

    var metricsSink: MetricsSinkAdapter = _

    def initSink(): Unit = {
        logMetricsSink = new StdOutMetricsShow

        metricsSink = if ("metaServer".equalsIgnoreCase(sinkType)) {
            if (StringUtils.isEmpty(metaServerUrl)) {
                logger.error("meta server url is empty!")
                null
            } else {
                new MetaServerMetrics(metaServerUrl)
            }
        } else if ("graphite".equalsIgnoreCase(sinkType)) {
            if (StringUtils.isEmpty(graphiteHost) || graphitePort <= 0) {
                logger.error("graphite host or port is empty!")
                null
            } else {
                try {
                    new GraphiteMetrics(graphiteHost, graphitePort)
                } catch {
                    case e: Exception =>
                        logger.error("init graphite sink error", e)
                        null
                }
            }
        } else {
            null
        }
    }

    def showMetrics(appInfo: AppInfo): Unit = {
        if (null != metricsSink) {
            metricsSink.showMetrics(appInfo)
        }
    }

    def close(): Unit = {
        if (null != metricsSink) {
            metricsSink.close()
        }
    }

    def printLog(appInfo: AppInfo): Unit = {
        if (null != logMetricsSink) {
            logMetricsSink.showMetrics(appInfo)
        }
    }

}