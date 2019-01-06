package org.apache.spark.diagnosis.utils

import org.apache.commons.lang3.StringUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._
/**
  * @author futao
  * create at 2018/9/21
  */
object PrometheusUtils {

	def getIpAddressByHostName(url: String, hostName: String): String ={
		
		val queryUrl = s"""$url/api/v1/query?query=node_uname_info%7Bnodename%3D"$hostName"%7D"""
		val response = HttpClient.sendGet(queryUrl)
		if (StringUtils.isEmpty(response)) {
			return null
		}

		val json = parse(response)
		val jsonMap = json.values.asInstanceOf[Map[String, _]]
		
		val status = jsonMap("status")
		if ("success".equals(status)) {
			val data = jsonMap("data").asInstanceOf[Map[String, _]]
			val result = data("result").asInstanceOf[List[_]]
			if (result.nonEmpty) {
				val metric = result.head.asInstanceOf[Map[String, _]]("metric").asInstanceOf[Map[String, _]]
				return metric("instance").toString
			}
		}
		""
	}
	
	
	def getLoadAverageByHostName(url: String, hostName: String): (String, String, String) ={
		
		def getLoadAverageByHostName(url: String, ipAddress: String, range: Int): String = {
			
			val queryUrl = s"$url/api/v1/query?query=node_load$range%7Binstance%3D%22$ipAddress%22%7D"
			val response = HttpClient.sendGet(queryUrl)
			if (StringUtils.isEmpty(response)) {
				return null
			}

			val json = parse(response)
			val jsonMap = json.values.asInstanceOf[Map[String, _]]

			val status = jsonMap("status")
			if ("success".equals(status)) {
				val data = jsonMap("data").asInstanceOf[Map[String, _]]
				val result = data("result").asInstanceOf[List[_]]
				if (result.nonEmpty) {
					val value = result.head.asInstanceOf[Map[String, _]]("value").asInstanceOf[List[_]]
					if (value.nonEmpty) {
						return value(1).toString
					}
				}
			}
			""
		}
		
		val ipAddress = getIpAddressByHostName(url, hostName)
		if (StringUtils.isNotEmpty(ipAddress)) {
			val load1 = getLoadAverageByHostName(url, ipAddress, 1)
			val load5 = getLoadAverageByHostName(url, ipAddress, 5)
			val load15 = getLoadAverageByHostName(url, ipAddress, 15)
			return (load1, load5, load15)
		}
		null
	}
	
	
	def getCpuUsageByHostName(url: String, hostName: String): Map[String, String] ={

		var resultMap = Map[String, String]()
		val ipAddress = getIpAddressByHostName(url, hostName)
		if (StringUtils.isNotEmpty(ipAddress)) {
			val queryUrl = s"""$url/api/v1/query?query=sum(rate(node_cpu%7Binstance%3D"$ipAddress"%7D%5B5s%5D))%20by%20(mode)%20*%20100%20%2F%20"""+
					s"""count(node_cpu%7Binstance%3D"$ipAddress"%7D)%20by%20(mode)%20or%20"""+
					s"""sum(irate(node_cpu%7Binstance%3D"$ipAddress"%7D%5B5m%5D))%20"""+
					s"""by%20(mode)%20*%20100%20%2F%20count(node_cpu%7Binstance%3D"$ipAddress"%7D)%20by%20(mode)"""
			val response = HttpClient.sendGet(queryUrl)
			if (StringUtils.isEmpty(response)) {
				return null
			}

			val json = parse(response)
			val jsonMap = json.values.asInstanceOf[Map[String, _]]

			val status = jsonMap("status")
			if ("success".equals(status)) {
				val data = jsonMap("data").asInstanceOf[Map[String, _]]
				val result = data("result").asInstanceOf[List[_]]
				if (null != result && result.nonEmpty) {
					for (metrics <- result) {
						val map = metrics.asInstanceOf[Map[String, _]]
						val mode = map("metric").asInstanceOf[Map[String, _]]("mode").toString
						mode match {
							case "system" | "idle" | "user" =>
								val value = map("value").asInstanceOf[List[_]](1).toString
								resultMap += (mode -> value)
							case _ =>	
						}
					}
				}
			}
		}
		resultMap
	}
	
	def getMemoryByHostName(url: String, hostName: String): Long ={
		val ipAddress = getIpAddressByHostName(url, hostName)
		if (StringUtils.isNotEmpty(ipAddress)) {
			val queryUrl = s"""$url/api/v1/query?query=node_memory_MemAvailable%7Binstance%3D"$ipAddress"%7D%20"""+
				   s"""or%20(node_memory_MemFree%7Binstance%3D"$ipAddress"%7D%20%2B%20""" +
				   s"""node_memory_Buffers%7Binstance%3D"$ipAddress"%7D%20%2B%20""" +
				   s"""node_memory_Cached%7Binstance%3D"$ipAddress"%7D)"""
			val response = HttpClient.sendGet(queryUrl)
			if (StringUtils.isNotEmpty(response)) {
				val json = parse(response)
				val jsonMap = json.values.asInstanceOf[Map[String, _]]

				val status = jsonMap("status")
				if ("success".equals(status)) {
					val data = jsonMap("data").asInstanceOf[Map[String, _]]
					val result = data("result").asInstanceOf[List[_]]
					if (null != result && result.nonEmpty) {
						val value = result.head.asInstanceOf[Map[String, _]]("value").asInstanceOf[List[_]]
						if (value.nonEmpty) {
							return value(1).toString.toLong
						}
					}
				}
			}
		}
		0L
	}
	
	
	def getIOActivityByHostName(url: String, hostName: String): (Double, Double) ={
		
		def getIORateByHostName(url: String, ipAddress: String, mode: String): Double ={
			val queryUrl = s"""$url/api/v1/query?query=rate(node_vmstat_pgpg$mode%7Binstance%3D%22$ipAddress%22%7D%5B5s%5D)%20*%201024"""+
				           s"""%20or%20irate(node_vmstat_pgpg$mode%7Binstance%3D%22$ipAddress%22%7D%5B5m%5D)%20*%201024"""
			val response = HttpClient.sendGet(queryUrl)
			if (StringUtils.isNotEmpty(response)) {
				val json = parse(response)
				val jsonMap = json.values.asInstanceOf[Map[String, _]]

				val status = jsonMap("status")
				if ("success".equals(status)) {
					val data = jsonMap("data").asInstanceOf[Map[String, _]]
					val result = data("result").asInstanceOf[List[_]]
					if (null != result && result.nonEmpty) {
						val value = result.head.asInstanceOf[Map[String, _]]("value").asInstanceOf[List[_]]
						if (value.nonEmpty) {
							return value(1).toString.toDouble
						}
					}
				}
			}
			0
		}

		val ipAddress = getIpAddressByHostName(url, hostName)
		if (StringUtils.isNotEmpty(ipAddress)) {
			val inRate = getIORateByHostName(url, ipAddress, "in")
			val outRate = getIORateByHostName(url, ipAddress, "out")
			return (inRate, outRate)
		}
		null
	}
}
