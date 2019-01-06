package org.apache.spark.diagnosis.utils

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory


/**
  * @author futao
  * create at 2018/9/21
  */
object HttpClient {

	private val logger = LoggerFactory.getLogger(this.getClass)

	private val requestConfig = RequestConfig.custom()
			.setSocketTimeout(5000)
			.setConnectTimeout(5000)
			.setConnectionRequestTimeout(5000)
			.build()
	
	private val client = HttpClientBuilder.create()
			.setDefaultRequestConfig(requestConfig)
			.setConnectionManager(new PoolingHttpClientConnectionManager)
			.build()

	def sendGet(url: String): String ={
		var response: CloseableHttpResponse = null
		try {
			val httpGet = new HttpGet(url)
			response = client.execute(httpGet)
			val resEntity = response.getEntity
			EntityUtils.toString(resEntity)
		} catch {
			case e:Exception => logger.error(s"send get request $url error: ${e.getMessage}")
				""
		} finally {
			try {
				if (response != null) {
					EntityUtils.consume(response.getEntity)
				}
			} catch {
				case _:Exception =>
			}
		}
	}
}
