package org.apache.spark.diagnosis.utils

import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.ByteArrayEntity
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
			.setSocketTimeout(50000)
			.setConnectTimeout(5000)
			.setConnectionRequestTimeout(5000)
			.build()
	
	private val client = HttpClientBuilder.create()
			.setDefaultRequestConfig(requestConfig)
			.setConnectionManager(new PoolingHttpClientConnectionManager)
			.build()

    private val objectMapper = new ObjectMapper

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

	def sendPost(url: String, params: util.Map[String, AnyRef]): String = {
        var response: CloseableHttpResponse = null
        try {
            val httpPost = new HttpPost(url)
            httpPost.setHeader("Content-Type", "application/json;charset=UTF-8")
            val entity = new ByteArrayEntity(objectMapper.writeValueAsBytes(params))
            httpPost.setEntity(entity)
            response = client.execute(httpPost)
            val resEntity = response.getEntity
            EntityUtils.toString(resEntity)
        } catch {
            case e:Exception =>  logger.error(s"send post request $url error: ", e)
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
