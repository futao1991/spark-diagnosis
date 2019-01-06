package cn.fraudmetrix.spark.metrics

import java.util
import java.util.Random
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.ConcurrentHashMap

import com.uber.profiling.util.JsonUtils

import scala.collection.JavaConversions._
import org.eclipse.jetty.server.Server
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.ServletContextHandler
import org.slf4j.LoggerFactory

/**
  * @author futao
  * create at 2018/11/19
  */
class ReporterServlet extends HttpServlet {
	
	override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
		resp.setContentType("application/json;charset=utf-8")
		resp.setStatus(HttpServletResponse.SC_OK)
		resp.getWriter.write(JsonUtils.serialize(ReporterServer.metricsInfo))
	}
}


object ReporterServer {

	private val logger = LoggerFactory.getLogger(classOf[ReporterServlet])
	
	val reporterServlet = new ReporterServlet
	
	val random = new Random
	
	val port = new AtomicInteger(random.nextInt(65535))
	
	val ready = new AtomicBoolean(false)

	val metricsInfo = new ConcurrentHashMap[String, AnyRef]()

	def updateInfo(metrics: util.Map[String, AnyRef]): Unit ={
		metricsInfo.clear()
		for (key <- metrics.keySet()) {
			metricsInfo.put(key, metrics.get(key))
		}
	}
	
	private val cachedThreadPool = Executors.newSingleThreadExecutor()
	cachedThreadPool.execute(new Runnable {
		override def run(): Unit = {
			def startServer: Server = {
				val server = new Server(port.get())
				val context = new ServletContextHandler(server, "/")
				context.addServlet(classOf[ReporterServlet], "/executor")
				server.setHandler(context)
				server.start()
				server
			}

			var server: Server = null
			var launcherCount = 0
			while (launcherCount < 10) {
				try {
					server = startServer
					launcherCount = 10
				} catch {
					case e: Exception => logger.error(s"launcher server error: ${e.getMessage}")
						port.getAndSet(random.nextInt(65535))
				} finally {
					launcherCount += 1
				}
			}

			if (null != server) {
				ready.getAndSet(true)
				logger.info(s"server stared at port ${port.get()}")
				server.start()
			}
		}
	})
}
