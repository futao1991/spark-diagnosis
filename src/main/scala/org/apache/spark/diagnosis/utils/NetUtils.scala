package org.apache.spark.diagnosis.utils

import java.net.{Inet6Address, InetAddress, NetworkInterface}

import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * @author futao
  * create at 2018/11/17
  */
object NetUtils {

	def getEthernetAddress: String = {
		val e1 = NetworkInterface.getNetworkInterfaces
		val ipAddressMap = new mutable.HashMap[String, Option[String]] ()
		while (e1.hasMoreElements) {
			val ni = e1.nextElement()
			if (StringUtils.startsWith(ni.getName, "eth")) {
				val e2 = ni.getInetAddresses
				while (e2.hasMoreElements) {
					val ia = e2.nextElement()
					if (!ia.isInstanceOf[Inet6Address]) {
						ipAddressMap(ni.getName) = Some(ia.getHostAddress)
					}
				}
			}
		}
		var ipAddress: Option[String] = None
		for (key <- ipAddressMap.keySet) {
			if (ipAddress.isEmpty) {
				ipAddress = ipAddressMap(key)
			}
		}
		ipAddress.getOrElse(InetAddress.getLocalHost.getHostAddress)
	}
}
