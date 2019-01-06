package org.apache.spark.diagnosis.errordiagnosis

import org.apache.commons.lang3.StringUtils


/**
  * @author futao
  * create at 2018/10/15
  */
object FailedReasonChecker {

	def checkFailedReason(errMsg: String): String ={
		
		val failedType = getFailedType(errMsg)
		if (null != failedType) {
			return s"reason: ${failedType.reason}, solutions: ${failedType.solution}"
		}
		""
	}
	
	private def getFailedType(errMsg: String): FailedReasonType ={

		if (StringUtils.contains(errMsg, "Exit code is 143")) {
			return FailedReasonType.TaskOOM
		}
		
		if (StringUtils.contains(errMsg, "spark.shuffle.FetchFailedException") &&
				StringUtils.contains(errMsg, "Failed to connect to")) {
			return FailedReasonType.ShuffleFetcherError
		}

		if (StringUtils.contains(errMsg, "ExecutorLostFailure")) {
			return FailedReasonType.ExecutorLost
		}
		
		null
	}
}
