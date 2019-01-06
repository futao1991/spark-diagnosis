package org.apache.spark.diagnosis.errordiagnosis

/**
  * @author futao
  * create at 2018/10/15
  */
sealed class FailedReasonType(errorType: String) {
	
	def reason : String = null
	
	def solution : String = null
}

object FailedReasonType {
	
	object TaskOOM extends FailedReasonType("OOM") {

		override def reason: String = {
			"data skew or too large"
		}
		
		override def solution: String = {
			"check if data is skewed or boost spark.executor.memoryOverhead"
		}
	}
	
	object ShuffleFetcherError extends FailedReasonType("Shuffle Fetcher ERROR") {

		override def reason: String = {
			"shuffle read data too large"
		}

		override def solution: String = {
			s"""1) boost spark.executor.memory 2) boost spark.executor.cores
	            3) boost spark.network.timeout 4) check if data is skewed
			    5) avoid to use shuffle transformations if possible
			 """.stripMargin
		}
	}
	
	object ExecutorLost extends FailedReasonType("ExecutorLostFailure") {

		override def reason: String = {
			"network problem or task gc overhead"
		}

		override def solution: String = {
			"boost spark.network.timeout"
		}
	}
}