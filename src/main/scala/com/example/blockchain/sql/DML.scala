package com.example.blockchain.sql

object DML {

	def dailyBalancesDeletePartition(icebergTable: String,
																	partitionValue: String
																 ): String = {

		f"""
			DELETE FROM  $icebergTable
	   	WHERE date = DATE('$partitionValue')
		"""
	}

}
