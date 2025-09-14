package com.example.blockchain.sql

object DML {

	def dailyBalancesDeletePartition(catalog: String,
																	database: String,
																	table: String,
																	partitionValue: String
																 ): String = {

		f"""
			DELETE FROM  $catalog.$database.$table
	   	WHERE date = DATE('$partitionValue')
		"""
	}

}
