package com.example.blockchain.sql

import com.example.blockchain.schemas.TokenTransfers.colNameBlockDate

object DML {

	def dailyBalancesDeletePartition(icebergTable: String,
																	partitionValue: String
																 ): String = {
		// If `date` = x, then `BlockDate` may be either x or x+1
		// +----------+-------------+-------+
		//|date      |block_ts_date|cnt    |
		//+----------+-------------+-------+
		//|2025-09-10|2025-09-10   |10000  |
		//|2025-09-11|2025-09-11   |6344   |
		//|2025-09-11|2025-09-12   |3656   |
		//|2025-09-12|2025-09-12   |2601666|
		//|2025-09-12|2025-09-13   |424518 |
		//+----------+-------------+-------+
		f"""
			DELETE FROM  $icebergTable
	   	WHERE $colNameBlockDate >= DATE('$partitionValue')
		"""
	}

}
