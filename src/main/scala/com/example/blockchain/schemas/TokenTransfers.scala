package com.example.blockchain.schemas

import org.apache.spark.sql.types._


object TokenTransfers {

	val colNameTokenAddress: String = "token_address"
	val colNameFromAddress: String = "from_address"
	val colNameToAddress: String = "to_address"
	val colNameValue: String = "value"
//	val colNameTransactionHash: String = "transaction_hash"
//	val colNameLogIndex: String = "log_index"
//	val colNameBlockNumber: String = "block_number"
//	val colNameBlockHash: String = "block_hash"
	val colNameBlockTimestamp: String = "block_timestamp"
	val colNameBlockDate: String = "block_date"
	val colNameDate: String = "date"
	val dateFormat: String = "yyyy-MM-dd"
//	val colNameLastModified: String = "last_modified"
	val colNameBalancesMap = "balances_map"

	val tokenTransfersSchema = StructType(Seq(
		StructField(colNameTokenAddress, StringType, nullable = true),
		StructField(colNameFromAddress, StringType, nullable = true),
		StructField(colNameToAddress, StringType, nullable = true),
		StructField(colNameValue, DoubleType, nullable = true),
//		StructField(colNameTransactionHash, StringType, nullable = true),
//		StructField(colNameLogIndex, LongType, nullable = true),
//		StructField(colNameBlockNumber, LongType, nullable = true),
//		StructField(colNameBlockHash, StringType, nullable = true),
		StructField(colNameBlockTimestamp, TimestampType, nullable = true),
		StructField(colNameDate, DateType, nullable = true)
//		StructField(colNameLastModified, TimestampType, nullable = true)
	))

	val colNameWalletAddress: String =  "wallet_address"
	val colNameDailyBalance: String = "daily_balance"
}
