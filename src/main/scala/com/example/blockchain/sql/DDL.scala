package com.example.blockchain.sql

import com.example.blockchain.schemas.TokenTransfers._

object DDL {

	def createTableDailyBalances(catalog: String,
	database: String,
	table: String
	): String =
		(f"""
			|CREATE TABLE $catalog.$database.$table (
			|  $colNameTokenAddress   STRING,
			|  $colNameWalletAddress  STRING,
			|  $colNameDate            DATE,
			|  $colNameDailyBalance   DECIMAL(38, 4)
			|)
			|USING iceberg
			|PARTITIONED BY ($colNameDate)
  """.stripMargin)

	def dropIcebergTable(catalog: String,
											 database: String,
											 table: String
											): String = {
		f"DROP TABLE IF EXISTS $catalog.$database.$table"
	}


}
