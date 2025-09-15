package com.example.blockchain.sql

import com.example.blockchain.schemas.TokenTransfers._

object DDL {

	def createTableDailyBalances(icebergtable: String): String =
		(f"""
			|CREATE TABLE $icebergtable (
			|  $colNameTokenAddress   STRING,
			|  $colNameWalletAddress  STRING,
			|  $colNameDate            DATE,
			|  $colNameDailyBalance   DECIMAL(38, 4)
			|)
			|USING iceberg
			|PARTITIONED BY ($colNameDate)
  """.stripMargin)

	def dropIcebergTable(icebergtable: String): String = {
		f"DROP TABLE IF EXISTS $icebergtable"
	}


}
