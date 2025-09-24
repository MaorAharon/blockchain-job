package com.example.blockchain.sql

import com.example.blockchain.schemas.TokenTransfers._

object DDL {

	def createTableDailyBalances(icebergtable: String): String =
		(f"""
			|CREATE TABLE $icebergtable (
			|  $colNameTokenAddress   STRING,
			|  $colNameWalletAddress  STRING,
			|  $colNameBlockDate            DATE,
			|  $colNameDailyBalance   DECIMAL(38, 4)
			|)
			|USING iceberg
			|PARTITIONED BY ($colNameBlockDate)
  """.stripMargin)

	def dropIcebergTable(icebergtable: String): String = {
		f"DROP TABLE IF EXISTS $icebergtable"
	}

	def createTableWalletBalances(icebergTable: String): String =
		s"""
			 |CREATE TABLE $icebergTable (
			 |  $colNameWalletAddress STRING,
			 |  $colNameBlockDate DATE,
			 |  $colNameBalancesMap MAP<STRING, DECIMAL(38,4)>
			 |)
			 |USING iceberg
			 |PARTITIONED BY ($colNameBlockDate)
   """.stripMargin
}
