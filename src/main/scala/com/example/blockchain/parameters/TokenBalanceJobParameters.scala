package com.example.blockchain.parameters

import java.sql.Date

case class TokenBalanceJobParameters(
																			unprocessedHours: Seq[Date] = Seq(Date.valueOf("2025-09-10"), Date.valueOf("2025-09-11")),
																			sparkLogLevel: String = "INFO",
																			bucket: String = "file:///home/maor/Documents/git/scala/blockchain-job/src/test/resources",
																			prefix: String = "token_transfers",
																			icebergCatalog: String = "local",
																			db: String = "db",
																			table: String = "daily_balances",
																			shouldReinitializeTokenTransfers: Boolean = true

																		) {
	lazy val inputPath: String = s"$bucket/$prefix/"
	lazy val dailyBalancesTable: String = s"$icebergCatalog.$db.$table"

	override def toString: String =
		s"""<<<<< TokenBalanceJobParameters >>>>>
			 |unprocessedHours: ${unprocessedHours.mkString(",")}
			 |sparkLogLevel: $sparkLogLevel
			 |bucket: $bucket
			 |prefix: $prefix
			 |inputPath: $inputPath
			 |icebergCatalog: $icebergCatalog
			 |db: $db
			 |table: $table
			 |shouldReinitializeTokenTransfers: $shouldReinitializeTokenTransfers
			 |""".stripMargin
}
