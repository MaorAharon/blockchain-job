package com.example.blockchain.parser

import com.example.blockchain.parameters.TokenBalanceJobParameters
import scopt.OptionParser
import java.sql.Date

import java.time.{ZoneId, ZonedDateTime}

object TokenBalanceJobParametersParser
	extends OptionParser[TokenBalanceJobParameters]("Token Balance Job Application") {

	private def nonEmpty(s: String): Either[String, Unit] =
		if (s.trim.nonEmpty) Right(()) else Left(s"can't be empty, got = [$s]")

	implicit val dateRead: scopt.Read[Date] = scopt.Read.reads { s =>
		// expects yyyy-MM-dd
		Date.valueOf(s.trim)
	}

	opt[Seq[Date]]("unprocessed-hours").action { (hours, p) =>
		p.copy(unprocessedHours = hours)
	}
		.text("Comma-separated list of dates in yyyy-MM-dd format")

	opt[String]("spark-log-level")
		.action((x, p) => p.copy(sparkLogLevel = x))
		.text("sparkLogLevel")


	opt[String]("bucket")
		.action((x, p) => p.copy(bucket = x))
		.text("Input bucket")

	opt[String]("prefix")
		.action((x, p) => p.copy(prefix = x))
		.text("Input prefix")

	opt[String]("iceberg-catalog")
		.action((x, p) => p.copy(icebergCatalog = x))
		.text("Iceberg catalog")

	opt[String]("db")
		.action((x, p) => p.copy(db = x))
		.text("Database name")

	opt[String]("table")
		.action((x, p) => p.copy(table = x))
		.text("Table name")

	opt[Boolean]("should-reinitialize-token-transfers").action { (x, p) => p.copy(shouldReinitializeTokenTransfers = x) }
}
