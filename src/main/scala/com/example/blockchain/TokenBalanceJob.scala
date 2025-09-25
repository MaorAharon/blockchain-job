package com.example.blockchain

import com.example.blockchain.parameters.TokenBalanceJobParameters
import com.example.blockchain.parser.TokenBalanceJobParametersParser
import com.example.blockchain.schemas.TokenTransfers._
import com.example.blockchain.sql.DDL._
import com.example.blockchain.sql.DML._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import com.example.blockchain.common.Logging
import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

import java.sql.Date

/** Computes an approximation to pi */
object TokenBalanceJob extends Logging {

  def main(args: Array[String]): Unit = {

		val params: TokenBalanceJobParameters =
			TokenBalanceJobParametersParser.parse(args, TokenBalanceJobParameters()).getOrElse(sys.exit(-1))

		implicit val spark: SparkSession = SparkSession
			.builder
			.appName("Spark Pi")
			.master("local[*]")
			.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
			.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
			.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
			.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
			.config("spark.sql.catalog.local.type", "hadoop")
			.config("spark.sql.catalog.local.warehouse", "target/iceberg-warehouse")
			.getOrCreate()

		spark.sparkContext.setLogLevel(params.sparkLogLevel)

		println(s"\n\n\n\n\n\n\n\nparams:\n${params.toString}\n\n\n\n\n")

		if (params.shouldReinitializeTokenTransfers) {
			spark.sql(dropIcebergTable(params.dailyBalancesTable))
			spark.sql(dropIcebergTable(params.walletBalancesTable))
			spark.sql(createTableDailyBalances(params.dailyBalancesTable))
			spark.sql(createTableWalletBalances(params.walletBalancesTable))
		}

		val icebergDailyBalancesTable: Table = Spark3Util.loadIcebergTable(spark, params.dailyBalancesTable)
		val icebergWalletBalancesTable: Table = Spark3Util.loadIcebergTable(spark, params.walletBalancesTable)

		for (unprocessedHour <- params.unprocessedHours) {
			val df: DataFrame =
				getTokenTransfersDF(params.inputPath)
					.where(col(colNameDate) === unprocessedHour)

			// delete current partition if exist
			spark.sql(dailyBalancesDeletePartition(params.dailyBalancesTable, unprocessedHour.toString))
			spark.sql(walletBalancesDeletePartition(params.walletBalancesTable, unprocessedHour.toString))

			val dailyBalances: DataFrame = calculateDailyBalances(df)
			dailyBalances.cache()

			dailyBalances
				.writeTo(params.dailyBalancesTable)
				.append()
			dailyBalances.show(10,false)

			val walletBalances: DataFrame = calculateCumulativeWalletBalances(dailyBalances, icebergWalletBalancesTable, unprocessedHour)
			walletBalances
				.writeTo(params.walletBalancesTable)
				.append()

			val walletBalancesTable: DataFrame = spark.table(params.walletBalancesTable)
			walletBalancesTable
				.show(10,false)

			executeCompaction(icebergDailyBalancesTable, unprocessedHour)
			executeCompaction(icebergWalletBalancesTable, unprocessedHour)
		}

  }

	def executeCompaction(tableName: Table, unprocessedHour: Date)(implicit spark: SparkSession): Unit = {
		SparkActions
			.get(spark)
			.rewriteDataFiles(tableName)
			.filter(Expressions.equal(colNameDate, unprocessedHour.toString))
	}

	def getTokenTransfersDF(path: String)(implicit spark: SparkSession): DataFrame = {
		spark
			.read
			.schema(tokenTransfersSchema)
			.parquet(path)
			.withColumn(colNameBlockDate, to_date(col(colNameBlockTimestamp)))
	}

	def calculateCumulativeWalletBalances(df: DataFrame, icebergWalletBalancesTable: Table, unprocessedHour: Date)(implicit spark: SparkSession): DataFrame = {

		val prevBalances = "prev_balances"
		val currBalances = "curr_balances"

		val prevDF: DataFrame = spark.read.format("iceberg")
			.load(icebergWalletBalancesTable.name())
			.where(col(colNameDate) === Date.valueOf(unprocessedHour.toLocalDate.minusDays(1)))
			.withColumnRenamed(colNameBalancesMap, prevBalances)

			val currDF: DataFrame = df
				.groupBy(colNameWalletAddress, colNameTokenAddress, colNameBlockDate)
				.agg(sum(col(colNameDailyBalance)).alias(colNameDailyBalance))
				.filter(col(colNameDailyBalance).isNotNull && col(colNameDailyBalance) =!= lit(0))
				.groupBy(colNameWalletAddress, colNameBlockDate)
				.agg(
					map_from_entries(
						collect_list(struct(col(colNameTokenAddress), col(colNameDailyBalance)))
					).alias(colNameBalancesMap)
				)
				.withColumnRenamed(colNameBalancesMap, currBalances)

			val joined: DataFrame = prevDF.join(currDF, Seq(colNameWalletAddress), "full_outer") // 1 wallet per df

			val merged: DataFrame = joined
				.withColumn(prevBalances, coalesce(col(prevBalances), map()))
				.withColumn(currBalances, coalesce(col(currBalances), map()))
				.withColumn(
					colNameBalancesMap,
					expr(s"""
						map_zip_with($prevBalances, $currBalances,
							(k, v1, v2) -> coalesce(v1, cast(0 as decimal(38,4))) + coalesce(v2, cast(0 as decimal(38,4)))
						)
					""") // by explicitly casting 0 to decimal(38,4), you tell Spark keep the same scale/precision as my map values.
				)
				.withColumn(colNameDate, lit(unprocessedHour))
				.select(
					col(colNameWalletAddress),
					col(colNameDate),
					col(colNameBalancesMap)
				)

			merged
	}

	def calculateDailyBalances(df: DataFrame): DataFrame = {
		val outgoing: DataFrame = df
			.select(
				col(colNameTokenAddress),
				col(colNameFromAddress).alias(colNameWalletAddress),
				col(colNameBlockDate),
				col(colNameValue).multiply(-1).alias(colNameValue)
			)

		val incoming: DataFrame = df
			.select(
				col(colNameTokenAddress),
				col(colNameToAddress).alias(colNameWalletAddress),
				col(colNameBlockDate),
				col(colNameValue).alias(colNameValue)
			)

		outgoing.union(incoming)
			.groupBy(colNameTokenAddress, colNameWalletAddress, colNameBlockDate)
			.agg(sum(col(colNameValue)).alias(colNameDailyBalance))
			.withColumn(colNameDailyBalance,col(colNameDailyBalance).cast(DecimalType(38,4)))
			.filter(col(colNameDailyBalance).isNotNull && col(colNameDailyBalance) =!= lit(0)) // clean balance 0
	}
}