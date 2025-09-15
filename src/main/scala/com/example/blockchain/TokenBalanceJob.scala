package com.example.blockchain

import com.example.blockchain.parameters.TokenBalanceJobParameters
import com.example.blockchain.parser.TokenBalanceJobParametersParser
import com.example.blockchain.schemas.TokenTransfers._
import com.example.blockchain.sql.DDL._
import com.example.blockchain.sql.DML.dailyBalancesDeletePartition
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
			spark.sql(createTableDailyBalances(params.dailyBalancesTable))
		}

		val icebergTable: Table = Spark3Util.loadIcebergTable(spark, params.dailyBalancesTable)

		for (unprocessedHour <- params.unprocessedHours) {
			val df: DataFrame =
				getTokenTransfersDF(params.inputPath)
				.where(col(colNameDate) === unprocessedHour)

			val dailyBalances: DataFrame = calculateDailyBalances(df)

			// delete current partition if exist
			spark.sql(dailyBalancesDeletePartition(params.dailyBalancesTable, unprocessedHour.toString))

			dailyBalances
				.writeTo(params.dailyBalancesTable)
				.append()

			executeCompaction(icebergTable, unprocessedHour)

			val icebergDf: DataFrame = spark.table(params.dailyBalancesTable)
			icebergDf.where(col(colNameDate) === unprocessedHour).show(5,false)
		}

  }

	def executeCompaction(dailyBalancesTable: Table, unprocessedHour: Date)(implicit spark: SparkSession): Unit = {
		SparkActions
			.get(spark)
			.rewriteDataFiles(dailyBalancesTable)
			.filter(Expressions.equal(colNameDate, unprocessedHour.toString))
	}

	def getTokenTransfersDF(path: String)(implicit spark: SparkSession): DataFrame = {
		spark
			.read
			.schema(tokenTransfersSchema)
			.parquet(path)
	}

	def calculateDailyBalances(df: DataFrame): DataFrame = {
		val outgoing: DataFrame = df
			.select(
				col(colNameTokenAddress),
				col(colNameFromAddress).alias(colNameWalletAddress),
				col(colNameDate),
				col(colNameValue).multiply(-1).alias(colNameValue)
			)

		val incoming: DataFrame = df
			.select(
				col(colNameTokenAddress),
				col(colNameToAddress).alias(colNameWalletAddress),
				col(colNameDate),
				col(colNameValue).alias(colNameValue)
			)

		outgoing.union(incoming)
			.groupBy(colNameTokenAddress, colNameWalletAddress, colNameDate)
			.agg(sum(col(colNameValue)).alias(colNameDailyBalance))
			.withColumn(colNameDailyBalance,col(colNameDailyBalance).cast(DecimalType(38,4)))


	}
}