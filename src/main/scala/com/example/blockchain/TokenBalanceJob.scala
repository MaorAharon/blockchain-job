package com.example.blockchain

import com.example.blockchain.schemas.TokenTransfers._
import com.example.blockchain.sql.DDL._
import com.example.blockchain.sql.DML.dailyBalancesDeletePartition
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

import java.sql.Date

/** Computes an approximation to pi */
object TokenBalanceJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
			.master("local[*]")
			.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
			.config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
			.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
			.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
			.config("spark.sql.catalog.local.type", "hadoop")
			.config("spark.sql.catalog.local.warehouse", "target/iceberg-warehouse")
			.getOrCreate()

		val date_ = "2025-09-10"

//		val fileSystemPath10 = f"file:///home/maor/Documents/git/scala/blockchain-job/src/test/resources/token_transfers/date=${date_}/"
//		val fileSystemPath11 = f"file:///home/maor/Documents/git/scala/blockchain-job/src/test/resources/token_transfers/date=${date_}/"
		val fileSystemPath = f"file:///home/maor/Documents/git/scala/blockchain-job/src/test/resources/token_transfers/"

		val df = spark
			.read
			.schema(tokenTransfersSchema)
			.parquet(fileSystemPath)
			.where(col(colNameDate) === Date.valueOf(date_))
			.where(col(colNameFromAddress) === "0x00000000000000000000000003e0b58dda38e4a5e577bcb71baf4fda25160153" or col(colNameToAddress) === "0x00000000000000000000000003e0b58dda38e4a5e577bcb71baf4fda25160153")
			.where(col(colNameTokenAddress) === "0x4063496401ba57197b4c03c889440512dbf2278a")

		df.printSchema()
		df.show(5, false)

		val dailyBalances = calculateDailyBalances(df)

		spark.sql(dropIcebergTable("local", "db", "daily_balances"))
		spark.sql(createTableDailyBalances("local", "db", "daily_balances"))

		spark.sql(dailyBalancesDeletePartition("local", "db", "daily_balances", date_))
		dailyBalances.writeTo("local.db.daily_balances")
			.append()
//
		val icebergDf = spark.table("local.db.daily_balances")
		icebergDf.show(5,false)
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