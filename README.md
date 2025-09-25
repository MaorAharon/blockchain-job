# TokenBalanceJob

## Overview

`TokenBalanceJob` is a Spark application that calculates daily token balances from token transfer data stored in Parquet files.
The job reads token transfer events, aggregates incoming and outgoing transfers per wallet and token, and writes the resulting daily balances into an Iceberg table.

This project is implemented in Scala using Apache Spark and Iceberg.

---

## Table of Contents

* [Prerequisites](#prerequisites)
* [Setup & Running](#setup--running)
* [Approach & Design Decisions](#approach--design-decisions)
* [Assumptions & Considerations](#assumptions--considerations)

---

## Prerequisites

* git
* Spark on Local Machine
* Java 11+
* Scala 2.12+
* Apache Spark 3.4+
* SBT (Scala Build Tool)
* Access to S3 (or local filesystem for testing)
* Iceberg Spark extensions

---

## Setup & Running

1. Download Spark to Local Machine (Ubuntu 24.04 noble)
```
# Download Spark 3.5.1
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz

# Verify the Download (Optional)
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz.sha512
sha512sum -c spark-3.5.1-bin-hadoop3.tgz.sha512

# Extract the Tarball
tar -xvf spark-3.5.1-bin-hadoop3.tgz
cd spark-3.5.1-bin-hadoop3

# Set up env
export SPARK_HOME="/home/maor/Downloads/spark_3_5_1/spark-3.5.1-bin-hadoop3"
export PATH=$SPARK_HOME/bin:$PATH
```
2. Clone the repository:

```
git clone git@github.com:MaorAharon/blockchain-job.git
cd <repository-folder>
```

2. Build the project using SBT:

```
sbt clean assembly
```

3. Run the Spark job locally:

```
# input:
# s3: `--bucket s3a://aws-public-blockchain/v1.0/eth \`
# local file system: --bucket file:///< path-to-the-project > /blockchain-job/src/test/resources \ 


spark-submit \
  --deploy-mode client \
  --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
  --conf spark.sql.caseSensitive=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=target/iceberg-warehouse \
  --class com.example.blockchain.TokenBalanceJob \
  --master local \
  --class com.example.blockchain.TokenBalanceJob < path-to-the-jar >/blockchain-job-assembly.jar \
  --spark-log-level INFO \
  --unprocessed-hours 2025-09-10 \
  --bucket s3a://aws-public-blockchain/v1.0/eth \
  --prefix token_transfers \
  --iceberg-catalog local \
  --db db \
  --daily-balances-table-name daily_balances \
  --wallet-balances-table-name wallet_balances \
  --should-reinitialize-token-transfers true
```

**Parameters:**

| Parameter                               | Description                                                                    |
| --------------------------------------- |--------------------------------------------------------------------------------|
| `--unprocessed-hours`                   | Comma-separated list of dates in `yyyy-MM-dd` format to process.               |
| `--spark-log-level`                     | Spark logging level (e.g., `INFO`, `WARN`, `ERROR`).                           |
| `--bucket`                              | Input S3 bucket / local file system, containing token transfer files.          |
| `--prefix`                              | Prefix/path inside the bucket for input files.                                 |
| `--iceberg-catalog`                     | Name of the Iceberg catalog to use.                                            |
| `--db`                                  | Database name for Iceberg tables.                                              |
| `--table`                               | Table name to store the daily balances.                                        |
| `--should-reinitialize-token-transfers` | Boolean flag indicating whether to drop and recreate the daily balances table. |


4. Sample data outputs:

The first 5 rows of the daily balances table are displayed after processing:

```
icebergDf.show(5, false)
```

---

## Approach & Design Decisions

* **Aggregation Logic:**

    * Outgoing transfers are negated (`value * -1`) and incoming transfers remain positive.
    * Union of incoming and outgoing transfers is aggregated by `tokenAddress`, `walletAddress`, and `date`.
    * Daily balances are stored as `Decimal(38,4)` for precision.

* **Table Management:**

    * If `shouldReinitializeTokenTransfers` is `true`, the Iceberg table is dropped and recreated to ensure a clean slate.

* **Spark Configurations:**

    * Local execution for testing with `local[*]`.
    * S3 access configured with `AnonymousAWSCredentialsProvider` for public buckets.
    * Iceberg is used as the table format for transactional writes.

* **Modularity:**

    * Separate methods for reading Parquet files (`getTokenTransfersDF`) and calculating daily balances (`calculateDailyBalances`) for clarity and reusability.

---

## Assumptions & Considerations

1. **Data Schema:**

    * Assumes input Parquet files follow `tokenTransfersSchema`.
    * Columns include `tokenAddress`, `fromAddress`, `toAddress`, `date`, and `value`.


2. **Partitioning:**

    * Data is processed per date. Only unprocessed partitions are read and written to reduce computation.


3. **Data Processing Optimizations:**

    * The job applies **predicate pushdown** by filtering input Parquet files with:
      ```scala
      df.where(col(colNameDate) === unprocessedHour)
      ```
      This ensures only the relevant partitions (per date) are read from storage,
      reducing the amount of scanned data and improving performance.
    * The Iceberg table is partitioned by `date` to further benefit from predicate pushdowns on queries.


4. **Multiple Hours Processing:**

    * The job supports processing multiple dates via the --unprocessed-hours parameter. 
    * For robustness, each date in the list is processed sequentially and independently.


5. **Idempotency:**

    * Daily balances for a given date are deleted before appending new results to handle reprocessing safely.

   
6. **Compaction**
    * After writing each partition, the job invokes Iceberg’s built-in procedure:
    * This compaction step merges small files created during incremental writes into larger ones, improving query performance and reducing metadata overhead. 
    * Compaction is scoped only to the processed partition (unprocessedHour) to avoid long table-wide rewrites.


7. **Environment:**

    * Designed to run locally or on a Spark cluster.
    * Iceberg warehouse path (`spark.sql.catalog.local.warehouse`) should be updated for production deployment.


8. **Error Handling:**

    * If job parameters cannot be parsed, the job exits immediately.


9. **Testing strategy**
   
   * Unit Tests – Validate each core method independently. For example, provide a small token_transfers dataset and assert that the calculated balances match the expected results.

   * Integration Tests – Execute the entire job on representative sample data and confirm that the generated Iceberg table contains the correct outputs.

   * Data Quality Checks – Ensure key constraints hold, such as uniqueness of (tokenAddress, walletAddress, date) and that aggregated balances (incoming/outgoing) align with total transfer amounts.

   * Regression Tests – Run the job multiple times on the same partition and across multiple partitions to verify idempotency and consistency of results over time.