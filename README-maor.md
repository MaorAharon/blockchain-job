# blockchain-job

## copy file
```
aws s3 cp s3://aws-public-blockchain/v1.0/eth/token_transfers/date=2025-09-10/part-00000-48eca1d8-a074-4175-9202-7da895d04ebe-c000.snappy.parquet . --no-sign-requesrt

```

## github
```
GIT_SSH_COMMAND="ssh -F ~/.ssh/config2" git push origin main
```


## build
```
sbt -J-Xmx4G clean assembly
```


## How to execute jat -
precondition
create a local spark env of Spark 3.5.1, for examlpe in Ubuntu -
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

# Run Your JAR with spark-submit
```shell
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
  --class com.example.blockchain.TokenBalanceJob /home/maor/Documents/git/scala/blockchain-job/target/scala-2.12/blockchain-job-assembly.jar \
  --unprocessed-hours 2025-09-10 \
  --bucket s3a://aws-public-blockchain/v1.0/eth \
  --prefix token_transfers \
  --iceberg-catalog local \
  --db db \
  --daily-balances-table-name daily_balances \
  --wallet-balances-table-name wallet_balances \
  --should-reinitialize-token-transfers no
```