import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18" // use >=2.12.15 for Spark 3.5

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
	.settings(
		name := "blockchain-job",

		libraryDependencies ++= Seq(
			// Spark core + SQL
			"org.apache.spark" %% "spark-core" % sparkVersion % Provided,
			"org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,

			// Hadoop AWS connector (for s3a:// support)
			"org.apache.hadoop" % "hadoop-aws" % "3.3.4",

			// Iceberg Spark runtime (note: single %)
			"org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.8.0",

			// AWS SDK
			"software.amazon.awssdk" % "bundle" % "2.25.40",

			// parser
			"com.github.scopt" %% "scopt" % "4.1.0",

			// Testing
			"org.scalatest" %% "scalatest" % "3.2.18" % Test
		),

			// Assembly plugin settings
			ThisBuild / assembly / mainClass := Some("com.example.blockchain.TokenBalanceJob"),
			ThisBuild / assembly / assemblyJarName := "blockchain-job-assembly.jar"

	)

assembly / assemblyMergeStrategy := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
	case PathList("reference.conf") => MergeStrategy.concat
	case PathList("google", "protobuf", xs @ _*) => MergeStrategy.first
	case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
	case PathList("io", "netty", xs @ _*) => MergeStrategy.first
	case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
	case PathList("org", "jetbrains", "annotations", xs @ _*) => MergeStrategy.first
	case x => MergeStrategy.first
}