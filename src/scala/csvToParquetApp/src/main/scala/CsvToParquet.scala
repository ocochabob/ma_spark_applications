//App to load a dataset from specific csv files and store the dataset as parquet file
//arg(0) file path - e.g. "hdfs:///user/hadoop/input/file.csv"
//arg(1) csv delimiter
//arg(2) parquet file path - e.g. "hdfs:///user/hadoop/analysis_data/file.parquet"

import org.apache.spark.sql.SparkSession

object CsvToParquet{
	def main(args: Array[String]){
		val spark = SparkSession.builder.appName("Csv to Parquet App").getOrCreate()

		import spark.implicits._

                val dataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", args(1)).load(args(0))

		dataFrame.write.parquet(args(2))

		spark.stop()
	
	}
}
