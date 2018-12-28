//Copyright (c) 2018 Gabriel Johannes Götz

//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE.

//App to load a dataset from a specific parquet file and return records which exceed a specific timestamp threshold compared to the last record of the same type
//arg(0) file path - e.g. "hdfs:///user/hadoop/analysis_data/record.parquet"
//args(1) timestamp threshold in ms - e.g. 500000 -- the max delay between two records of the same type
//args(2) output file path - e.g. "hdfs:///user/hadoop/output"

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Encoders

case class Record(timestamp: BigInt, attribute: Double, value: Double)

object TimestampConsistencyLagCSVApp{
	def main(args: Array[String]){
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val spark = SparkSession.builder.appName("Timestamp Consistency Lag CSV App").getOrCreate()

		//Check args number
		if(args.length < 3){
			println("Missing parameters. Usage - ")	
			spark.stop()
		}

		import spark.implicits._

		val schema = Encoders.product[Record].schema

		val ds = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(args(0)).as[Record]

		val win = Window.partitionBy('attribute).orderBy('timestamp)

		val diff = lag('timestamp, 1).over(win)

		val diffDS = ds.withColumn("diff", 'timestamp - diff)

		val filteredDS = diffDS.filter('diff > args(1))

		filteredDS.orderBy('timestamp.asc).write.csv(args(2))

		spark.stop()
	
	}
}
