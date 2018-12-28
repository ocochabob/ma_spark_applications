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

//App to load a dataset from a specific parquet file and return records which satisfy the specified conditions
//arg(0) file path - e.g. "hdfs:///user/hadoop/analysis_data/record.parquet"
//args(1) output file path - e.g. "hdfs:///user/hadoop/output/spark_output_thresholds"

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class Record(timestamp: BigInt, attribute: Double, value: Double)

object ThresholdApp{
	def main(args: Array[String]){
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val spark = SparkSession.builder.appName("Threshold App").getOrCreate()

		//Check args number
		if(args.length < 2){
			println("Missing parameters. Usage - ")	
			spark.stop()
		}

		import spark.implicits._

		val df = spark.read.parquet(args(0))

		val ds = df.as[Record]

		val cond1 = ('attribute === 26.0 && 'value > 1021)

		val cond2 = ('attribute === 28.0 && 'value > 4089)

		val cond3 = ('attribute === 29.0 && 'value < 0.0105)						
		ds.where(cond1 || cond2 || cond3).orderBy('timestamp.asc).write.csv(args(1))

		spark.stop()
	
	}
}
