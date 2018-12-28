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

//App to load a dataset from a specific parquet file and output us records that do not match the test data
//args(0) file path of the us data - e.g. "hdfs:///user/hadoop/analysis_data/us_data"
//args(1) file path of the test data - e.g. "hdfs:///user/hadoop/analysis_data/test_data"
//args(2) output path - e.g. "hdfs:///user/hadoop/output/spark_output_us_eva_csv"
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class UsRecord(timestamp: Integer, ui32ArduinoTimestamp: Integer, f32Value: Double)
case class TestRecord(timestamp: Integer, objectRecognized: Double)
case class JoinRecord(timestamp: Integer, ui32ArduinoTimestamp: Integer, f32Value: Double, objectRecognized: Double)

object UsEvaApp{
	def main(args: Array[String]){
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val spark = SparkSession.builder.appName("US Eva App").getOrCreate()

		//Check args number
		if(args.length < 1){
			println("Missing parameters. Usage - ")	
			spark.stop()
		}

		import spark.implicits._

		val usDs = spark.read.parquet(args(0)).as[UsRecord]				
		val testDs = spark.read.parquet(args(1)).as[TestRecord]

		val joinDs = usDs.join(testDs, "timestamp").as[JoinRecord]	

		val condFalseNegative = (('f32Value > 220 || 'f32Value < 0) && 'objectRecognized === 1.0)

		val condFalsePositive = (('f32Value <= 220 && 'f32Value >= 0) && 'objectRecognized === 0.0)

		val errorDs = joinDs.where(condFalseNegative || condFalsePositive)

		errorDs.show

		errorDs.write.csv(args(2))	

		spark.stop()
	
	}
}
