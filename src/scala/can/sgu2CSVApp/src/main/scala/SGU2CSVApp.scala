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

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Encoders

case class Record(timestamp: BigInt, attribute: Double, value: Double)

object Sgu2CSVApp{
	def main(args: Array[String]){
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val spark = SparkSession.builder.appName("SGU 2 CSV App").getOrCreate()

		//Check args number
		if(args.length < 2){
			println("Missing parameters. Usage - ")	
			spark.stop()
		}

		import spark.implicits._

		val schema = Encoders.product[Record].schema

		val ds = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(args(0)).as[Record]

		val ds2 = ds.where('attribute === 15.0 || 'attribute === 10.0)

		val win1 = Window.partitionBy('attribute).orderBy('timestamp)

		val diff = lag('timestamp, 1).over(win1)

		val dsDiff = ds2.withColumn("diff", 'timestamp -diff)

		val dsFiltered = dsDiff.where(('attribute === 15.0 && 'value < 6.0) || 'attribute === 10.0)

		val win = Window.orderBy('timestamp)

                val lagFunc = lag(when('value === 1.0 && 'attribute === 10.0, 1).otherwise(0), 1, 0).over(win)

		dsFiltered.withColumn("last", lagFunc).orderBy('timestamp).where('attribute === 15.0 && 'last === 1 && 'diff > 50000).write.csv(args(1))

		spark.stop()
	
	}
}
