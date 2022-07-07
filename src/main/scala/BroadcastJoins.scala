package content

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local")
    .getOrCreate()            //create sparksession object

  //Huge Dataset
  val table = spark.range(1,100000)

  //small dataset
  val rows: RDD[Row] = spark.sparkContext.parallelize(List(
    Row(1,"gold"),
    Row(2,"silver"),
    Row(3,"Bronze")
  ))

  val rowsSchema = StructType(Array(
    StructField("id",IntegerType),
    StructField("metal", StringType),
  ))

  val lookupTable : DataFrame = spark.createDataFrame(rows,rowsSchema)

  val joined = table.join(lookupTable,"id")
  joined.show()


  import org.apache.spark.sql.functions._
  val joinedsmart = table.join(broadcast(lookupTable))
  joinedsmart.show()

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}

