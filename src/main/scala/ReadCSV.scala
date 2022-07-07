import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadCSV {
  def main(args:Array[String]): Unit = {

    val spark_Conf = new SparkConf().setMaster("local[1]").setAppName("Reading_CSV_File")
    val sc = new SparkContext(spark_Conf)

    val spark_Session = SparkSession.builder()
      .config(spark_Conf)
      .getOrCreate()

    val path = "gun-violence-data.csv"
    val read_df = spark_Session.read.csv(path)
   read_df.show(10)

    //read_df.orderBy("_c1").show()

   // read_df.select

   // read_df.printSchema()

  //  read_df.describe("_c5").show()

   // read_df.select("_c2","_c6").show()

   // read_df.select("_c1","_c2","_c5","_c12").filter("_c5>1").show()
  }

}
