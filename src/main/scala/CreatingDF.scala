import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}



object CreatingDF extends App {

  val spark = SparkSession.builder().master("local[*]").appName("creating DF").getOrCreate()
  val df = spark.read.json("src/main/resources/data.json")
  df.show()

  //toDF method
  import spark.implicits._
  val data = Seq((10,"Accounting"), (20,"HR"),(30,"IT")).toDF("DeptID","DeptName")
  data.printSchema()
  data.show()

  //using create dataframe method

  val data1 = spark.sparkContext.parallelize(Seq(
    Row(101,"Praneet"),
    Row(102,"Lalit"),
    Row(103,"Rahul")
  ))
   val empschema = new StructType()
     .add(StructField("Empno.",IntegerType,true))
     .add(StructField("EmpName",StringType,true))
  val dataframe = spark.createDataFrame(data1,empschema)
  dataframe.show()
}
