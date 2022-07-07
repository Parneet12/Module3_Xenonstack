import org.apache.spark.{SparkContext,SparkConf}


object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("word.txt")
    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("output")
    System.out.println("It works");
  }
}

