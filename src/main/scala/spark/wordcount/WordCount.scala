package spark.wordcount

import org.apache.spark._
import SparkContext._
/**
  * Created by jianying.wcj on 18/2/18.
  */
object WordCount {

  def main(args : Array[String]): Unit = {
    val sc = new SparkContext("local","WordCount")
    val testFilePath = "/Users/jianying.wcj/github/bigdata_code_sample/src/main/scala/spark/wordcount/wordcount.txt"
    val result = sc.textFile(testFilePath).flatMap(line => line.split("\\s+")).map(word => (word,1)).reduceByKey(_+_)
    result.foreach(println)
  }

}
