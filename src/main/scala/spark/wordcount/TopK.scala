package spark.wordcount

import java.util

import org.apache.spark._

/**
  * Created by jianying.wcj on 18/2/18.
  */
object TopK {

  def main(args : Array[String]): Unit = {
    val sc = new SparkContext("local","WordCount")
    val testFilePath = "/Users/jianying.wcj/github/bigdata_code_sample/src/main/scala/spark/wordcount/wordcount.txt"
    val result = sc.textFile(testFilePath).flatMap(line => line.split("\\s+")).map(word => (word,1)).reduceByKey(_+_)
    val result1 = result.mapPartitions { iter =>
      val array = new util.ArrayList[(String,Int)]()
      var maxIndex = 0
      var maxValue = ""
      while(iter.hasNext) {
        val item = iter.next()
        if(item._2 > maxIndex) {
          maxIndex = item._2
          maxValue = item._1
        }
      }
      array.add((maxValue,maxIndex))
      array.toArray().iterator
    }.collect()
    result1.foreach(println)
  }
}
