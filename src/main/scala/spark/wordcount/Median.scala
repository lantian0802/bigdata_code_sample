package spark.wordcount

import org.apache.spark.SparkContext

/**
  * Created by jianying.wcj on 18/2/19.
  */
object Median {

  def main(args:Array[String]): Unit = {

    val sc = new SparkContext("local", "Median")
    val dataPath = "/Users/jianying.wcj/github/bigdata_code_sample/src/main/scala/spark/wordcount/data.txt"
    val data = sc.textFile(dataPath)
    val words = data.flatMap(_.split("\\s+")).map(word => word.toInt)
    val number = words.map { num =>
      (num.toInt / 4, num.toInt)
    }.sortByKey()

    val pariCount = words.map(word => (word / 4, 1)).reduceByKey(_ + _).sortByKey()
    val count = words.count().toInt
    var mid = 0
    if (count % 2 != 0) {
      mid = count / 2 + 1
    } else {
      mid = count / 2
    }
    var temp =0
    var temp1= 0
    var index = 0
    val tongNumber = pariCount.count().toInt

    var foundIt = false
    for(i <- 0 until tongNumber-1 if !foundIt) {
      temp = temp + pariCount.collectAsMap()(i)
      temp1 = temp - pariCount.collectAsMap()(i)
      if(temp >= mid) {
        index = i
        foundIt = true
      }
    }
    val tonginneroffset = mid - temp1

    val median = number.filter(_._1==index).takeOrdered(tonginneroffset)
    sc.setLogLevel("ERROR")
    println(median(tonginneroffset-1)._2)
    sc.stop()

  }
}
