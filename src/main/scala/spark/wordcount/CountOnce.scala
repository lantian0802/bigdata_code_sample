package spark.wordcount

import org.apache.spark.SparkContext

/**
  * Created by jianying.wcj on 18/2/21.
  */
object CountOnce {

  def main(args : Array[String]): Unit = {

    val sc = new SparkContext("local","CountOnce")
    val path = "/Users/jianying.wcj/github/bigdata_code_sample/src/main/scala/spark/wordcount/countonce.txt"
    val lines = sc.textFile(path)

    val nums = lines.flatMap{ line =>
      line.split("\\s+")
    }.map { item =>
        item.toInt
    }

    val result = nums.mapPartitions { iterator =>
       var result = iterator.next()
       while(iterator.hasNext) {
         val value = iterator.next()
         result ^= value
       }
       Seq((1,result)).iterator
    }.reduceByKey(_^_).collect()

    result.foreach(println)
  }
}
