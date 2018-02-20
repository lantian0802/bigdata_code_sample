package spark.wordcount

import java.util

import org.apache.spark.SparkContext

/**
  * Created by jianying.wcj on 18/2/20.
  */
object InvertedIndex {

  def main(args : Array[String]): Unit = {

    val sc = new SparkContext("local","InvertedIndex")
    val filePath = "/Users/jianying.wcj/github/bigdata_code_sample/src/main/scala/spark/wordcount/invertindex.txt"
    val srcFile = sc.textFile(filePath)
    val id2Words = srcFile.map { line =>
       println("line="+line)
       val kv = line.split(",")
       println("kv="+kv(0))
       //println("kv0="+=kv(0))
       val words = kv(1).split("\\s+")
       (kv(0),words)
    }

    val word2Id = id2Words.flatMap { temp =>
        val valueList = new util.ArrayList[(String,String)]();
        val id = temp._1
        for(item <- temp._2) {
          valueList.add((item,id))
        }
        valueList.toArray
    }

    val result = word2Id.collect()
    result.foreach(println)
  }

}
