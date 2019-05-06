package cn.person

import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    println("Hello World!")
    val path: String = "C:\\Users\\dell\\Desktop\\spark.txt"
    val spark = SparkSession.builder().appName("WordCount").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val lines = spark.sparkContext.textFile(path)
    val words = lines.flatMap{line => line.split(" ")}
    val wordCounts = words.map{word => (word,1)}.reduceByKey(_ + _)
    val countWord = wordCounts.map{word =>(word._2,word._1)}
    val sortedCountWord = countWord.sortByKey(false)
    val sortedWordCount = sortedCountWord.map{word => (word._2, word._1)}
    sortedWordCount.foreach(
      s=> { println("word \""+s._1+ "\" appears "+s._2+" times.")}
    )
    spark.stop()
  }

}
