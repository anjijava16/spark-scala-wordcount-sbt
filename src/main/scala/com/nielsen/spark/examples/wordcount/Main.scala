package com.nielsen.spark.examples.wordcount

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val spark = SparkSession
      .builder
      .appName("Spark Scala Wordcount")
      .getOrCreate()

    val lines = spark.sparkContext.textFile(input)
    val words = lines.flatMap(line => line.split(" "))
    val ones = words.map(word => (word, 1))
    val counts = ones.reduceByKey(_ + _)

    counts.saveAsTextFile(output)

    spark.stop()
  }
}