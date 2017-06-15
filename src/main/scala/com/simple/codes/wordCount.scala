package com.simple.codes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("hdfs://localhost:54310/SampleFile/SampData")
    val wordCount = dataRDD.flatMap(x => (x.split(" "))).map(x => (x,1)).reduceByKey(_+_)
    wordCount.saveAsTextFile("hdfs://localhost:54310/SampleFile/OutData")
  }
}