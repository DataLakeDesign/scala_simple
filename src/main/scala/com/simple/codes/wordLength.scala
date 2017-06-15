package com.simple.codes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordLength {
  def main(args: Array[String]): Unit = { 
  val conf = new SparkConf().setAppName("WordLength Count")
  val sc = new SparkContext(conf)
  val dataRDD = sc.textFile("hdfs://localhost:54310/SampleFile/SampData")
  val lengthCount = dataRDD.flatMap(line => (line.split(" "))).map(word => (word.length(),1)).reduceByKey(_+_)
  val lengthCountFnl = lengthCount.map(res => (res._1.toString() + "byte long word count", res._2))
  lengthCountFnl.saveAsTextFile("hdfs://localhost:54310/SampleFile/LenCountData")
  } 
}