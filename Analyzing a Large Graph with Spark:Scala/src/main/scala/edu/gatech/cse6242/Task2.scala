package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object Task2 {
	def main(args: Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("Task2"))
		
		val threshold = 0
		
		val file = sc.textFile("hdfs://localhost:8020" + args(0))
		
		val line = file.flatMap(_.split(" "))

		val tokenized = line.map( x => (x.split("\t")(1).toInt, x.split("\t")(2).toInt) )
		//filter weight = 0 
		val filtered = tokenized.filter{case (x, y) => y > 0}
		//aggregate under same key
		val wordCounts = filtered.reduceByKey( (x, y) => x + y )
		
		val results = wordCounts.collect{ case (x,y) => Array (x, y).mkString("\t") }
		
		results.saveAsTextFile("hdfs://localhost:8020" + args(1))

    }
}
