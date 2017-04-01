package org.spark.sparkshell1

import org.apache.spark.SparkContext

object problem3 {
  def main(args: Array[String]) {
    val sc=new SparkContext("local[*]","Weights and Nodes")
    val input = sc.textFile("/dxs161930/Assignment2/input.txt").map(line =>line.split("\t"))
    var weights = input.map(line=>(line(1),line(2).toInt))

    val result = weights.reduceByKey((x, y) => x + y)
    result.sortBy(_._1).sortByKey(true,1).foreach(println)
  }
}