package org.spark.sparkshell1

import org.apache.spark.SparkContext

object problem2 {
  def main(args: Array[String]) {
    val sc=new SparkContext("local[*]","Top 10 Businesses")
    val review = sc.textFile("/dxs161930/Assignment2/review.csv").map(line =>line.split("::"))
    var reviewMap = review.map(line=>(line(2),line(3).toDouble)).distinct
    val avgRatings = reviewMap.groupByKey().map(data => { val avg = data._2.sum / data._2.size; (avg,data._1)})
    val result = avgRatings.sortBy(_._2).sortByKey(false,1).take(10)
    val res = result.map(_.swap)

    val business = sc.textFile("/dxs161930/Assignment2/business.csv").map(line =>line.split("::"))
    var businessMap = business.map(line=>(line(0),line(1)+" ,"+line(2))).distinct

    val parallelTopTen = sc.parallelize(res)
    val joinedMaps = businessMap.join(parallelTopTen).collect
    joinedMaps.sortBy(_._1).foreach{data =>
		  val businessId = data._1	
		  val details = data._2
		  println(businessId,details._1,details._2)
	}
 }
}