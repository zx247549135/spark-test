package cn.edu.hust

import org.apache.spark.{SparkContext, SparkConf}

import scala.math._

object FlintTest {
  def main(args: Array[String]) {
    import org.apache.spark.flint.implicits._

    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.withFlint().reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
