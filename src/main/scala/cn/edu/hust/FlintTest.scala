package cn.edu.hust

import org.apache.spark.SparkConf
import org.apache.spark.flint.FlintContext


import scala.math._

object FlintTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val flint = new FlintContext(conf)

    import flint.implicits._

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val range = flint.parallelize(1 to n, slices).withFlint()
    val count = range.map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    flint.stop()
  }
}
