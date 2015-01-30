package cn.edu.hust

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object CodeTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Code Cache Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 4
    val n = 60 * slices
    val rawData = spark.parallelize(1 to n, slices)


    val filtered = rawData.filter(_ > 3)

    println(filtered.getClass.getDeclaredField("f").getType.getSimpleName)


    spark.stop()
  }
}
