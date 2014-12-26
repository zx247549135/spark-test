package cn.edu.hust

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.io.WritableComparator
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class FloatChunk(size: Int = 4196) extends ByteArrayOutputStream(size) {
  def max(): Float = {
    var maxValue = 0.0f
    var currentValue = 0.0f
    var offset = 0
    while (offset <= count) {
      currentValue = WritableComparator.readFloat(buf, offset)
      if (currentValue > maxValue) {
        maxValue = currentValue
      }
      offset += 4
    }
    maxValue
  }
}

case class FloatWrapper(value: Float)

object SerTest {
  def testMemory(input: RDD[FloatWrapper]) {
    testNative(input, StorageLevel.MEMORY_ONLY)
  }

  def testMemorySer(input: RDD[FloatWrapper]) {
    testNative(input, StorageLevel.MEMORY_ONLY_SER)
  }

  def testNative(input: RDD[FloatWrapper], level: StorageLevel) {
    println("-------- Native " + level.description + " --------")

    val cachedData = input.persist(level)
    
    val maxF1 = (x: Float, y: FloatWrapper) => if (y.value > x) y.value else x
    val maxF2 = (x: Float, y: Float) => if (y > x) y else x

    var startTime = System.currentTimeMillis
    println("Max value is " + cachedData.aggregate(0.0f)(maxF1, maxF2))
    var duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")

    for (i <- 1 to 5) {
      startTime = System.currentTimeMillis
      cachedData.aggregate(0.0f)(maxF1, maxF2)
      duration = System.currentTimeMillis - startTime
      println("Duration is " + duration / 1000.0 + " seconds")
    }

    cachedData.unpersist()
    println()
  }

  def testManuallyOptimized(input: RDD[FloatWrapper]) {
    println("------------------ Manually optimized ------------------")

    val cachedData = input.mapPartitions { iter =>
      val chunk = new FloatChunk(41960)
      val dos = new DataOutputStream(chunk)
      iter.foreach(x => dos.writeFloat(x.value))
      Iterator(chunk)
    }.persist(StorageLevel.MEMORY_ONLY)

    var startTime = System.currentTimeMillis
    println("Max value is " + cachedData.map(_.max()).max())
    var duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")

    for (i <- 1 to 5) {
      startTime = System.currentTimeMillis
      cachedData.map(_.max()).max()
      duration = System.currentTimeMillis - startTime
      println("Duration is " + duration / 1000.0 + " seconds")
    }

    cachedData.unpersist()
    println()
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Ser Cache Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 4
    val n = 6000000 * slices
    val rawData = spark.parallelize(1 to n, slices).map(x => new FloatWrapper(x.toFloat))

    //testManuallyOptimized(rawData)
    testMemorySer(rawData)
    //testMemory(rawData)

    spark.stop()
  }
}
