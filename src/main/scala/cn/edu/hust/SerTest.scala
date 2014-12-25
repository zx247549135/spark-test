package cn.edu.hust

import java.io.{DataOutputStream, ByteArrayOutputStream}

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

object SerTest {
  def testMemory(input: RDD[Float]) {
    testNative(input, StorageLevel.MEMORY_ONLY)
  }

  def testMemorySer(input: RDD[Float]) {
    testNative(input, StorageLevel.MEMORY_ONLY_SER)
  }

  def testNative(input: RDD[Float], level: StorageLevel) {
    val cachedData = input.persist(level)

    var startTime = System.currentTimeMillis
    println("Max value is " + cachedData.max())
    var duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + "seconds")

    for (i <- 1 to 5) {
      startTime = System.currentTimeMillis
      cachedData.max()
      duration = System.currentTimeMillis - startTime
      println("Duration is " + duration / 1000.0 + "seconds")
    }
  }

  def testManuallyOptimized(input: RDD[Float]) {
    val cachedData = input.mapPartitions { iter =>
      val chunk = new FloatChunk(41960)
      val dos = new DataOutputStream(chunk)
      iter.foreach(dos.writeFloat)
      Iterator(chunk)
    }.persist(StorageLevel.MEMORY_ONLY)

    var startTime = System.currentTimeMillis
    println("Max value is " + cachedData.map(_.max()).max())
    var duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + "seconds")

    for (i <- 1 to 5) {
      startTime = System.currentTimeMillis
      cachedData.map(_.max()).max()
      duration = System.currentTimeMillis - startTime
      println("Duration is " + duration / 1000.0 + "seconds")
    }
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Ser Cache Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 2
    val n = 4000000 * slices
    val rawData = spark.parallelize((1 to n).map(_.toFloat), slices)

    testManuallyOptimized(rawData)

    spark.stop()
  }
}
