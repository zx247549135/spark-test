package cn.edu.hust

/**
 * Created by peicheng on 15-6-11.
 */

import java.util.Random
import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.spark.rdd.RDD

import scala.math.exp

import breeze.linalg.{Vector, DenseVector}
import org.apache.spark._
import org.apache.hadoop.io.WritableComparator

/**
 * Created by · on 2015/6/11.
 */
object LRTest {
  val N = 300000  // Number of data points
  val D = 100   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  // Initialize w to a random value
  var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
  println("Initial w: " + w)

  def generateData(size: Int) = {
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D){rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    Array.tabulate(size)(generatePoint)
  }

  class VectorChunk(dimensions: Int, size: Int = 4196) extends ByteArrayOutputStream(size) { self =>

    def computeGradient(w: Array[Double]): Array[Double] = {
      val result = new Array[Double](dimensions)
      val current = new Array[Double](dimensions)

      var offset = 0
      while(offset < self.count) {
        var multiplier = 0.0

        var i = 0
        while (i < dimensions) {
          val value = WritableComparator.readDouble(buf, offset)
          multiplier += w(i) * value
          current(i) = value
          offset += 8
          i += 1
        }

        val y = WritableComparator.readDouble(buf, offset)
        offset += 8
        multiplier = (1 / (1 + exp(-y * multiplier)) - 1) * y

        i = 0
        while (i < dimensions) {
          result(i) = result(i) + current(i) * multiplier
          i += 1
        }
      }

      result
    }

  }

  def run(data: RDD[DataPoint]): Unit = {
    data.cache().count()
    val startTime = System.currentTimeMillis
    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = data.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
  }

  def runWithFlint(data: RDD[DataPoint]): Unit = {
    val cachedPoints = data.mapPartitions { iter =>
      val chunk = new VectorChunk(D)
      val dos = new DataOutputStream(chunk)
      for (point <- iter) {
        point.x.foreach(dos.writeDouble)
        dos.writeDouble(point.y)
      }
      Iterator(chunk)
    }.cache()
    cachedPoints.count()

    val w_op = new Array[Double](D)
    for (i <- 0 to D-1) {
      w_op(i) = w(i)
    }

    val startTime = System.currentTimeMillis
    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = cachedPoints.mapPartitions { iter =>
        val chunk = iter.next()
        Iterator(chunk.computeGradient(w_op))
      }.reduce{ (lArray, rArray) =>
        for(i <- 0 to D - 1)
          lArray(i) = lArray(i) + rArray(i)
        lArray
      }

      for(i <- 0 to D - 1)
        w_op(i) = w_op(i) - gradient(i)
    }
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkLR").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val numSlices = if (args.length > 0) args(0).toInt else 4
    val points = sc.parallelize(generateData(N), numSlices)

    runWithFlint(points)

    sc.stop()
  }

}
