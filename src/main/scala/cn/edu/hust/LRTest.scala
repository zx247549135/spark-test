package cn.edu.hust

/**
 * Created by peicheng on 15-6-11.
 */

import java.util.Random
import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.spark.rdd.RDD
import org.apache.log4j._

import scala.math.exp

import breeze.linalg.{Vector, DenseVector}
import org.apache.spark._
import org.apache.hadoop.io.WritableComparator

/**
 * Created by · on 2015/6/11.
 */
object LRTest {
  val N = 300000
  //val N = 300000  // Number of data points
  val D = 100   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  // Initialize w to a random value
  var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
  //println("Initial w: " + w)

  def generateData(size: Int, seed: Int = 0) = {
    val rand = new Random(42 + seed)
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D){rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    Array.tabulate(size)(generatePoint)
  }

  class PointChunk(dimensions: Int = D, size: Int = 4196) extends ByteArrayOutputStream(size) { self =>

    def getVectorValueIterator(w: Array[Double]) = new Iterator[Array[Double]] {
      var offset = 0
      var currentPoint = new Array[Double](dimensions)
      var i = 0
      var y = 0.0
      var dotvalue = 0.0

      override def hasNext = offset < self.count

      override def next() = {
        if (!hasNext) Iterator.empty.next()
        else {
          //read data from the chunk
          i = 0
          while (i < dimensions) {
            currentPoint(i)= WritableComparator.readDouble(buf, offset)
            offset += 8
            i += 1
          }
          y = WritableComparator.readDouble(buf, offset)
          offset += 8
          //calculate the dot value
          i = 0
          dotvalue = 0.0
          while (i < dimensions) {
            dotvalue += w(i) * currentPoint(i)
            i += 1
          }
          //transform to values
          val factor = (1 / (1 + exp(-y * dotvalue)) - 1)
          i = 0
          while (i < dimensions) {
            currentPoint(i) *= factor * y
            i += 1
          }
          currentPoint.clone()
        }
      }
    }
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
    println(data.cache().count() + " points")

    val startTime = System.currentTimeMillis
    for (iter <- 1 to ITERATIONS) {
      val startTime = System.currentTimeMillis

      val gradient = data.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient

      val duration = System.currentTimeMillis - startTime
      println("Iteration " + iter + " duration is " + duration / 1000.0 + " seconds")
    }
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
  }

  def runWithChunk(points: RDD[DataPoint]): Unit = {
    val cachedPoints = points.mapPartitions { iter =>
      val chunk = new PointChunk(D, 41960)
      val dos = new DataOutputStream(chunk)
      for (point <- iter) {
        point.x.foreach(dos.writeDouble)
        dos.writeDouble(point.y)
      }
      Iterator(chunk)
    }.cache()
    cachedPoints.count()

    val w_op = new Array[Double](D)
    var i = 0
    while(i < D) {
      w_op(i) = w(i)
      i += 1
    }

    val startTime = System.currentTimeMillis

    for (iter <- 1 to ITERATIONS) {
      val startTime = System.currentTimeMillis

      val gradient = cachedPoints.mapPartitions{ iter =>
        val chunk = iter.next()
        chunk.getVectorValueIterator(w_op)
      }.reduce{ (lArray, rArray) =>
        val result_array = new Array[Double](D)
        var i = 0
        while(i < D) {
          result_array(i) = lArray(i) + rArray(i)
          i += 1
        }
        result_array
      }

      var i = 0
      while(i < D) {
        w_op(i) = w_op(i) - gradient(i)
        i += 1
      }

      val duration = System.currentTimeMillis - startTime
      println("Iteration " + iter + " duration is " + duration / 1000.0 + " seconds")
    }

    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")

  }

  def runWithFlint(data: RDD[DataPoint]): Unit = {
    val cachedPoints = data.mapPartitions { iter =>
      val chunk = new VectorChunk(D, 41960)
      val dos = new DataOutputStream(chunk)
      for (point <- iter) {
        point.x.foreach(dos.writeDouble)
        dos.writeDouble(point.y)
      }
      Iterator(chunk)
    }.cache()
    cachedPoints.count()

    val w_op = new Array[Double](D)
    var i = 0
    while(i < D) {
      w_op(i) = w(i)
      i += 1
    }

    val startTime = System.currentTimeMillis
    for (iter <- 1 to ITERATIONS) {
      val startTime = System.currentTimeMillis

      val gradient = cachedPoints.mapPartitions { iter =>
        val chunk = iter.next()
        Iterator(chunk.computeGradient(w_op))
      }.reduce{ (lArray, rArray) =>
        var i = 0
        while(i < D) {
          lArray(i) = lArray(i) + rArray(i)
          i += 1
        }
        lArray
      }

      var i = 0
      while(i < D) {
        w_op(i) = w_op(i) - gradient(i)
        i += 1
      }

      val duration = System.currentTimeMillis - startTime
      println("Iteration " + iter + " duration is " + duration / 1000.0 + " seconds")
    }
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkLR").setMaster("local")
    val sc = new SparkContext(sparkConf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val numSlices = if (args.length > 0) args(0).toInt else 4
    val indices =  Array.tabulate(numSlices)(i => i)
    val points = sc.parallelize(indices, numSlices).mapPartitions( iter => {
      val part = iter.next()

      new Iterator[DataPoint] {
        var index = 0
        val count = N / numSlices
        val rand = new Random(42 + part)

        override def hasNext = index < count

        override def next() = {
          if (!hasNext) Iterator.empty.next()
          val y = if(index % 2 == 0) -1 else 1
          val x = DenseVector.fill(D){rand.nextGaussian + y * R}
          index += 1
          DataPoint(x, y)
        }
      }
    })

    //run(points)
    runWithChunk(points)
    //runWithFlint(points)

    sc.stop()
  }

}
