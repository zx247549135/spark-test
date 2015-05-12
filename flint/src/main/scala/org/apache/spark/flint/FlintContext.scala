package org.apache.spark.flint

import org.apache.spark.{TaskContext, SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.BitSet
import scala.reflect.ClassTag

/**
 * implicits for rdd to flint
 */
class FlintContext(conf: SparkConf) extends SparkContext(conf) {

  private val startIds = new BitSet(100)
  private val endIds = new BitSet(100)

  def this() = this(new SparkConf())

  def registerStartId(id: Int) = startIds.add(id)

  def registerEndId(id: Int) = endIds.add(id)

  override def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit) {
    super.runJob(rdd, func, partitions, allowLocal, resultHandler)
  }

  object implicits extends Serializable {

    implicit def rddToFlintRDDFunctions[T: ClassTag](rdd: RDD[T]): FlintRDDFunctions[T] = {
      new FlintRDDFunctions(rdd)
    }

  }

}
