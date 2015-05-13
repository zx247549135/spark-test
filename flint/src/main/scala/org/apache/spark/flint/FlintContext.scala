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

  private[flint] def registerStartId(id: Int) {
    if (endIds.contains(id))
      endIds.remove(id)
    else
      startIds.add(id)
  }

  private[flint] def registerEndId(id: Int) {
    if (startIds.contains(id))
      startIds.remove(id)

    endIds.add(id)
  }

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
