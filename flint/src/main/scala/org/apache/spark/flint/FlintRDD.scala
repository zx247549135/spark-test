package org.apache.spark.flint

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * TODO
 */

private[spark] class FlintRDDPartition(val idx: Int) extends Partition {
  override val index = idx
  override def hashCode(): Int = idx
}

private[spark] class FlintRDD[T: ClassTag, U](
    @transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]],
    @transient origin: RDD[T],
    numPartitions: Int,
    generatedLoopFunc:  Option[(Partition, TaskContext) => U] = None,
    generatedIterator: Option[(Partition, TaskContext) => Iterator[T]] = None)
  extends RDD[T](sc, deps) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    if (generatedIterator.isDefined) {
      generatedIterator.get(split, context)
    } else {
      throw new UnsupportedOperationException
    }
  }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numPartitions)(i => new FlintRDDPartition(i))
  }

  @DeveloperApi
  def execute(split: Partition, context: TaskContext): U = {
    if (generatedLoopFunc.isDefined) {
      generatedLoopFunc.get(split, context)
    } else {
      throw new UnsupportedOperationException
    }
  }

  val useFlintTask: Boolean = generatedLoopFunc.isDefined
}
