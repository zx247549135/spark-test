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
    @transient deps: Seq[FlintDependency[_, _, _]],
    numPartitions: Int,
    loopFunc:  (Partition, TaskContext) => U,
    loopIterator: Option[(Partition, TaskContext) => Iterator[T]] = None)
  extends RDD[T](sc, deps) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    if (loopIterator.isDefined) {
      loopIterator.get(split, context)
    } else {
      throw new UnsupportedOperationException
    }
  }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numPartitions)(i => new FlintRDDPartition(i))
  }

  def execute(split: Partition, context: TaskContext): U = {
    loopFunc(split, context)
  }
}
