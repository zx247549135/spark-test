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

private[spark] class FlintRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient deps: Seq[FlintDependency[_, _, _]],
    numPartitions: Int)
  extends RDD[T](sc, deps) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = throw new UnsupportedOperationException

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numPartitions)(i => new FlintRDDPartition(i))
  }
}
