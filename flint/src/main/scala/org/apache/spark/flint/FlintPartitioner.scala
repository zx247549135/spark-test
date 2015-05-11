package org.apache.spark.flint

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

/**
 * Created by Administrator on 2015/5/11.
 */

class FlintPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = throw new UnsupportedOperationException

  override def equals(other: Any): Boolean = other match {
    case h: FlintPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}