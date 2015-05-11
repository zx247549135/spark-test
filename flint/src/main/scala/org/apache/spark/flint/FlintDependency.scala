package org.apache.spark.flint

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2015/5/11.
 */
class FlintDependency[K, V, C](
  @transient _rdd: RDD[_ <: Product2[K, V]],
  _partitioner: FlintPartitioner) extends ShuffleDependency[K, V, C](_rdd, _partitioner)


