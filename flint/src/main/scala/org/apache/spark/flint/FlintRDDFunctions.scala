package org.apache.spark.flint

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * FIXME Created by Administrator on 2015/5/7.
 */
class FlintRDDFunctions[T: ClassTag](self: RDD[T]) extends Logging with Serializable {

  def withFlint(): RDD[T] = {
    self
  }

}
