package org.apache.spark.flint

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * TODO
 */
class FlintRDDFunctions[T: ClassTag](self: RDD[T]) extends Logging with Serializable {

  def withFlint(): RDD[T] = {
    self.sparkContext.asInstanceOf[FlintContext].registerStartId(self.id)
    self
  }

  def withoutFlint(): RDD[T] = {
    self.sparkContext.asInstanceOf[FlintContext].registerEndId(self.id)
    self
  }

}
