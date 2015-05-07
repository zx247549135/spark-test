package org.apache.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by Administrator on 2015/5/7.
 */
package object flint {

  object implicits extends Serializable {

    implicit def rddToFlintRDDFunctions[T: ClassTag](rdd: RDD[T]): FlintRDDFunctions[T] = {
      new FlintRDDFunctions(rdd)
    }
  }

}
