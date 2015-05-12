package org.apache.spark.flint

import java.nio.ByteBuffer

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler.{Task, TaskLocation}

/**
 * Created by Administrator on 2015/5/12.
 */
private[spark] class FlintTask[T, U](
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[T](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rdd = ser.deserialize[FlintRDD[T, U]](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    try {
      rdd.execute(partition, context)
    } catch {
      case e: Exception =>
        log.debug("Could not stop writer", e)
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "FlintTask(%d, %d)".format(stageId, partitionId)
}
