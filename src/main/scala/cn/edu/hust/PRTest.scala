package cn.edu.hust

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
 
import SparkContext._

class EdgeChunk(size: Int = 4196) extends ByteArrayOutputStream(size) { self =>
  
  def show() {
    var offset = 0
    while (offset < count) {
      println("src id: " + WritableComparator.readLong(buf, offset))
      offset += 8
      val numDests = WritableComparator.readInt(buf, offset)
      offset += 4
      print("has " + numDests + " dests:")
      var count = 0
      while (count < numDests) {
        print(" " + WritableComparator.readLong(buf, offset))
        offset += 8
        count += 1
      }
      println("")
    }
  }

  def getInitValueIterator(value: Double) = new Iterator[(Long, Double)] {
    var offset = 0
    
    override def hasNext = offset < self.count
    
    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        val srcId = WritableComparator.readLong(buf, offset)
        offset += 8
        val numDests = WritableComparator.readInt(buf, offset)
        offset += 4 + 8 * numDests
        (srcId, value)
      }
    }
  }
  
  def getMessageIterator(vertices: Iterator[(Long, Double)]) = new Iterator[(Long, Double)] {
    var changeVertex = true
    var currentVertex: (Long, Double) = _
    var offset = 0
    var currentDestIndex = 0
    var currentDestNum = 0

    override def hasNext = !changeVertex || vertices.hasNext

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        if (changeVertex) {
          currentVertex = vertices.next()
          while (currentVertex._1 != WritableComparator.readLong(buf, offset)) {
            offset += 8
            val numDests = WritableComparator.readInt(buf, offset)
            offset += 4 + 8 * numDests
          }
          offset += 8
          currentDestNum = WritableComparator.readInt(buf, offset)
          offset += 4
          currentDestIndex = 0
          changeVertex = false
        }

        currentDestIndex += 1
        if (currentDestIndex == currentDestNum) changeVertex = true

        val destId = WritableComparator.readLong(buf, offset)
        offset += 8
        
        (destId, currentVertex._2)
      }
    }
    
  }
}

object PRTest {
  private val ordering = implicitly[Ordering[Long]]
  private val iters = 3
  
  def testNative(links: RDD[(Long, Iterable[Long])]): Unit = {
    links.cache().foreach(_ => Unit)

    val initRanks = links.mapValues(v => 1.0)

    var ranks = initRanks
    
    val startTime = System.currentTimeMillis
    for (i <- 1 to iters) {
      val contribs = links.join(ranks, links.partitioner.get).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.foreach(_ => Unit)
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
    
  }
  
  def testOptimized(groupedEdges: RDD[(Long, Iterable[Long])]) {
    val cachedEdges = groupedEdges.mapPartitions { iter =>
      val chunk = new EdgeChunk
      val dos = new DataOutputStream(chunk)
      for ((src, dests) <- iter) {
        dos.writeLong(src)
        dos.writeInt(dests.size)
        dests.foreach(dos.writeLong)
      }
      Iterator(chunk)
    }.cache()

    cachedEdges.foreach(_ => Unit)
    
    val initRanks = cachedEdges.mapPartitions{ iter =>
      val chunk = iter.next()
      chunk.getInitValueIterator(1.0)
    }

    var ranks = initRanks
    
    val startTime = System.currentTimeMillis
    for (i <- 1 to iters) {
      val contribs = cachedEdges.zipPartitions(ranks) { (EIter, VIter) =>
        val chunk = EIter.next()
        chunk.getMessageIterator(VIter)
      }
      ranks = contribs.reduceByKey(_ + _).asInstanceOf[ShuffledRDD[Long, _, _]].
        setKeyOrdering(ordering).
        asInstanceOf[RDD[(Long, Double)]].
        mapValues(0.15 + 0.85 * _)
    }
    ranks.foreach(_ => Unit)
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
        
  }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark PageRank Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 4
    val n: Long = 120000 * slices
    val edgesPerVert = 40
    val edges = spark.parallelize(0L until n, slices).flatMap ( x => {
      val result = new Array[(Long, Long)](edgesPerVert)
      for (i <- 0 until edgesPerVert) {
        result(i) = (x, (x + slices.toLong + i.toLong) % n)
      }
      result
    }).groupByKey().
      asInstanceOf[ShuffledRDD[Long, _, _]].
      setKeyOrdering(ordering).
      asInstanceOf[RDD[(Long, Iterable[Long])]]

    testNative(edges)
    //testOptimized(edges)
    
    spark.stop()
  }
}
