package cn.edu.hust

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
 
import SparkContext._
import org.apache.spark.storage.StorageLevel

class EdgeChunk(size: Int = 4196) extends ByteArrayOutputStream(size) { self =>
  
  def show() {
    var offset = 0
    while (offset < count) {
      println("src id: " + WritableComparator.readInt(buf, offset))
      offset += 4
      val numDests = WritableComparator.readInt(buf, offset)
      offset += 4
      print("has " + numDests + " dests:")
      var count = 0
      while (count < numDests) {
        print(" " + WritableComparator.readInt(buf, offset))
        offset += 4
        count += 1
      }
      println("")
    }
  }

  def getInitValueIterator(value: Float) = new Iterator[(Int, Float)] {
    var offset = 0
    
    override def hasNext = offset < self.count
    
    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        val srcId = WritableComparator.readInt(buf, offset)
        offset += 4
        val numDests = WritableComparator.readInt(buf, offset)
        offset += 4 + 4 * numDests
        (srcId, value)
      }
    }
  }
  
  def getMessageIterator(vertices: Iterator[(Int, Float)]) = new Iterator[(Int, Float)] {
    var changeVertex = true
    var offset = 0
    var currentDestIndex = 0
    var currentDestNum = 0
    var currentContrib = 0.0f

    override def hasNext = !changeVertex || vertices.hasNext

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        if (changeVertex) {
          val currentVertex = vertices.next()
          while (currentVertex._1 != WritableComparator.readInt(buf, offset)) {
            offset += 4
            val numDests = WritableComparator.readInt(buf, offset)
            offset += 4 + 4 * numDests
          }
          offset += 4
          currentDestNum = WritableComparator.readInt(buf, offset)
          offset += 4
          currentDestIndex = 0
          currentContrib = currentVertex._2 / currentDestNum
          changeVertex = false
        }

        currentDestIndex += 1
        if (currentDestIndex == currentDestNum) changeVertex = true

        val destId = WritableComparator.readInt(buf, offset)
        offset += 4
        
        (destId, currentContrib)
      }
    }
    
  }

  def getMessageIterator2(vertices: Iterator[(Long, Double)]) = new Iterator[(Long, Double)] {
    var changeVertex = true
    var offset = 0
    var currentDestIndex = 0
    var currentDestNum = 0
    var currentContrib = 0.0

    override def hasNext =  {
      if (offset >= self.count) {
        while (vertices.hasNext)
          vertices.next()
      }
      offset < self.count
    }

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        if (changeVertex) {
          WritableComparator.readLong(buf, offset)
          offset += 8
          currentDestNum = WritableComparator.readInt(buf, offset)
          offset += 4
          currentDestIndex = 0
          currentContrib = 1.0 / currentDestNum
          changeVertex = false
        }

        currentDestIndex += 1
        if (currentDestIndex == currentDestNum) changeVertex = true

        val destId = WritableComparator.readLong(buf, offset)
        offset += 8

        (destId, currentContrib)
      }
    }

  }
  
}

object PRTest {
  private val ordering = implicitly[Ordering[Int]]
  private val iters = 3
  
  def testNative(links: RDD[(Int, Iterable[Int])]) {
    links.persist(StorageLevel.MEMORY_ONLY).foreach(_ => Unit)

    val initRanks = links.mapValues(v => 1.0f)

    var ranks = initRanks
    
    val startTime = System.currentTimeMillis
    for (i <- 1 to iters) {
      val contribs = links.join(ranks, links.partitioner.get).values.flatMap { case (urls, rank) =>
        val contrib = rank / urls.size
        urls.map(url => (url, contrib))
      }
      ranks = contribs.reduceByKey(links.partitioner.get, _ + _).mapValues(0.15f + 0.85f * _)
    }
    ranks.foreach(_ => Unit)
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
    
  }
  
  def testOptimized(groupedEdges: RDD[(Int, Iterable[Int])]) {
    val cachedEdges = groupedEdges.mapPartitions ( iter => {
      val chunk = new EdgeChunk
      val dos = new DataOutputStream(chunk)
      for ((src, dests) <- iter) {
        dos.writeInt(src)
        dos.writeInt(dests.size)
        dests.foreach(dos.writeInt)
      }
      Iterator(chunk)
    }, true).cache()

    cachedEdges.foreach(_ => Unit)
    
    val initRanks = cachedEdges.mapPartitions( iter => {
      val chunk = iter.next()
      chunk.getInitValueIterator(1.0f)
    }, true)

    var ranks = initRanks
    
    val startTime = System.currentTimeMillis
    for (i <- 1 to iters) {
      val contribs = cachedEdges.zipPartitions(ranks) { (EIter, VIter) =>
        val chunk = EIter.next()
        chunk.getMessageIterator(VIter)
      }
      ranks = contribs.reduceByKey(cachedEdges.partitioner.get, _ + _).asInstanceOf[ShuffledRDD[Int, _, _]].
        setKeyOrdering(ordering).
        asInstanceOf[RDD[(Int, Float)]].
        mapValues(0.15f + 0.85f * _)
    }
    ranks.foreach(_ => Unit)
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
        
  }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark PageRank Test").
      setMaster("local").
      set("spark.shuffle.memoryFraction", "0.6")
    val spark = new SparkContext(conf)

    //Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 2
    val n = 120000 * slices
    val edgesPerVert = 50
    val edges = spark.parallelize(0 until n, slices).flatMap ( x => {
      val result = new Array[(Int, Int)](edgesPerVert)
      for (i <- 0 until edgesPerVert) {
        result(i) = (x, (x + slices + i) % n)
      }
      result
    }).groupByKey().
      asInstanceOf[ShuffledRDD[Int, _, _]].
      setKeyOrdering(ordering).
      asInstanceOf[RDD[(Int, Iterable[Int])]]

    testNative(edges)
    //testOptimized(edges)
    
    spark.stop()
  }
}
