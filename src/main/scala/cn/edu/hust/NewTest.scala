package cn.edu.hust

object NewTest {
  
  var state1 = 1
  var state2 = 1
  
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis

    var index = 0
    while (index < 100000000) {
      state1 += 1
      state2 += 1
      index += 1
    }
    
    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")
  }
}
