package cn.edu.hust

import scala.pickling._
import json._

object PickleTest {
  def main(args: Array[String]) {
    println(List(42).pickle)
  }
}
