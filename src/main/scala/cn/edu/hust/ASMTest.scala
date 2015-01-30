package cn.edu.hust

import org.apache.log4j.{Level, Logger}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.objectweb.asm.Opcodes._
import org.objectweb.asm._

class FakeTaskContext extends TaskContext {
  override def isCompleted: Boolean = false

  override def addOnCompleteCallback(f: () => Unit) {}

  override def taskMetrics(): TaskMetrics = null

  override def isRunningLocally: Boolean = true

  override def isInterrupted: Boolean = false

  override def runningLocally(): Boolean = true

  override def partitionId(): Int = 0

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = this

  override def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext = this

  override def attemptId(): Long = 0

  override def stageId(): Int = 0
}


class Replacee {
  def newShow() = println("new")

  def show() = oldShow()

  def oldShow() = println("old")
}

object Generator {
  def generatePrinter() = {
    //定义一个叫做Example的类
    val cw = new ClassWriter(0)
    cw.visit(V1_1, ACC_PUBLIC, "Example", null, "java/lang/Object", null)

    //生成默认的构造方法
    var mw = cw.visitMethod(ACC_PUBLIC,
      "<init>",
      "()V",
      null,
      null)

    //生成构造方法的字节码指令
    mw.visitVarInsn(ALOAD, 0)
    mw.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V")
    mw.visitInsn(RETURN)
    mw.visitMaxs(1, 1)
    mw.visitEnd()

    //生成main方法
    mw = cw.visitMethod(ACC_PUBLIC + ACC_STATIC,
      "main",
      "([Ljava/lang/String;)V",
      null,
      null)

    //生成main方法中的字节码指令
    mw.visitFieldInsn(GETSTATIC,
      "java/lang/System",
      "out",
      "Ljava/io/PrintStream;")

    mw.visitLdcInsn("Hello world!")
    mw.visitMethodInsn(INVOKEVIRTUAL,
      "java/io/PrintStream",
      "println",
      "(Ljava/lang/String;)V")
    mw.visitInsn(RETURN)
    mw.visitMaxs(2, 2)

    //字节码生成完成
    mw.visitEnd()

    // 获取生成的class文件对应的二进制流
    val codes = cw.toByteArray


    //直接将二进制流加载到内存中
    //val loader = new MemoryClassLoader
    //loader.addClass("Example", codes)
  }
}

object ASMTest {
  def main(args: Array[String]) {
    val rewriter = ClassRewriter()
    rewriter.rewriteRDDIterator()
    rewriter.runAfterRewrite {
      val conf = new SparkConf().setAppName("Spark ASM Test").setMaster("local")
      val spark = new SparkContext(conf)
      spark.stop()

      Logger.getRootLogger.setLevel(Level.FATAL)

      val slices = 4
      val n = 60 * slices
      val rawData = spark.parallelize(1 to n, slices).cache()

      println(rawData.iterator(rawData.partitions(0), new FakeTaskContext).next())
    }

  }
}


