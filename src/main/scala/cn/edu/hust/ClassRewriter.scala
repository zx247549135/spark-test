package cn.edu.hust

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}

import org.objectweb.asm.Opcodes._
import org.objectweb.asm._

class MemoryClassLoader(_parent: ClassLoader) extends ClassLoader(_parent) {

  def this() {
    this(ClassLoader.getSystemClassLoader())
  }

  private val cache = new collection.mutable.HashMap[String, Class[_]]

  private def parentDefineClass(name: String, b: Array[Byte], off: Int, len: Int) = {
    val cls = classOf[ClassLoader]
    val method = cls.getDeclaredMethod("defineClass", classOf[String], classOf[Array[Byte]], classOf[Int], classOf[Int])
    method.setAccessible(true)
    method.invoke(getParent, name, b, new Integer(off), new Integer(len)).asInstanceOf[Class[_]]
  }

  private def parentResolveClass(c: Class[_]) {
    val cls = classOf[ClassLoader]
    val method = cls.getDeclaredMethod("resolveClass", classOf[Class[_]])
    method.setAccessible(true)
    method.invoke(getParent, c).asInstanceOf[Unit]
  }

  def addClass(className: String, opCodes: Array[Byte], resolve: Boolean = false) = {
    val c = parentDefineClass(className, opCodes, 0, opCodes.length)
    if (resolve) parentResolveClass(c)
    c
  }
}

case class MethodInfo(acc: Int,
                      name: String,
                      desc: String,
                      signature: String,
                      exceptions: Array[String])

class FilterdMethodsRewriter(_cv: ClassVisitor,
                             rules: Seq[(MethodInfo => Boolean, MethodVisitor => MethodAdapter)])
  extends ClassAdapter(_cv) {

  override def visitMethod(acc: Int,
                           name: String,
                           desc: String,
                           signature: String,
                           exceptions: Array[String]): MethodVisitor = {
    var mv = cv.visitMethod(acc, name, desc, signature, exceptions)
    val info = MethodInfo(acc, name, desc, signature, exceptions)
    val ruleOpt = rules.find(_._1(info))
    ruleOpt.foreach(rule => mv = rule._2(mv))
    mv
  }
}

class ClassRewriter private(private val loader: MemoryClassLoader) {

  private def getClassReader(cls: Class[_]): ClassReader = getClassReader(cls.getName)

  private def getClassReader(className: String): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val classPath = className.replace(".", "/") + ".class"
    val resourceStream = loader.getResourceAsStream(classPath)

    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) return new ClassReader(resourceStream)

    val baos = new ByteArrayOutputStream(128)
    ClassRewriter.copyStream(resourceStream, baos, true)
    new ClassReader(new ByteArrayInputStream(baos.toByteArray))
  }

  def rewriteRDD() {
    val rules = Seq(
      ("<init>", new MethodAdapter(_: MethodVisitor) {
        override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
          if (name == "newRddId") {
            super.visitInsn(POP)
            super.visitLdcInsn(0)
          }
          else if (name == "getCallSite") {
            super.visitInsn(POP)
            super.visitInsn(ACONST_NULL)
          }
          else {
            super.visitMethodInsn(opcode, owner, name, desc)
          }
        }
      }),
      ("iterator", new MethodAdapter(_: MethodVisitor) {
        override def visitCode() {
          super.visitVarInsn(ALOAD, 0)
          super.visitVarInsn(ALOAD, 1)
          super.visitVarInsn(ALOAD, 2)
          super.visitMethodInsn(INVOKEVIRTUAL,
            "org/apache/spark/rdd/RDD",
            "compute",
            "(Lorg/apache/spark/Partition;Lorg/apache/spark/TaskContext;)Lscala/collection/Iterator;")
          super.visitInsn(Opcodes.ARETURN)
        }
      }))
    rewriteMethodsByName("org.apache.spark.rdd.RDD", rules)
  }

  def rewriteRDDId() {
    rewriteMethodByName("org.apache.spark.rdd.RDD", "<init>", new MethodAdapter(_) {
      override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
        if (name == "newRddId")
          super.visitLdcInsn(0)
        else
          super.visitMethodInsn(opcode, owner, name, desc)
      }
    })
  }

  def rewriteRDDIterator() {
    rewriteMethodByName("org.apache.spark.rdd.RDD", "iterator", new MethodAdapter(_) {
      override def visitCode() {
        super.visitVarInsn(ALOAD, 0)
        super.visitVarInsn(ALOAD, 1)
        super.visitVarInsn(Opcodes.ALOAD, 2)
        super.visitMethodInsn(INVOKEVIRTUAL,
          "org/apache/spark/rdd/RDD",
          "compute",
          "(Lorg/apache/spark/Partition;Lorg/apache/spark/TaskContext;)Lscala/collection/Iterator;")
        super.visitInsn(Opcodes.ARETURN)
      }
    })
  }

  def rewriteRDDIterator2() {
    val className = "org.apache.spark.rdd.RDD"
    val cr = getClassReader(className)
    val cw = new ClassWriter(cr,
      ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES)
    val cv = new ClassAdapter(cw) {
      override def visitMethod(acc: Int,
                               name: String,
                               desc: String,
                               signature: String,
                               exceptions: Array[String]): MethodVisitor = {
        var newName = name
        if (name == "iterator") {
          newName = "origin$" + name
          val mv = cv.visitMethod(acc, name, desc, signature, exceptions)
          mv.visitCode()
          mv.visitVarInsn(ALOAD, 0)
          mv.visitVarInsn(ALOAD, 1)
          mv.visitVarInsn(ALOAD, 2)
          mv.visitMethodInsn(INVOKEVIRTUAL,
            "org/apache/spark/rdd/RDD",
            "compute",
            "(Lorg/apache/spark/Partition;Lorg/apache/spark/TaskContext;)Lscala/collection/Iterator;")
          mv.visitInsn(Opcodes.ARETURN)
          mv.visitMaxs(3, 3)
          mv.visitEnd()
        }
        super.visitMethod(acc, newName, desc, signature, exceptions)

      }
    }
    cr.accept(cv, 0)
    val codes = cw.toByteArray
    loader.addClass(className, codes)
  }

  def rewriteSparkContextConstructor() {
    rewriteMethod("org.apache.spark.SparkContext", info => info.name == "<init>" && info.desc == "()V", new MethodAdapter(_) {
      override def visitCode() {
        super.visitVarInsn(ALOAD, 0)
        super.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V")
        super.visitInsn(RETURN)
      }
    })
  }

  def printRDDCompute() {
    val cr = getClassReader("org.apache.spark.rdd.RDD")
    val cw = new ClassWriter(cr,
      ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES)
    val cv = new ClassAdapter(cw) {
      override def visitMethod(acc: Int,
                               name: String,
                               desc: String,
                               signature: String,
                               exceptions: Array[String]): MethodVisitor = {
        if (name == "compute") println(desc)
        cv.visitMethod(acc, name, desc, signature, exceptions)
      }
    }
    cr.accept(cv, 0)
    val codes = cw.toByteArray
  }

  def replaceMethodCall(className: String,
                        callSite: String,
                        oldCall: String,
                        newCall: String) = {
    rewriteMethodByName(className, callSite, new MethodAdapter(_) {
      override def visitMethodInsn(opcode: Int, owner: String, name: String, desc: String) {
        if (name == oldCall)
          super.visitMethodInsn(opcode, owner, newCall, desc)
        else
          super.visitMethodInsn(opcode, owner, name, desc)
      }
    })
  }

  def rewriteMethod(className: String,
                    predicate: MethodInfo => Boolean,
                    adapterBuilder: MethodVisitor => MethodAdapter) {
    rewriteMethods(className, Seq((predicate, adapterBuilder)))
  }

  def rewriteMethods(className: String,
                     rules: Seq[(MethodInfo => Boolean, MethodVisitor => MethodAdapter)]) {
    val cr = getClassReader(className)
    val cw = new ClassWriter(cr,
      ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES)
    val cv = new FilterdMethodsRewriter(cw, rules)
    cr.accept(cv, 0)
    val codes = cw.toByteArray
    loader.addClass(className, codes)
  }

  def rewriteMethodByName(className: String,
                          methodName: String,
                          adapterBuilder: MethodVisitor => MethodAdapter) {
    rewriteMethod(className, _.name == methodName, adapterBuilder)
  }

  def rewriteMethodsByName(className: String,
                           rules: Seq[(String, MethodVisitor => MethodAdapter)]) {
    rewriteMethods(className, rules.map(rule => ((info: MethodInfo) => info.name == rule._1, rule._2)))
  }

}

object ClassRewriter {

  def apply() = new ClassRewriter(new MemoryClassLoader)

  def apply(parentLoader: ClassLoader) = new ClassRewriter(new MemoryClassLoader(parentLoader))

  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false): Long = {
    try {
      var count = 0L
      val buf = new Array[Byte](8192)
      var n = 0
      while (n != -1) {
        n = in.read(buf)
        if (n != -1) {
          out.write(buf, 0, n)
          count += n
        }
      }
      count
    } finally {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }
}
