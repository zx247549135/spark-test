package cn.edu.hust

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}

import org.objectweb.asm._

class MemoryClassLoader extends ClassLoader {
  val cache = new collection.mutable.HashMap[String, Class[_]]

  def addClass(className: String, opCodes: Array[Byte]) = {
    try {
      if (!cache.contains(className))
        cache(className) = defineClass(className, opCodes, 0, opCodes.length)
      cache(className)
    } catch {
      case e: SecurityException => null
    }
  }

  @throws(classOf[ClassNotFoundException])
  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    getClassLoadingLock(name).synchronized {
      var c: Class[_] = findLoadedClass(name)
      if (c == null) {
        c = cache.getOrElse(name, null)
        if (c == null && (name.startsWith("org.apache.spark") || name.startsWith("cn.edu.hust"))) {
          val classPath = name.replace(".", "/") + ".class"
          val resourceStream = getResourceAsStream(classPath)
          if (resourceStream != null) {
            val baos = new ByteArrayOutputStream(128)
            ClassRewriter.copyStream(resourceStream, baos, true)
            c = addClass(name, baos.toByteArray)
          }
        }
        if (c == null)
          c = super.loadClass(name, resolve)
      }
      if (resolve) {
        resolveClass(c)
      }
      return c
    }
  }
}

case class MethodInfo(acc: Int,
                      name: String,
                      desc: String,
                      signature: String,
                      exceptions: Array[String])

class FilterdMethodRewriter(_cv: ClassVisitor,
                            predicate: MethodInfo => Boolean,
                            adapterBuilder: MethodVisitor => MethodAdapter)
  extends ClassAdapter(_cv) {

  override def visitMethod(acc: Int,
                           name: String,
                           desc: String,
                           signature: String,
                           exceptions: Array[String]): MethodVisitor = {
    var mv = cv.visitMethod(acc, name, desc, signature, exceptions)
    val info = MethodInfo(acc, name, desc, signature, exceptions)
    if (predicate(info))
      mv = adapterBuilder(mv)
    mv
  }
}

class ClassRewriter {

  val loader = new MemoryClassLoader

  Thread.currentThread().setContextClassLoader(loader)

  def loadClass(cls: Class[_]): Class[_] = loadClass(cls.getName)

  def loadClass(className: String): Class[_] = {
    val classPath = className.replace(".", "/") + ".class"
    val classStream = loader.getResourceAsStream(classPath)
    val baos = new ByteArrayOutputStream(128)
    ClassRewriter.copyStream(classStream, baos, true)
    val result = loader.addClass(className, baos.toByteArray)

    Seq("$$anon$1", "$$anonfun$main$1").foreach(postfix => {
      val classPath = className.replace(".", "/") + postfix + ".class"
      val classStream = loader.getResourceAsStream(classPath)
      if (classStream != null) {
        val baos = new ByteArrayOutputStream(128)
        ClassRewriter.copyStream(classStream, baos, true)
        loader.addClass(className + postfix, baos.toByteArray)
      }
    })

    result
  }

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

  def runAfterRewrite(body: => Unit) {
    val box = () => body
    val bodyClass = loadClass(box.getClass)
    val bodyInstance = bodyClass.newInstance()
    val applyMethod = bodyClass.getMethod("apply")
    applyMethod.invoke(bodyInstance)
  }

  def rewriteRDDIterator() {
    rewriteMethodByName("org.apache.spark.rdd.RDD", "iterator", new MethodAdapter(_) {
      override def visitCode() {
        super.visitVarInsn(Opcodes.ALOAD, 0)
        super.visitVarInsn(Opcodes.ALOAD, 1)
        super.visitVarInsn(Opcodes.ALOAD, 2)
        super.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
          "org/apache/spark/rdd/RDD",
          "compute",
          "(Lorg/apache/spark/Partition;Lorg/apache/spark/TaskContext;)Lscala/collection/Iterator;")
        mv.visitInsn(Opcodes.ARETURN)
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
          mv.visitVarInsn(Opcodes.ALOAD, 0)
          mv.visitVarInsn(Opcodes.ALOAD, 1)
          mv.visitVarInsn(Opcodes.ALOAD, 2)
          mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
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

  def printRDDCompute(): Unit = {
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

  def rewriteMethodByName(className: String,
                          methodName: String,
                          adapterBuilder: MethodVisitor => MethodAdapter) {
    val cr = getClassReader(className)
    val cw = new ClassWriter(cr,
      ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES)
    val cv = new FilterdMethodRewriter(cw, _.name == methodName, adapterBuilder)
    cr.accept(cv, 0)
    val codes = cw.toByteArray
    loader.addClass(className, codes)
  }

}

object ClassRewriter {

  def apply() = new ClassRewriter

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
