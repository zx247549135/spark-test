package cn.edu.hust

import soot.jimple.InvokeExpr
import soot.jimple.internal.{JimpleLocal, AbstractVirtualInvokeExpr, InvokeExprBox}
import soot.options.Options
import soot.{Scene, SootClass}

import scala.collection.JavaConversions._

object SootTest {

  def main(args: Array[String]){
    
    def sootInit() {
      Options.v.set_soot_classpath(System.getProperty("java.class.path"))
      Options.v.set_coffi(false)
      Options.v.set_src_prec(Options.src_prec_only_class)
      Options.v.set_on_the_fly(true)
      Scene.v.loadNecessaryClasses
    }

    def sootLoad(cls: String) = {
      val clazz = Scene.v.forceResolve(cls, SootClass.BODIES)
      clazz.setApplicationClass
      clazz
    }

    
    sootInit()
    val mainClass = sootLoad(SparkPi.getClass.getName)
    
    val mainBody = mainClass.getMethodByName("main").retrieveActiveBody
    val userFuncs = mainBody.getUnits.flatMap(_.getUseBoxes.map(_.getValue).filter(_.isInstanceOf[AbstractVirtualInvokeExpr]).flatMap( x => {
      val invoke = x.asInstanceOf[AbstractVirtualInvokeExpr]
      val method = invoke.getMethodRef
      val returnType = method.returnType.toString
      val owner = method.declaringClass.getName
      if (owner == "org.apache.spark.rdd.RDD" || returnType == "org.apache.spark.rdd.RDD") {
        val indices = method.parameterTypes().zipWithIndex.filter(_._1.toString.startsWith("scala.Function")).map(_._2)
        indices.map( index => {
          invoke.getArgs.get(index).asInstanceOf[JimpleLocal].getType.toString
        })
      } else {
        Nil
      }
    }))
    
    userFuncs.foreach( f => {
      val funcClass = sootLoad(f)
      val methods = funcClass.getMethods.filter(_.getName startsWith "apply")
      println(methods.map(_.retrieveActiveBody).mkString("\n"))
    })
    
    
//    val source = SourceLocator.v.getClassSource(cl)
//    clazz = Scene.v.getSootClass(cl)
//    clazz.setResolvingLevel(SootClass.BODIES)
//    source.resolve(clazz)
//    
//    val writerOut = new PrintWriter(new EscapedWriter(new OutputStreamWriter(System.out)))
//    Printer.v.printTo(clazz, writerOut)
  }
}
