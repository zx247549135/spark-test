package cn.edu.hust

import java.io.{OutputStreamWriter, PrintWriter}

import soot.options.Options
import soot.util.EscapedWriter
import soot.{Printer, SourceLocator, Scene, SootClass}

object SootTest {

  def main(args: Array[String]){
    
    val cl = SparkPi.getClass.getName
    println(cl)
    
    Options.v.set_soot_classpath(System.getProperty("java.class.path"))
    Options.v.set_coffi(false)
    Options.v.set_src_prec(Options.src_prec_only_class)
    Options.v.set_on_the_fly(true)

    var clazz = Scene.v.forceResolve(cl, SootClass.SIGNATURES)
    clazz.setApplicationClass
    
    val source = SourceLocator.v.getClassSource(cl)
    clazz = Scene.v.getSootClass(cl)
    clazz.setResolvingLevel(SootClass.BODIES)
    source.resolve(clazz)
    
    val writerOut = new PrintWriter(new EscapedWriter(new OutputStreamWriter(System.out)))
    Printer.v.printTo(clazz, writerOut)
  }
}
