package com.soy.sparkapp.common

import java.io.File
import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}

import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedWebappClassLoader


/**
  * author  soy
  * version  [版本号, 2018/9/5]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object ExtClasspathLoader {

  val addURLHandler: Method = initAddMethod

  val classloader = ClassLoader.getSystemClassLoader()


  def initAddMethod: Method = {
    val add: Method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    add.setAccessible(true)
    add
  }

  def loadClasspath: Unit = {
    val jarFiles = getJarFile
    for(file <- jarFiles){
      loadClasspath(file)
    }
  }

  def loadClasspath(filePath: String): Unit = {
    val file = new File(filePath)
    loopFiles(file)
  }

  def loopFiles(file: File): Unit = {
    if(file.isDirectory){
      val fileList = file.listFiles()
      for(x <- fileList){
        loopFiles(x)
      }
    }else{
      if(file.getAbsolutePath.endsWith(".jar")){
        addURL(file)
      }
    }

  }

  def getJarFile: List[String] = {
    val prefix = "/opt/jars"
    val jars = List("phoenix-4.14.0-HBase-1.4-hive.jar")
    jars.map(prefix + _)
  }

  def addURL(file: File) = {
    val uDir = file.toURI().toURL()
    val uDirObj = uDir match {
      case o : Object => o
      case _ => throw new Exception( "impossible" )
    }
    //addURLHandler.invoke(classloader, uDirObj)
    val sparkClassLoader: TomcatEmbeddedWebappClassLoader = getContextOrSparkClassLoader.asInstanceOf[TomcatEmbeddedWebappClassLoader]
    addURLHandler.invoke(sparkClassLoader,uDirObj)
  }

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def main(args: Array[String]): Unit = {
    ExtClasspathLoader.loadClasspath
    val a1 = Class.forName("org.apache.phoenix.hive.PhoenixSerDe", true, classloader)
    val b1 = a1.newInstance()
    println(b1)
  }

}
