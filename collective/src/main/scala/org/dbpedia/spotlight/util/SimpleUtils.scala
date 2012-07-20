package org.dbpedia.spotlight.util

import java.io.File
import net.sf.json.JSONObject

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/11/12
 * Time: 1:01 AM
 */

object SimpleUtils {
  def hadoopTuplesToMap(bagString:String):Map[String,Int] = {
    var targetMap = Map.empty[String,Int]
    val pattern = """\((.*?),(.*?)\)""".r
    for(m <- pattern.findAllIn(bagString).matchData) {
      // LOG.info(m.group(1)+"\t"+m.group(2))
      targetMap += (m.group(1) -> m.group(2).toInt)
    }
    targetMap
  }

  def mapFromJSONFile(srcFile:File):Map[String,Int] ={
    val srcString = io.Source.fromFile(srcFile).mkString
    mapFromJSONString(srcString)
  }

  //TODO: method to read JSON as Map
  def mapFromJSONString(str:String):Map[String,Int] ={
    val jsonObj = JSONObject.fromObject(str)
    null
  }

  def createDir(dirName:String){
    val theDir = new File(dirName)
    if (!theDir.exists())
    {
      theDir.mkdir()
    }
  }

  def findCommonInSortedArray(a:Array[Int],b:Array[Int]):Int = {
    var count = 0
    var pa = 0
    var pb = 0
    val lenA = a.length
    val lenB = b.length
    while((pa<lenA) && (pb<lenB))
      if (a(pa) == b(pb)){
        count += 1
        pa += 1
        pb += 1
      }else if (a(pa) > b(pb)){
        pb += 1
      }else{
        pa += 1
      }
    count
  }

  /*
  *Test the functions in the class
   */
  def main(args:Array[String]){
    println("Checking the functions!")
    val range = 0 to 100000

    for (i <- range) {
      val rnd = new scala.util.Random()
      val a:Array[Int] = new Array[Int](rnd.nextInt(range.length))
      val b:Array[Int] = new Array[Int](rnd.nextInt(range.length))

      for (index <- 0 to a.length -1 ) a(index) = (rnd.nextInt(range.length))
      for (index <- 0 to b.length -1 ) b(index) = (rnd.nextInt(range.length))

      findCommonInSortedArray(a,b)

      if (i%10000 == 0) println(i)
    }

  }
}