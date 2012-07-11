package org.dbpedia.spotlight.util

import java.io.File

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

  def createDir(dirName:String){
    val theDir = new File(dirName)
    if (!theDir.exists())
    {
      theDir.mkdir()
    }
  }
}
