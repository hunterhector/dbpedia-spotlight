package org.dbpedia.spotlight.graph

import org.apache.commons.logging.LogFactory
import java.io.{PrintStream, FileOutputStream, OutputStream, File}
import io.Source
import org.dbpedia.spotlight.util.SimpleUtils

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/11/12
 * Time: 1:25 AM
 */

class CommonInLinkGraph {
  val LOG = LogFactory.getLog(this.getClass)

  def parseCooccsList(cooccsFile:File , hostMap:Map[String,Int] , integerListFile:File) = {
    LOG.info("Parsing Cooccurrences into Integer List")

    val ilfo: OutputStream = new FileOutputStream(integerListFile)
    val ilfoStream = new PrintStream(ilfo, true)

    Source.fromFile(cooccsFile).getLines().filterNot(line => line.trim() == "").foreach(
      line => {
        val fields = line.split("\\t")

        if (fields.length == 2){
          val srcUri = fields(0)
          val srcIdx = hostMap.getOrElse(srcUri,-1)
          if (srcIdx == -1)
            LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",srcUri))
          else{
            val targetMap = SimpleUtils.hadoopTuplesToMap(fields(1))
            targetMap.foreach{
              case(tarUri,cooccCount) => {
                val tarIdx = hostMap.getOrElse(tarUri,-1)
                if (tarIdx == -1)
                  LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",tarUri))
                else{
                  if (srcIdx != tarIdx){
                    //co-occurrences are bi-directional, but only save one direction may save space
                    val intString = srcIdx + "\t" + tarIdx + "\t" + getWeight(srcUri, tarUri,cooccCount)
                    ilfoStream.println(intString)
                  }else{
                    //LOG.warn("We don't particularly welcome self co-occurrences.")
                  }
                }
              }
            }
          }
        }else  LOG.error("Invailid line in file at \n -> \t" + line)
      })
  }

  private def getWeight(srcUri:String, targetUri: String, cooccCount:Int): Double = {
    return cooccCount
  }
}
