package org.dbpedia.spotlight.graph

import io.Source
import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.DBpediaResource
import java.io.{PrintStream, FileOutputStream, OutputStream, File}

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/11/12
 * Time: 5:49 PM
 *
 * Store a graph storing all co-occurrences in Wikipedia
 *
 * @author hector.liu
 */

/**
 * Create a Integer list to help build a co-occurrence graph (use GraphUtils$ to build)
 * This class will use the Ocurrence-Integer map created by WikipediaOccurrenceGraph for consistence
*/
class WikipediaCooccurrencesGraph {
  val LOG = LogFactory.getLog(this.getClass)



  private def hadoopTuplesToMap(bagString:String):Map[String,Int] = {
     var targetMap = Map.empty[String,Int]
     val pattern = """\((.*?),(.*?)\)""".r
     for(m <- pattern.findAllIn(bagString).matchData) {
      // LOG.info(m.group(1)+"\t"+m.group(2))
       targetMap += (m.group(1) -> m.group(2).toInt)
     }
    targetMap
  }

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
             val targetMap = hadoopTuplesToMap(fields(1))
             targetMap.foreach{
               case(tarUri,cooccCount) => {
                 val tarIdx = hostMap.getOrElse(tarUri,-1)
                 if (tarIdx == -1)
                   LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",tarUri))
                 else{
                   //co-occurrences are bi-directional, but only save one direction may save space
                   val intString = srcIdx + "\t" + tarIdx + "\t" + getWeight(srcUri, tarUri,cooccCount)

                   ilfoStream.println(intString)
                 }
               }
             }
           }
         }else  LOG.error("Invailid line in file at \n -> \t" + line)
      })
  }

  //may have some variation here
  private def getWeight(srcUri:String, targetUri: String, cooccCount:Int): Double = {
    return cooccCount
  }
}