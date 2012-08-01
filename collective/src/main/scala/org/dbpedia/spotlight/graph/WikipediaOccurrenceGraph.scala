package org.dbpedia.spotlight.graph

import java.io.{PrintStream, FileOutputStream, OutputStream, File}
import io.Source
import org.dbpedia.spotlight.model.DBpediaResource
import org.apache.commons.logging.LogFactory
import collection.mutable
import com.officedepot.cdap2.collection.CompactHashMap


/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/13/12
 * Time: 4:37 PM
 */

/**
 * Create a Integer list to help build a occurrence graph (use GraphUtils$ to build)
 * Alongside, this class also create a Occurrences-Integer map (a one-to-one mapping) to retrieve the origin ocurrences
 */
class WikipediaOccurrenceGraph {
  val LOG = LogFactory.getLog(this.getClass)

  def parseOccsList(occsFile: File,  hostMap:CompactHashMap[String,Int], integerListFile: File) = {
    LOG.info("Parsing occurrences into Integer List")

    val arcMap = new mutable.HashMap[(Int,Int),Double]

    Source.fromFile(occsFile).getLines().filterNot(line => line.trim() == "").foreach(
      line => {
        var lineNum = 0
        val fields = line.split("\\t")
        if (fields.length == 5) {
          val id = fields(0)
          val targetUri = fields(1)
          val srcUri = id.split("-")(0)

          if (targetUri != srcUri){
            val weight = getWeight(new DBpediaResource(srcUri),new DBpediaResource(targetUri))

            val srcIndex = hostMap.getOrElse(srcUri,-1)
            val targetIndex = hostMap.getOrElse(targetUri,-1)

            if (srcIndex == -1)
              LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",srcUri))
            else if (targetIndex == -1)
              LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",targetUri))
            else{
                if (arcMap.contains((srcIndex,targetIndex))){
                  arcMap((srcIndex,targetIndex)) += weight
                }else arcMap((srcIndex,targetIndex)) = weight
            }
          }else{
            //LOG.warn("We don't particularly welcome self occurrences")
          }
        } else {
          LOG.error("Invailid line in file at \n -> \t" + line)
        }
        lineNum += 1
        if (lineNum % 100000 == 0) LOG.info(String.format("Store %s valid URIs and %s Links", lineNum.toString))
      })

    //output the arc map
    val ilfo: OutputStream = new FileOutputStream(integerListFile)
    val ilfoStream = new PrintStream(ilfo, true)
    arcMap.foreach{case ((src,tar),weight)=> ilfoStream.println(src+"\t"+tar+"\t"+weight)}
    ilfoStream.close()

    LOG.info(String.format("Integer List File saved at %s", integerListFile.getName))
  }

  private def getWeight(src: DBpediaResource, target: DBpediaResource): Double = {
    1.0
  }
}