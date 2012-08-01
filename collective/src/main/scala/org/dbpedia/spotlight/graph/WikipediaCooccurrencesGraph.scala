package org.dbpedia.spotlight.graph

import io.Source
import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.DBpediaResource
import java.io.{PrintStream, FileOutputStream, OutputStream, File}
import org.dbpedia.spotlight.util.{GraphUtils, GraphConfiguration, SimpleUtils}
import org.json.JSONObject
import sun.util.calendar.LocalGregorianCalendar
import org.json
import com.officedepot.cdap2.collection.CompactHashMap

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

  def parsePigStorageFile(cooccsFile:File,hostMap:CompactHashMap[String,Int],ilfoStream:PrintStream){
    var count = 0
    Source.fromFile(cooccsFile).getLines().filterNot(line => line.trim() == "").foreach(
      line => {
        val fields = line.split("\\t")

        if (fields.length == 2){
          val srcUri = fields(0)
          val srcIdx = hostMap.getOrElse(srcUri,-1)
          if (srcIdx == -1){
            LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",srcUri))
          }else{
            val targetMap = SimpleUtils.hadoopTuplesToMap(fields(1))
            targetMap.foreach{
              case(tarUri,cooccCount) => {
                val tarIdx = hostMap.getOrElse(tarUri,-1)
                if (tarIdx == -1){
                  LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",tarUri))
                }else{
                  if (srcIdx != tarIdx){
                    //co-occurrences are bi-directional, but only save one direction may save space
                    val intString = srcIdx + "\t" + tarIdx + "\t" + cooccCount
                    ilfoStream.println(intString)
                  }else{
                    //LOG.warn("We don't particularly welcome self co-occurrences.")
                  }
                }
              }
            }
          }
        }else  LOG.error("Invailid line in file at \n -> \t" + line)
        count += 1
        if (count % 100000 == 0) LOG.info(String.format("%s lines processed.",count.toString))
      })
  }

  def parseJSONStorageFile(cooccsFile:File,hostMap:CompactHashMap[String,Int],ilfoStream:PrintStream) {
    var count = 0

    val fileList =
    if (cooccsFile.isDirectory){
      cooccsFile.listFiles().filter((f=> SimpleUtils.isPigPartFile(f))).sorted.toList
    }  else{
      List(cooccsFile)
    }

    fileList.foreach(f => (Source.fromFile(f).getLines().filterNot(l => l.trim() == "").foreach(
      line =>{
        val js = new json.JSONObject(line)

        val srcUri = js.getString("src")
        val srcIdx = hostMap.getOrElse(srcUri,-1)

        if (srcIdx == -1)
          LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",srcUri))
        else{
          val countArray = js.getJSONArray("counts")
          val len = countArray.length()
          (0 to len -1).foreach(i => {
            val countPair = countArray.getJSONObject(i)
            if (countPair != null){

              val tarUri = countPair.getString("tar")
              val count = countPair.getString("count")
              val tarIdx = hostMap.getOrElse(tarUri,-1)

              if (tarIdx == -1){
                LOG.error(String.format("Uri [%s] was not found in host map, if this happens a lot, something might be wrong",tarUri))
              }else{
                if (srcIdx != tarIdx){
                  //co-occurrences are bi-directional, but only save one direction may save space
                  val intString = srcIdx + "\t" + tarIdx + "\t" + count
                  ilfoStream.println(intString)
                }else{
                  //LOG.warn("We don't particularly welcome self co-occurrences.")
                }
              }

            }
          })
        }

        count += 1
        if (count % 100000 == 0) LOG.info(String.format("%s objects processed.",count.toString))
      }
    )))
  }

  def parseCooccsList(cooccsFile:File , hostMap:CompactHashMap[String,Int] , integerListFile:File) {
    LOG.info("Parsing Cooccurrences into Integer List")

    val ilfo: OutputStream = new FileOutputStream(integerListFile)
    val ilfoStream = new PrintStream(ilfo, true)

    if (cooccsFile.getName.endsWith(".tsv")){
      parsePigStorageFile(cooccsFile,hostMap,ilfoStream)
    }else{
      parseJSONStorageFile(cooccsFile,hostMap,ilfoStream)
    }
  }
}

object WikipediaCooccurrencesGraph{
  def main(args:Array[String]){
      val graphConfigFileName = args(0)
      val config = new GraphConfiguration(graphConfigFileName)

      val baseDir = config.get("org.dbpedia.spotlight.graph.dir")

      val uriMapFile = new File(config.get("org.dbpedia.spotlight.graph.dir")+config.get("org.dbpedia.spotlight.graph.mapFile"))

      val hostMap = HostMap.load(uriMapFile)

      //Generating for co-occurrences files
      val cooccSubDir = baseDir+config.get("org.dbpedia.spotlight.graph.coocc.dir")
      SimpleUtils.createDir(cooccSubDir)

      val cooccsSrcFile = new File(config.get("org.dbpedia.spotlight.graph.coocc.src"))
      val cooccInterListFile = new File(cooccSubDir+config.get("org.dbpedia.spotlight.graph.coocc.integerList"))
      val cooccBaseName = config.get("org.dbpedia.spotlight.graph.coocc.basename")

      //parse the cooccsSrcFile and store the parsed result as an IntegerList
      val wcg = new WikipediaCooccurrencesGraph
      wcg.parseCooccsList(cooccsSrcFile,hostMap,cooccInterListFile)

      //build a weighted graph and store.
      //We should use the method that specify a node number, which make it possible to have nodes with no arcs
      val cowg = GraphUtils.buildWeightedGraphFromFile(cooccInterListFile,867361)
      GraphUtils.storeWeightedGraph(cowg,cooccSubDir,cooccBaseName)
  }
}