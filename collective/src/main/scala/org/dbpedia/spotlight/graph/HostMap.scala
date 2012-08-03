package org.dbpedia.spotlight.graph

import org.apache.commons.logging.LogFactory
import java.io.{PrintStream, FileOutputStream, File}
import collection.mutable.{HashMap,HashSet}
import io.Source
import com.officedepot.cdap2.collection.{CompactHashMap, CompactHashSet}

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/21/12
 * Time: 6:54 PM
 */

object HostMap {
  val LOG = LogFactory.getLog(this.getClass)

  /**
   * Create a one-to-one host map from URI to Integer, used as the base for all occ based graph
   * @param occsFile  the occurrence file: occs.tsv or occs.uriSorted.tsv, created during indexing
   * @param mapFile   the output file containing the one-to-one mapping
   * @return An Integer counting the total number of URIs collected
   */
  def parseToHostMap(occsFile: File, mapFile: File): Int = {
    LOG.info("Parsing the occs file to Map")
    val uriSet = CompactHashSet[String]()       // use a set to control duplicat URIs and to count the URI number

    val mfo = new FileOutputStream(mapFile)
    val mfoStream = new PrintStream(mfo, true)

    //Go through the file and attach give a index to each URI encountered
    var indexCount = 0

    Source.fromFile(occsFile).getLines().filterNot(line => line.trim() == "").foreach(
      line => {
        val fields = line.split("\\t")
        if (fields.length == 5) {
          val id = fields(0)
          val targetUri = fields(1)
          val srcUri = id.split("-")(0)

          if (!uriSet.contains(srcUri)) {
            uriSet += srcUri

            val mapString = indexCount + "\t" + srcUri
            indexCount += 1
            mfoStream.println(mapString)
          }

          if (!uriSet.contains(targetUri)) {
            uriSet += targetUri
            val mapString = indexCount + "\t" + targetUri
            indexCount += 1
            mfoStream.println(mapString)
          }
          if (uriSet.size % 500000 == 0) LOG.info(String.format("Map %s valid URIs", indexCount.toString))
        } else {
          LOG.warn("Invailid line in file at \n -> \t" + line)
        }
      })

    mfoStream.close()

    LOG.info(String.format("Succesfully process map with size %s.", uriSet.size.toString))
    LOG.info(String.format("Map file saved at %s.", mapFile.getName))

    uriSet.size
  }

  /**
   * Method to load the host map as Map in memory
   * @param mapFile the map file created by parseToHostMap
   * @return a scala Map: host map from uri to graph index
   */
  def load (mapFile:File): CompactHashMap[String,Int] = {
    LOG.info("Reading the host map.")
    val hostMap = new CompactHashMap[String,Int]
    Source.fromFile(mapFile).getLines().filterNot(line => line.trim() == "").foreach(
      line =>{
        val fields = line.split("\\t")
        if (fields.length == 2){
          val index = fields(0).toInt
          val uri = fields(1)

          hostMap += (uri -> index)
        }else{
          LOG.warn("Invalid string in occsMapFile, lines should be two fields seperated by tab: \n\t-->\t"+line)
        }
      }
    )
    LOG.info("Done")
    hostMap
  }

  /**
   * Method to load the host map reversely as Map in memory
   * @param mapFile the map file created by parseToHostMap
   * @return a scala Map: host map from graph index to uri
   */
  def loadReverse (mapFile:File): CompactHashMap[Int,String] = {
    LOG.info("Reading the host map reversely.")
    val hostMap = new CompactHashMap[Int,String]
    Source.fromFile(mapFile).getLines().filterNot(line => line.trim() == "").foreach(
      line =>{
        val fields = line.split("\\t")
        if (fields.length == 2){
          val index = fields(0).toInt
          val uri = fields(1)

          hostMap += (index -> uri)
        }else{
          LOG.warn("Invalid string in occsMapFile, lines should be two fields seperated by tab: \n\t-->\t"+line)
        }
      }
    )
    LOG.info("Done")
    hostMap
  }
}