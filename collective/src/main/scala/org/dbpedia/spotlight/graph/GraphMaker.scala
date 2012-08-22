package org.dbpedia.spotlight.graph

import java.io.{PrintWriter, File}
import es.yrbcn.graph.weighted.WeightedBVGraph
import org.dbpedia.spotlight.util.{SimpleUtils, GraphConfiguration, GraphUtils}
import org.apache.commons.logging.LogFactory
import scala.io.Source
import com.officedepot.cdap2.collection.CompactHashMap

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/13/12
 * Time: 6:14 PM
 */

/**
 * This class is used during indexing to create the graphs needed for disambiguation.
 *
 * It simply call different of graph classes to generate different Wikipedia link graph. The graph classes
 * simply return arc list to represent the graph, this class call WebGraph methods to store them in WebGraph format.
 *
 * If new kinds of graphs are implemented, they can be also added here so that they constructed at one run.
 *
 * Please note that all graphs should be passed with a numberOfNodes parameter to ensure they are of the same
 * size, and the index will conform to which recorded in the HostMap
 */
object GraphMaker{
  private val LOG = LogFactory.getLog(this.getClass)

  /**
   * Make the occurrence graph with a occurrence count file generated by ExtractOccsFromWikipedia
   * @param occsSrcFile The occurrence count file generated by ExtractOccsFromWikipedia
   * @param hostMap The host map parsed from the uriMap file
   * @param baseDir Base directory that graph files are stored because we recommend different graphs are stored in the same directory
   * @param config The graph config
   * @param numberOfNodes The number of nodes stored in HostMap
   */
  def makeOccsGraph(occsSrcFile:File, hostMap: CompactHashMap[String,Int], baseDir:String, config:GraphConfiguration, numberOfNodes:Int) = {
    //Generating for occurrences files
    val occSubDir = baseDir+config.get("org.dbpedia.spotlight.graph.occ.dir")
    SimpleUtils.createDir(occSubDir)

    val occInterListFile = new File(occSubDir+config.get("org.dbpedia.spotlight.graph.occ.integerList"))
    val occBaseName = config.get("org.dbpedia.spotlight.graph.occ.basename")

    //parse the occSrcFile and store the parsed result as an IntegerList
    val wog = new WikipediaOccurrenceGraph
    wog.parseOccsList(occsSrcFile,hostMap, occInterListFile)

    //build a weighted graph and store
    val ocwg = GraphUtils.buildWeightedGraphFromFile(occInterListFile,numberOfNodes)
    GraphUtils.storeWeightedGraph(ocwg,occSubDir,occBaseName)

    val occTransposeBaseName = config.get("org.dbpedia.spotlight.graph.transpose.occ.basename")
    val batchSize = config.get("org.dbpedia.spotlight.graph.transpose.batchSize").toInt

    //also store the transpose graph, easy to use outdegree to find the indegree of a node in the origin graph
    val ocwgTrans = GraphUtils.transpose(ocwg,batchSize)
    GraphUtils.storeWeightedGraph(ocwgTrans,occSubDir,occTransposeBaseName)
  }

  /**
   * Create and store the co-occurrence graph using the CooccurrencesCount.pig script
   * @param hostMap The host map parsed from the uriMap file
   * @param baseDir Base directory that graph files are stored because we recommend different graphs are stored in the same directory
   * @param config The graph config
   * @param numberOfNodes The number of nodes stored in HostMap
   */
  def makeCooccGraph(hostMap: CompactHashMap[String,Int], baseDir:String, config:GraphConfiguration, numberOfNodes:Int){
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
    val cowg = GraphUtils.buildWeightedGraphFromFile(cooccInterListFile,numberOfNodes)
    GraphUtils.storeWeightedGraph(cowg,cooccSubDir,cooccBaseName)
  }

  //parameter is the graph properties file: ../../conf/graph.properties
  def main(args: Array[String]) {
    val graphConfigFileName = args(0)
    val config = new GraphConfiguration(graphConfigFileName)

    val baseDir = config.get("org.dbpedia.spotlight.graph.dir")

    //create if not exists
    SimpleUtils.createDir(baseDir)

    val uriMapFile = new File(config.get("org.dbpedia.spotlight.graph.dir")+config.get("org.dbpedia.spotlight.graph.mapFile"))
    val occsSrcFile = new File(config.get("org.dbpedia.spotlight.graph.occ.src"))

    //Generate the host map
    val numberOfNodes = HostMap.parseToHostMap(occsSrcFile,uriMapFile)
    val outWriter = new PrintWriter(new File(config.get("org.dbpedia.spotlight.graph.dir")+"/nodenumber"))
    outWriter.println(numberOfNodes)
    outWriter.close()

    //Get the host map
    val hostMap = HostMap.load(uriMapFile)

    makeOccsGraph(occsSrcFile,hostMap,baseDir,config,numberOfNodes)
    makeCooccGraph(hostMap,baseDir,config,numberOfNodes)
  }
}