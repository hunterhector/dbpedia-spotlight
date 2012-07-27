package org.dbpedia.spotlight.graph

import java.io.{PrintWriter, File}
import es.yrbcn.graph.weighted.WeightedBVGraph
import org.dbpedia.spotlight.util.{SimpleUtils, GraphConfiguration, GraphUtils}
import org.apache.commons.logging.LogFactory
import scala.io.Source
/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/13/12
 * Time: 6:14 PM
 */

/**
 * Just a Object to run the whole process of creating the graphs
 */
object GraphMaker{
  private val LOG = LogFactory.getLog(this.getClass)

  def makeOccsGraph(uriMapFile:File, occsSrcFile:File, hostMap: Map[String,Int], baseDir:String, config:GraphConfiguration, numberOfNodes:Int) = {
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

  def makeCooccGraph(hostMap: Map[String,Int], baseDir:String, config:GraphConfiguration, numberOfNodes:Int){
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

  //might be useless
/*  def makeCommonInlinkGraph(hostMap: Map[String,Int], baseDir:String, config:GraphConfiguration){
    val inlinkSubDir = baseDir + config.get("org.dbpedia.spotlight.graph.commoninlink.dir")
    SimpleUtils.createDir(inlinkSubDir)

    val inlinkSrcFile = new File(config.get("org.dbpedia.spotlight.graph.commoninlink.src"))
    val inlinkIntegerListFile = new File(inlinkSubDir+config.get("org.dbpedia.spotlight.graph.commoninlink.integerList"))
    val inlinkBaseName = config.get("org.dbpedia.spotlight.graph.commoninlink.basename")

    //parse to IntegerList
    val wcg = new WikipediaCooccurrencesGraph //also use the coocurrence graph class becuz they are essential the same concept, but with different data
    wcg.parseCooccsList(inlinkSrcFile,hostMap,inlinkIntegerListFile)

    //build the graph and store with node number
    val cilg = GraphUtils.buildWeightedGraphFromFile(inlinkIntegerListFile,config.getNodeNumber)
    GraphUtils.storeWeightedGraph(cilg,inlinkSubDir,inlinkBaseName)
  }*/



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

    makeOccsGraph(uriMapFile,occsSrcFile,hostMap,baseDir,config,numberOfNodes)
    makeCooccGraph(hostMap,baseDir,config,numberOfNodes)
  }
}