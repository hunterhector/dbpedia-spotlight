package org.dbpedia.spotlight.graph

import java.io.File
import es.yrbcn.graph.weighted.WeightedBVGraph
import org.dbpedia.spotlight.util.{GraphConfiguration, GraphUtils}
import org.apache.commons.logging.LogFactory

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

  def main(args: Array[String]) {
    val graphConfigFileName = args(0)
    val config = new GraphConfiguration(graphConfigFileName)

    val baseDir = config.get("org.dbpedia.spotlight.graph.dir")

    //create if not exists
    createDir(baseDir)

    val uriMapFile = new File(config.get("org.dbpedia.spotlight.graph.mapFile"))

    //Generating for occurrences files
    val occSubDir = baseDir+config.get("org.dbpedia.spotlight.graph.occ.dir")
    createDir(occSubDir)

    val occsSrcFile = new File(config.get("org.dbpedia.spotlight.graph.occsrc"))
    val occInterListFile = new File(occSubDir+config.get("org.dbpedia.spotlight.graph.occ.integerList"))
    val occBaseName = occSubDir + config.get("org.dbpedia.spotlight.graph.occ.basename")

    val occTransposeBaseName = occSubDir + config.get("org.dbpedia.spotlight.graph.transpose.occ.basename")
    val batchSize = config.get("org.dbpedia.spotlight.graph.transpose.batchSize").toInt

    //Generate the host map
    val numberOfNodes = HostMap.parseToHostMap(occsSrcFile,uriMapFile)
    config.setNodeNumber(numberOfNodes)

    //Get the host map
    val hostMap = HostMap.load(uriMapFile)

    //parse the occSrcFile and store the parsed result as an IntegerList
    val wog = new WikipediaOccurrenceGraph
    wog.parseOccsList(occsSrcFile,hostMap, occInterListFile)

    //build a weighted graph and store
    val ocwg = GraphUtils.buildWeightedGraphFromFile(occInterListFile)
    GraphUtils.storeWeightedGraph(ocwg,occBaseName)

    //also store the transpose graph, easy to use outdegree to find the indegree of a node in the origin graph
    val ocwgTrans = GraphUtils.transpose(ocwg,batchSize)
    GraphUtils.storeWeightedGraph(ocwgTrans,occTransposeBaseName)

    //Generating for co-occurrences files
    val cooccSubDir = baseDir+config.get("org.dbpedia.spotlight.graph.coocc.dir")
    createDir(cooccSubDir)

    val cooccsSrcFile = new File(config.get("org.dbpedia.spotlight.graph.cooccsrc"))
    val cooccInterListFile = new File(cooccSubDir+config.get("org.dbpedia.spotlight.graph.coocc.integerList"))
    val cooccBaseName = cooccSubDir + config.get("org.dbpedia.spotlight.graph.coocc.basename")

    //parse the cooccsSrcFile and store the parsed result as an IntegerList
    val wcg = new WikipediaCooccurrencesGraph
    wcg.parseCooccsList(cooccsSrcFile,hostMap,cooccInterListFile)

    //build a weighted graph and store.
    //We should use the method that specify a node number, which make it possible to have nodes with no arcs
    val cowg = GraphUtils.buildWeightedGraphFromFile(cooccInterListFile,config.getNodeNumber)
    GraphUtils.storeWeightedGraph(cowg,cooccBaseName)


  }

  private def createDir(dirName:String){
     val theDir = new File(dirName)
      if (!theDir.exists())
      {
        LOG.info("Creating directory: " + dirName)
        theDir.mkdir()
      }
  }
}