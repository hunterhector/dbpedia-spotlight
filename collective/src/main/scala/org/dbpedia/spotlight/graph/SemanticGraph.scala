package org.dbpedia.spotlight.graph

import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import org.dbpedia.spotlight.exceptions.InitializationException
import collection.mutable
import java.io.{File, PrintStream, FileOutputStream, OutputStream}
import org.dbpedia.spotlight.util.{GraphUtils, GraphConfiguration}
import org.apache.commons.logging.LogFactory

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/3/12
 * Time: 8:55 PM
 */

class SemanticGraph(occTranGraph: ArcLabelledImmutableGraph, cooccGraph: ArcLabelledImmutableGraph) {
    val LOG = LogFactory.getLog(this.getClass)
    val nodeNumber = occTranGraph.numNodes()

    if (occTranGraph.numNodes() != cooccGraph.numNodes()){
      throw new InitializationException("The number of nodes of two graph are not the same")
    }

    def getInlinkStats(): Map[Int,Int]= {
      LOG.info("Counting indegree statistics")
      val iter = occTranGraph.nodeIterator()
      val map = new mutable.HashMap[Int,Int]()
      (1 to nodeNumber).foreach(count =>{
        val curr = iter.nextInt
        val d = iter.outdegree
        map += (curr -> d)
        if (count%500000 == 0){
          LOG.info(String.format("%s nodes visited.",count.toString))
        }
      })
      val res = map.toMap
      LOG.info("Done.")
      res
    }

    def buildIntegerList(integerListFile:File)  {
      val ilfo: OutputStream = new FileOutputStream(integerListFile)
      val ilfoStream = new PrintStream(ilfo, true)

      val indegreeMap = getInlinkStats()
      val cooccIter = cooccGraph.nodeIterator()

      LOG.info("Building integer list for semantic graph, this will take a while")
      (1 to nodeNumber).foreach(count =>{
        val curr = cooccIter.nextInt()

        val numOfCoocc = cooccIter.outdegree
        val cooccSucc = cooccIter.successorArray
        val cooccLabels = cooccIter.labelArray

        //println(String.format("number of coocc for node %s is %s",curr.toString,numOfCoocc.toString))
        (0 to numOfCoocc-1).foreach(idx => {
            val succ = cooccSucc(idx)
            val cooccCount = cooccLabels(idx).getInt
            val sr = mwSemanticRelateness(indegreeMap(curr),indegreeMap(succ),cooccCount,nodeNumber)
            val str = curr + "\t" + succ + "\t" + sr
            ilfoStream.println(str)
        })
        if (count%100000 == 0){
          LOG.info(String.format("%s nodes built.",count.toString))
        }
      })
      LOG.info("Done.")
    }

    private def mwSemanticRelateness(indegreeA:Int,indegreeB:Int,cooccCount:Int,wikiSize:Int) = {
      val maxIn = math.max(indegreeA,indegreeB)
      val minIn = math.min(indegreeA,indegreeB)
      1-(math.log(maxIn)-math.log(cooccCount))/(math.log(wikiSize) - math.log(minIn))
    }
}

object SemanticGraph{
  val LOG = LogFactory.getLog(this.getClass)

  val graphConfig = new GraphConfiguration("../conf/graph.properties")
  private val offline = "true" == graphConfig.getOrElse("org.dbpedia.spotlight.graph.offline","false")

  LOG.info("Preparing graphs...")
  private val baseDir = graphConfig.get("org.dbpedia.spotlight.graph.dir")

  private val cooccGraphBasename = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.coocc.dir")+graphConfig.get("org.dbpedia.spotlight.graph.coocc.basename")
  private val occTransposeGraphBaseName = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.occ.dir") + graphConfig.get("org.dbpedia.spotlight.graph.transpose.occ.basename")

  private val rowg = GraphUtils.loadAsArcLablelled(occTransposeGraphBaseName, offline)
  private val cwg = GraphUtils.loadAsArcLablelled(cooccGraphBasename,offline)

  private val sgSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.dir")
  createDir(sgSubDir)
  private val sgBasename = sgSubDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.basename")
  private val sgIntegerListFileName = sgSubDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.integerList")

   def main(args:Array[String]){
//     val sg = new SemanticGraph(rowg,cwg)
//
     val sgFile = new File(sgIntegerListFileName)
//
//     sg.buildIntegerList(sgFile)

     val g = GraphUtils.buildWeightedGraphFromFile(sgFile,graphConfig.getNodeNumber)

     GraphUtils.storeWeightedGraph(g,sgBasename)
   }

  private def createDir(dirName:String){
    val theDir = new File(dirName)
    if (!theDir.exists())
    {
      theDir.mkdir()
      LOG.info("Created directory: " + dirName)
    }
  }
}
