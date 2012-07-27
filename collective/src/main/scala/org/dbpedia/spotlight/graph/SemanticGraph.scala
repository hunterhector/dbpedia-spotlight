package org.dbpedia.spotlight.graph

import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import org.dbpedia.spotlight.exceptions.InitializationException
import collection.mutable
import java.io.{File, PrintStream, FileOutputStream, OutputStream}
import org.dbpedia.spotlight.util.{SimpleUtils, GraphUtils, GraphConfiguration}
import org.apache.commons.logging.LogFactory

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/3/12
 * Time: 8:55 PM
 */

class SemanticGraph(occTranGraph: ArcLabelledImmutableGraph, cooccsGraph: ArcLabelledImmutableGraph) {
    val LOG = LogFactory.getLog(this.getClass)
    val nodeNumber = occTranGraph.numNodes()

    if (occTranGraph.numNodes() != cooccsGraph.numNodes()){
      throw new InitializationException("The number of nodes of two graph are not the same")
    }

    def getInlinkStats(): Map[Int,(Int,Array[Int])]= {
      LOG.info("Counting indegree statistics")
      val iter = occTranGraph.nodeIterator()
      val map = new mutable.HashMap[Int,(Int,Array[Int])]()
      (1 to nodeNumber).foreach(count =>{
        val curr = iter.nextInt
        val d = iter.outdegree
        val succs = iter.successorArray().slice(0,d).sorted
        map += (curr -> (d,succs))
        if (count%500000 == 0){
          LOG.info(String.format("%s nodes visited.",count.toString))
        }
      })
      val res = map.toMap
      res
    }

    def buildIntegerList(integerListFile:File)  {
      val ilfo: OutputStream = new FileOutputStream(integerListFile)
      val ilfoStream = new PrintStream(ilfo, true)

      val indegreeMap = getInlinkStats()
      val cooccsIter = cooccsGraph.nodeIterator()

      LOG.info("Building integer list for semantic graph, this will take a while")
      (1 to nodeNumber).foreach(count =>{
        val curr = cooccsIter.nextInt()

        val numCooccNodes = cooccsIter.outdegree
        val cooccSucc = cooccsIter.successorArray
        val cooccLabels = cooccsIter.labelArray

        (0 to numCooccNodes-1).foreach(idx => {
            val succ = cooccSucc(idx)
            val cooccCount = cooccLabels(idx).getInt
            val sr = mwSemanticRelatedness(indegreeMap(curr)._1,indegreeMap(succ)._1,indegreeMap(curr)._2,indegreeMap(succ)._2,cooccCount,nodeNumber)
            if (sr != 0.0){
              val str = curr + "\t" + succ + "\t" + sr
              ilfoStream.println(str)
            }
        })
        if (count%100000 == 0){
          LOG.info(String.format("%s nodes built.",count.toString))
        }
      })
      LOG.info("Done.")
    }

    // the return value is not necessarily allowNonNegative. if maxIn/commonInLinks > wikisize/minIn, it will return negative value
    private def mwSemanticRelatedness(indegreeA:Int,indegreeB:Int,inlinksA:Array[Int],inlinksB:Array[Int],cooccCount:Int,wikiSize:Int) = {
      val commonInLinks = SimpleUtils.findCommonInSortedArray(inlinksA,inlinksB)
      val maxIn = math.max(indegreeA,indegreeB)
      val minIn = math.min(indegreeA,indegreeB)
      val r = 1-(math.log(maxIn)-math.log(commonInLinks))/(math.log(wikiSize) - math.log(minIn))
      r
//      if (r < 0){
//        LOG.warn(String.format("The computed semantic link is less than 0, indegreeA: %s, indegreeB: %s, commonInLinks: %s",indegreeA.toString,indegreeB.toString,commonInLinks.toString))
//      }
    }
}

object SemanticGraph{
   def main(args:Array[String]){
     val LOG = LogFactory.getLog(this.getClass)

     //val graphConfig = new GraphConfiguration("../conf/graph.properties")
     val graphConfigFileName = args(0)
     val graphConfig = new GraphConfiguration(graphConfigFileName)


     val offline = "true" == graphConfig.getOrElse("org.dbpedia.spotlight.graph.offline","false")

     LOG.info("Preparing graphs...")

     val baseDir = graphConfig.get("org.dbpedia.spotlight.graph.dir")
     val cooccSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.coocc.dir")
     val cooccGraphBasename = graphConfig.get("org.dbpedia.spotlight.graph.coocc.basename")

     val occTransposeSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.occ.dir")
     val occTransposeGraphBaseName = graphConfig.get("org.dbpedia.spotlight.graph.transpose.occ.basename")

     val rowg = GraphUtils.loadAsArcLablelled(occTransposeSubDir,occTransposeGraphBaseName, offline)
     val cwg = GraphUtils.loadAsArcLablelled(cooccSubDir,cooccGraphBasename,offline)


     //preparing output graph
     val sgSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.dir")
     SimpleUtils.createDir(sgSubDir)
     val sgBasename = graphConfig.get("org.dbpedia.spotlight.graph.semantic.basename")
     val sgIntegerListFileName = sgSubDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.integerList")

     val sg = new SemanticGraph(rowg,cwg)

     val sgFile = new File(sgIntegerListFileName)

     sg.buildIntegerList(sgFile)

     val g = GraphUtils.buildWeightedGraphFromFile(sgFile,graphConfig.getNodeNumber)

     GraphUtils.storeWeightedGraph(g,sgSubDir,sgBasename)
   }
}
