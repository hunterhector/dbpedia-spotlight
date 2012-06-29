package org.dbpedia.spotlight.graph

import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.{DBpediaResourceOccurrence, DBpediaResource, SurfaceFormOccurrence}
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import es.yrbcn.graph.weighted.WeightedPageRankPowerMethod
import org.dbpedia.spotlight.util.{GraphConfiguration, GraphUtils}
import collection.mutable
import it.unimi.dsi.webgraph.ImmutableSubgraph
import collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/2/12
 * Time: 4:27 PM
 */

/**
 * Construct a referent graph described in Han to compute the evidence population
 * This graph can be viewed as a subgraph extracted from the graph of all entities
 * There are
 * ->two kinds of vertices: entity (represented by DBpediaResource) and surfaceform
 * ->two kinds of edge: between entities of different surfaceforms and from surfaceform to entity
 * @author Hectorliu
 * @param scoredSf2CandsMap
 */
class ReferentGraph(scoredSf2CandsMap: Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]], initialEvidences: Map[SurfaceFormOccurrence,Double], occTranGraph: ArcLabelledImmutableGraph, cooccGraph: ArcLabelledImmutableGraph, uri2IdxMap: Map[String,Int], idx2UriMap:Map[Int,String]) {
  private val LOG = LogFactory.getLog(this.getClass)

  private val s2c = scoredSf2CandsMap
  private val otg = occTranGraph
  private val cg = cooccGraph
  private val totalNodeNumber = otg.numNodes
  private var subGraphNumNodes = 0   //get during buildReferentArcList
  private val ie = initialEvidences
  private val surfaceIndexMap = new mutable.HashMap[SurfaceFormOccurrence,Int]()
  private val arcList:List[(Int,Int,Float)] = buildReferentArcList()

  private val rg = buildReferentGraph


  def buildReferentArcList ():List[(Int,Int,Float)]  = {

    LOG.info("Getting a subgraph of all candiddates.")
    val allIndex = s2c.foldLeft(List[Int]())((idxList,tuple)=> {
      val sf = tuple._1
      val occList = tuple._2
      val list = occList.map(occ => uri2IdxMap.getOrElse(occ.resource.uri,-1))
      idxList ++ list.filterNot(index => index == -1)
    })

    val subCooccGraph = new ImmutableSubgraph(cg,allIndex.toArray)
    val subGraphIterator = subCooccGraph.nodeIterator(0)

    LOG.info("Compute semantic weight or arcs.")
    subGraphNumNodes = subCooccGraph.numNodes
    val tmpArcList = new ListBuffer[(Int,Int,Float)]()
    (0 to subGraphNumNodes).foreach(_ =>{
      val subCurrIdx = subGraphIterator.nextInt()
      val outdegree = subGraphIterator.outdegree()

      if (outdegree != 0){
        val rootCurrIdx = subCooccGraph.toRootNode(subCurrIdx)
        val occTranCurrIter = otg.nodeIterator(rootCurrIdx)
        val cooccCurrIter = cg.nodeIterator(rootCurrIdx)

        val indegreeCurr = occTranCurrIter.outdegree

        val labels = cooccCurrIter.labelArray
        val succs = subGraphIterator.successorArray
        (0 to outdegree).foreach(i => {
          val subNextIdx = succs(i)
          val cooccCount = labels(i).getInt
          val rootNextIdx = subCooccGraph.toRootNode(subNextIdx)
          val occTranNextIter = otg.nodeIterator(rootNextIdx)
          val indegreeNext = occTranNextIter.outdegree()
          val semRelated = mwSemanticRelateness(indegreeCurr,indegreeNext,cooccCount,totalNodeNumber)

          val tuple = (subCurrIdx,subNextIdx,semRelated.toFloat)
          val tupleR = (subNextIdx,subCurrIdx,semRelated.toFloat)
          tmpArcList += tuple
          tmpArcList += tupleR
        })
      }
    })

    LOG.info("Combining surface forms and candidates into one graph list")
    var sfSubIdx = subGraphNumNodes
    s2c.foreach{
      case (sfOcc,occList) => {
         occList.foreach(occ => {
           val idx = uri2IdxMap.getOrElse(occ.resource.uri,-1)
           if (idx == -1) LOG.error("Resouce not found in uriMap")
           else{
             val subIdx = subCooccGraph.fromRootNode(idx)
             val contextualScore = occ.contextualScore
             val tuple = (sfSubIdx,subIdx,contextualScore.toFloat)
             tmpArcList += tuple
           }
         })
      }
      case _ => LOG.error("Incorrect tuple in scoredSf2CandsMap")
    }


    tmpArcList.toList
  }

  def mwSemanticRelateness(indegreeA:Int,indegreeB:Int,cooccCount:Int,wikiSize:Int) = {
    val maxIn = math.max(indegreeA,indegreeB)
    val minIn = math.min(indegreeA,indegreeB)
    1-(math.log(maxIn)-math.log(cooccCount))/(math.log(wikiSize) - math.log(minIn))
  }

  def buildReferentGraph() : ArcLabelledImmutableGraph = {
    LOG.info("Creating the referent graph")
    val g= GraphUtils.buildWeightedGraphFromTriples(arcList)
    LOG.info("Done!")
    g
  }

  def getResult():Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]] = {
    null
  }

  def getRankedCands(){
    runPageRank()
  }

  def runPageRank(){
    val pr:WeightedPageRankPowerMethod  = new WeightedPageRankPowerMethod(rg)

    LOG.info("Saving ranks...");

  }


  def subGraphIndexToUri(){

  }
}
