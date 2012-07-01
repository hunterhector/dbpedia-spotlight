package org.dbpedia.spotlight.graph

import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.{DBpediaResourceOccurrence, DBpediaResource, SurfaceFormOccurrence}
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import es.yrbcn.graph.weighted.{WeightedPageRank, WeightedPageRankPowerMethod}
import org.dbpedia.spotlight.util.{GraphConfiguration, GraphUtils}
import collection.mutable
import it.unimi.dsi.webgraph.ImmutableSubgraph
import collection.mutable.ListBuffer
import it.unimi.dsi.fastutil.doubles.{DoubleArrayList, DoubleList}
import org.dbpedia.spotlight.model.Factory.SurfaceFormOccurrence
import org.dbpedia.spotlight.model.SurfaceFormOccurrence

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
class ReferentGraph(scoredSf2CandsMap: Map[SurfaceFormOccurrence,(List[DBpediaResourceOccurrence],Double)], occTranGraph: ArcLabelledImmutableGraph, cooccGraph: ArcLabelledImmutableGraph, uri2IdxMap: Map[String,Int]) {
  private val LOG = LogFactory.getLog(this.getClass)

  LOG.info("Initializing Graph Object.")

  private val s2c = scoredSf2CandsMap
  private val otg = occTranGraph
  private val cg = cooccGraph

  private val indexRecord = new mutable.HashMap[Int,(DBpediaResourceOccurrence,SurfaceFormOccurrence)]()
  private val subCooccGraph = getCandiddateSubGraph
  private val totalNodeNumber = otg.numNodes //total node number in occ graph, should be the largest node number possible
  private val candidateNumber = subCooccGraph.numNodes

  //initial vector assign to nodes at the start of pagerank
  private val initialVector: DoubleArrayList = new DoubleArrayList() //build up during buildReferentArcList
  //preference vector for pagerank
  private val preferenceVector: DoubleArrayList = new DoubleArrayList() //should also be build up during buildReferentArcList

  private val arcList:List[(Int,Int,Float)] = buildReferentArcList()

  val rg = buildReferentGraph //For index less than candidateNumber, referentGraph and subCooccGraph have index pointing to the same thing

  private def getCandiddateSubGraph = {
    LOG.info("Getting a subgraph of all candiddates.")
    val allCandidateIndex = s2c.foldLeft(List[Int]())((idxList,tuple)=> {
      val sf = tuple._1
      val occList = tuple._2._1
      val list = occList.map(occ => uri2IdxMap.getOrElse(occ.resource.uri,-1)).filterNot(index => index == -1)
      idxList ++ list
    })
    LOG.info("Done.")
    new ImmutableSubgraph(cg,allCandidateIndex.toArray)

  }

  private def buildReferentArcList ():List[(Int,Int,Float)]  = {
    val subGraphIterator = subCooccGraph.nodeIterator(0)

    LOG.info("Computing semantic weight or arcs.")

    val tmpArcList = new ListBuffer[(Int,Int,Float)]()
    (0 to candidateNumber).foreach(_ =>{
      val subCurrIdx = subGraphIterator.nextInt()
      // give each candidate node a initial evidence 0.
      initialVector.add(subCurrIdx,0)

      val outdegree = subGraphIterator.outdegree()
      // add the arcs in co-occurrences graph
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
    // Just need to know where surface forms start in graph, their score on them
    // are irrelevant to results.
    // the start index of surfaceform nodes is the number of candidates nodes
    var sfSubIdx = candidateNumber
    s2c.foreach{
      case (sfOcc,(occList,initialEvidence)) => {
        //connect surfaceform nodes to candidates nodes
        occList.foreach(occ => {
           val idx = uri2IdxMap.getOrElse(occ.resource.uri,-1)
           if (idx == -1) LOG.error("Resouce not found in uriMap")
           else{
             val subIdx = subCooccGraph.fromRootNode(idx)
             indexRecord += (subIdx -> (occ,sfOcc))
             val contextualScore = occ.contextualScore
             val tuple = (sfSubIdx,subIdx,contextualScore.toFloat)
             tmpArcList += tuple
           }
        })
        //for each surface form node, give a initial evidence
        initialVector.add(sfSubIdx,initialEvidence)
        sfSubIdx += 1    //increment to add next surface form
      }
      case _ => LOG.error("Incorrect tuple in scoredSf2CandsMap") //well, this should not happen
    }

    tmpArcList.toList
  }

  private def mwSemanticRelateness(indegreeA:Int,indegreeB:Int,cooccCount:Int,wikiSize:Int) = {
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


  def getResult (k: Int): Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]] = {
    val rankVector = runPageRank(rg)
    val tmpResult = new mutable.HashMap[SurfaceFormOccurrence,ListBuffer[DBpediaResourceOccurrence]]()
    (0 to candidateNumber).foreach(idx => {
       val tuple = indexRecord(idx)
       val sfOcc = tuple._2
       val resOcc = tuple._1
       val rank = rankVector(idx)
       resOcc.setSimilarityScore(rank)
       tmpResult(sfOcc).append(resOcc)
    })
    val result = tmpResult.foldLeft(Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]]())( (finalMap,valuePair) =>{
      val sfOcc = valuePair._1
      val listBuf = valuePair._2
      finalMap + (sfOcc -> listBuf.toList.sortBy(o => o.similarityScore).reverse.take(k))
    })
    result
  }

  private def runPageRank(g: ArcLabelledImmutableGraph) : Array[Double] = {
    LOG.info("Running page ranks...")

    WeightedPageRankWrapper.run(g,WeightedPageRank.DEFAULT_ALPHA,false,WeightedPageRank.DEFAULT_THRESHOLD,WeightedPageRank.DEFAULT_MAX_ITER,initialVector)

/*    //TODO: make parameters configurable
    val pr:WeightedPageRankPowerMethod  = new WeightedPageRankPowerMethod(g)
    pr.alpha = WeightedPageRank.DEFAULT_ALPHA
    pr.stronglyPreferential = false
    pr.start = initialVector

    val deltaStop = new WeightedPageRank.NormDeltaStoppingCriterion(WeightedPageRank.DEFAULT_THRESHOLD)
    val iterStop = new WeightedPageRank.IterationNumberStoppingCriterion(WeightedPageRank.DEFAULT_MAX_ITER)
    val finalStop = WeightedPageRank.or(deltaStop, iterStop)
    //TODO problem here: can't compile with this line in scala, but can successfully call in java
    //pr.stepUntil(finalStop)

    pr.rank*/
  }

  def subGraphIndexToUri(){

  }
}
