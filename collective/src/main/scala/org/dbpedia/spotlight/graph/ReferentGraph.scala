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
import com.officedepot.cdap2.collection.CompactHashMap

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/2/12
 * Time: 4:27 PM
 */

/**
 * Construct a referent graph described in Han's paper to compute the evidence population
 * This graph can be viewed as a combination of the subgraph extracted from the graph of all entities connected with the ambiguous surface forms
 * So there are:
 * ->two kinds of vertices: entity (represented by DBpediaResource) and surface form
 * ->two kinds of edge: between entities (bi-directional) and from surface form to entity (uni-directional)
 *
 * Han's paper is full referenced as:
 * X. Han, “Collective Entity Linking in Web Text : A Graph-Based Method,” in Proceedings of the 34th international ACM SIGIR conference on Research and development in Information, 2011, pp. 765-774.
 * @author Hectorliu
 * @param scoredSf2CandsMap
 */
class ReferentGraph(semanticGraph:ArcLabelledImmutableGraph, scoredSf2CandsMap: CompactHashMap[SurfaceFormOccurrence,(List[DBpediaResourceOccurrence],Double)], uri2IdxMap: CompactHashMap[String,Int], config:GraphConfiguration) {
  private val LOG = LogFactory.getLog(this.getClass)

  LOG.debug("Initializing Graph Object.")

  private val s2c = scoredSf2CandsMap
  private val sg = semanticGraph

  private val indexRecord = new mutable.HashMap[Int,ListBuffer[(DBpediaResourceOccurrence,SurfaceFormOccurrence)]]()
  private val subSemanticGraph = getCandiddateSubGraph

  private val sfNumber = s2c.keys.size
  private val coreGraphNodeNumber = subSemanticGraph.subgraphSize     // it is not true for n-hop graph now
  private val nodeNumber = sfNumber + coreGraphNodeNumber

  private val zeroArray = Array.fill[Double](nodeNumber)(0)
  //initial vector assign to nodes at the start of pagerank
  private val initialVector: DoubleArrayList = new DoubleArrayList(zeroArray) //build up during buildReferentArcList
  //preference vector for pagerank
  private val preferenceVector: DoubleArrayList = new DoubleArrayList(zeroArray) //should also be build up during buildReferentArcList

  private val arcList:List[(Int,Int,Float)] = buildReferentArcList()

  val rg = buildReferentGraph() //For index less than candidateNumber, referentGraph and subCooccGraph have index pointing to the same thing

  private def getCandiddateSubGraph = {
    LOG.debug("Getting a subgraph of all candiddates.")
    val allCandidateIndex = s2c.foldLeft(Set[Int]())((idxSet,tuple)=> {
      val sf = tuple._1
      val occList = tuple._2._1
      val list = occList.map(occ => uri2IdxMap.getOrElse(occ.resource.uri,-1)).filterNot(index => index == -1)
      idxSet ++ list
    })
    LOG.debug("Number of candidates: "+allCandidateIndex.size)
    new ArcLabelledSubGraph(sg,allCandidateIndex,1)
  }

  private def buildReferentArcList ():List[(Int,Int,Float)]  = {
    LOG.debug("Getting the core entities graph")

    val tmpArcList = subSemanticGraph.getBidirectionalArcList

    LOG.debug("Connecting surface forms to the core entities graph")
    // Just need to know where surface forms start in graph, scores on them
    // are irrelevant to results.
    // the start index of surfaceform nodes is the number of core graph nodes
    var sfSubIdx = coreGraphNodeNumber
    s2c.foreach{
      case (sfOcc,(occList,initialEvidence)) => {
        //connect surfaceform nodes to candidates nodes
        occList.foreach(occ => {
           //see the index of this uri in root graph
           val idx = uri2IdxMap.getOrElse(occ.resource.uri,-1)
           if (idx == -1) LOG.debug("Resouce not found in uriMap: "+occ.resource.uri)
           else{
             //get the candidate index in sub graph
             val subIdx = subSemanticGraph.fromSupergraphNode(idx)
             if (indexRecord.contains(subIdx)){          // use to retrieve the final score
               indexRecord(subIdx).append((occ,sfOcc))
             }  else {
               val lsBuffer = new ListBuffer[(DBpediaResourceOccurrence,SurfaceFormOccurrence)]
               val tuple = (occ,sfOcc)
               lsBuffer += tuple
               indexRecord += (subIdx -> lsBuffer)
             }

             val contextualScore = occ.contextualScore
             // add a link from sf to candidate, link with zero contextual score will be omitted
             if (contextualScore > 0.0) {
               val tuple = (sfSubIdx,subIdx,contextualScore.toFloat)
               tmpArcList += tuple
             }
           }
        })
        //for each surface form node, give a initial evidence
        initialVector.set(sfSubIdx,initialEvidence)
        //preference vector also to surface forms
        preferenceVector.set(sfSubIdx,1.0/sfNumber)
        sfSubIdx += 1    //increment to add next surface form
      }
      case _ => LOG.error("Incorrect tuple in uriMap") //well, this should not happen
    }
    LOG.debug(String.format("Referent Graph: %s nodes in total; %s core graph nodes ; %s surfaceform.",sfSubIdx.toString,coreGraphNodeNumber.toString,sfNumber.toString))
    tmpArcList.toList
  }

  private def buildReferentGraph() : ArcLabelledImmutableGraph = {
    LOG.debug(String.format("Creating the referent graph with %s arcs",arcList.length.toString))
    val g= GraphUtils.buildWeightedGraphFromTriples(arcList,nodeNumber)
    g
  }

  /**
   * Return the best k results after evidence population.
   *
   * @param k Number of best results to be returned if existed
   * @return k results with the highest rank in descending order
   */
  def getResult (k: Int): Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]] = {
    val rankVector = runPageRank(rg)
    val tmpResult = new mutable.HashMap[SurfaceFormOccurrence,ListBuffer[DBpediaResourceOccurrence]]()

    //indexRecord store indices of possible candidates to be disambiguated.
    //so can be used to retrieve the result
    indexRecord.foreach{case (idx,edgeList) =>{
        val rank = rankVector(idx)
        edgeList.foreach{
          case (resOcc,sfOcc) =>{
            resOcc.setSimilarityScore(rank)
            if (tmpResult.contains(sfOcc)){
              tmpResult(sfOcc).append(resOcc)
            }else{
              val lsBuffer = new ListBuffer[DBpediaResourceOccurrence]
              lsBuffer += resOcc
              tmpResult += (sfOcc -> lsBuffer)
            }
          }
        }
    }}

    //well just transform listBuffer to list and sort the result, take best k
    val result = tmpResult.foldLeft(Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]]())( (finalMap,valuePair) =>{
      val sfOcc = valuePair._1
      val listBuf = valuePair._2
      finalMap + (sfOcc -> listBuf.toList.sortBy(o => o.similarityScore).reverse.take(k))
    })
    result
  }

  /**
   * make the vector Stochastic (L1 norm equal to 1) so that the WeightedPageRank method will accept it
   *
   *
   * @param vector the initial vector
   * @return the resulted stochatic vector
   */
  private def makeStochastic(vector:DoubleArrayList):DoubleArrayList = {
    val l1Sum = l1Norm(vector)
    if (l1Sum == 0) return vector //if the l1 norm is 0 than actually it can't be stochastic. It will be simply returned

    (0 to vector.size-1)foreach(idx => {
      val ori = vector.get(idx)
      vector.set(idx,ori/l1Sum)
    })
    vector
  }

  /**
   * Calculate the L1Norm (sum of the abs of value in the list)
   * @param a The list to be examined
   * @return
   */
  private def l1Norm (a: DoubleArrayList): Double ={
    a.toDoubleArray().foldLeft(0.0)((norm,v)=>{
      norm + math.abs(v)
    })
  }

  private def runPageRank(g: ArcLabelledImmutableGraph) : Array[Double] = {
    LOG.debug("Running page ranks...")

    val iter = config.getOrElse("org.dbpedia.spotlight.graph.pagerank.iter","20").toInt

    //use the Java Wrapper to run the WeightedPageRank, running it direactly from scala generate unexpected errors
    WeightedPageRankWrapper.run(g,WeightedPageRank.DEFAULT_ALPHA,false,WeightedPageRank.DEFAULT_THRESHOLD,iter,makeStochastic(initialVector),preferenceVector)
  }
}
