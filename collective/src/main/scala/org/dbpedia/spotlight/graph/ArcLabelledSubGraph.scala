package org.dbpedia.spotlight.graph

import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import org.apache.commons.logging.LogFactory
import collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/4/12
 * Time: 1:38 PM
 */

/**
 * An implementation of a sub graph for ArclablledGraph.
 *
 * With a super graph and a set of nodes provided. The class will initially generate a graph that include
 * all arcs between or within N-hop distance of these nodes. The hop parameter can be used to controlled
 * the number of hop desired. Non positive weighted arcs are by default removed, it can be included by setting
 * the removeNonPositiveWeightedArc parameter
 *
 * The subgraph can be get as an Integer arc list, which can be easily extended by adding arcs and nodes to
 * it.
 *
 * @param superGraph An ArcLabelledImmutableGraph that is the super graph of this sub graph
 * @param nodeSet A Set of Integer indicating the basic set of nodes to generate the sub graph
 * @param hop The number of hop to which neighbours will be included in the sub graph
 * @param removeNonPositiveWeightedArc Whether to remove the non positive weighted arc, default to be true
 */
class ArcLabelledSubGraph(superGraph:ArcLabelledImmutableGraph,nodeSet: Set[Int], hop:Int, removeNonPositiveWeightedArc: Boolean
                           = true) {
  private val LOG = LogFactory.getLog(this.getClass)

  val supergraphNumNodes = superGraph.numNodes()
  val supergraphNode = Array.fill[Int](supergraphNumNodes)(-1)

  val initialSubGraphNode = nodeSet.toArray.sorted

  // initialize supergraphNode array to store all initial subgraph nodes
  updateSuperGraphRecord(initialSubGraphNode)

  val initialArcList = buildSelfConnectedArcList(initialSubGraphNode)

  //extend the subgraph with n-hop relation
  val (subgraphNode,subgraphArcList) = build(initialSubGraphNode,initialArcList,hop)
  val subgraphSize = subgraphNode.length

  LOG.debug(String.format("Subgraph size: %s;  Supergraph size: %s.",subgraphSize.toString,supergraphNumNodes.toString))

  if (subgraphSize > 0 && subgraphNode(subgraphSize - 1) >= supergraphNumNodes) throw new IllegalArgumentException("Subnode index out of bounds (larger than supergraph number of nodes): "+subgraphNode(subgraphSize - 1))


  /**
   * Recursively expand the sub graph to reach the N-hop neighbour.
   *
   * @param currentSubGraphNode The current sub graph nodes array
   * @param currentArcList The current arc list
   * @param n number of hops desired to reach from the current graph
   * @return  A tuple containing the expanded sub graph nodes and arc list
   */
  private def build(currentSubGraphNode:Array[Int],currentArcList:ListBuffer[(Int,Int,Float)],n:Int):(Array[Int],ListBuffer[(Int,Int,Float)]) = {
    if (n == 0){
      (currentSubGraphNode,currentArcList)
    }else{
      val iter = superGraph.nodeIterator()
      iter.next()

      var lastVisited = 0

      //find out new nodes and new arcs to be added to the subgraph
      var newNodes = Set[Int]()
      val newArcs =
        currentSubGraphNode.foldLeft(Map[(Int,Int),Float]())((newArcs,currIdx)=>{
          val step = currIdx - lastVisited
          iter.skip(step)
          lastVisited = currIdx

          val outdegree = iter.outdegree()
          val succs = iter.successorArray()
          val labels = iter.labelArray()

          val currNewArcs = (0 to outdegree-1).foldLeft(Map[(Int,Int),Float]())((na,pos) =>{
            val succIdx = succs(pos)
            val weight = labels(pos).getFloat
            //add the new succ, ignore already exists ones, which are already included
            //note that only one direction is added to save memory
            if (supergraphNode(succIdx) == -1)
            {
              newNodes += succIdx
              na + ((currIdx,succIdx)->weight)
            }else na
          })
          newArcs ++ currNewArcs
      })

      val newSubGraphNode = updateGraphNodes(currentSubGraphNode,newNodes)
      val newArcList = expandArcList(currentArcList,newArcs)

      build(newSubGraphNode,newArcList,n-1)
    }
  }

  private def expandArcList(oldArcList:ListBuffer[(Int,Int,Float)], newArcMap: Map[(Int,Int),Float]) = {
    newArcMap.foreach{case ((currIdx,succIdx),weight) => {
       if (supergraphNode(succIdx) >= 0){  // check if the successor index is in our subgraph
         if (removeNonPositiveWeightedArc || weight > 0.0){
           val t = (supergraphNode(currIdx),supergraphNode(succIdx),weight)
           oldArcList += t
         }
       }
     }}
    oldArcList
  }

  private def updateGraphNodes(oldSubGraphNode:Array[Int],newNodes:Set[Int]) = {
    val newSubGraphNode = (oldSubGraphNode.toList ++ newNodes.toList).sorted.toArray
    updateSuperGraphRecord(newSubGraphNode)
    newSubGraphNode
  }


  //whild only adding elements to subgraph, this will be correct
  private def updateSuperGraphRecord(sortedNodes:Array[Int]) = {
    (0 to sortedNodes.length-1).foreach(i =>{
      supergraphNode(sortedNodes(i)) = i
    })
  }

  /**
   * Return the corresponding supergraph index of a subgraph index
   *
   * @param x The subgraph index
   * @return  The corresponding supergraph index
   */
  def toSupergraphNode(x:Int):Int = {
    if (x < 0 || x>= subgraphSize) throw new IllegalArgumentException
    subgraphNode(x)
  }

  /**
   * Return the corresponding subgraph index of a supergraph index
   * @param x The supergraph index
   * @return  The corresponding subgraph index
   */
  def fromSupergraphNode(x:Int):Int = {
    if (x < 0 || x>= supergraphNumNodes) throw new IllegalArgumentException
    supergraphNode(x)
  }

  /**
   * Return the subgraph as a arc list, each arc is defined by 2 Integer and a weight
   * @return A ListBuffer that represent the graph
   */
  def getArcList = {
    subgraphArcList
  }

  /**
   * Return a arc List which extends to both direction. It is useful because most of the
   * graphs used to represent the semantic relations are in essential bi-directional
   * @return
   */
  def getBidirectionalArcList = {
    val reverseArcList = subgraphArcList.map(arc=>{(arc._2,arc._1,arc._3)})
    subgraphArcList ++ reverseArcList
  }

  private def buildSelfConnectedArcList(graphNodes:Array[Int]) = {
    LOG.debug("Building initial Arc List.")
    val tmpArcList = new ListBuffer[(Int,Int,Float)]
    val iter = superGraph.nodeIterator()
    iter.next

    var lastVisited = 0
    graphNodes.foreach(currIdx => {
      val step = currIdx-lastVisited
      iter.skip(step)
      lastVisited = currIdx

      val outdegree = iter.outdegree()
      val succs = iter.successorArray()
      val labels = iter.labelArray()

      (0 to outdegree-1).foreach(pos =>{
        val succIdx = succs(pos)
        val weight = labels(pos).getFloat

        if (supergraphNode(succIdx) >= 0){  // check if the successor index is in our subgraph
          if (!removeNonPositiveWeightedArc || weight > 0.0){
            val t = (supergraphNode(currIdx),supergraphNode(succIdx),weight)
            tmpArcList += t
          }
        }
      })
    })
    tmpArcList
  }
}
