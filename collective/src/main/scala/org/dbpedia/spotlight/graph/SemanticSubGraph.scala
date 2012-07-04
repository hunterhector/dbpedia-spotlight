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

class SemanticSubGraph(superGraph:ArcLabelledImmutableGraph,nodesSet: Set[Int]) {
  val LOG = LogFactory.getLog(this.getClass)

  val subgraphNode = setToSortedArray()
  val subgraphSize = subgraphNode.length
  val supergraphNumNodes = superGraph.numNodes()
  val supergraphNode = Array.fill[Int](supergraphNumNodes)(-1)

  (0 to subgraphSize-1).foreach(i =>{
    supergraphNode(subgraphNode(i)) = i
  })

  if (subgraphSize > 0 && subgraphNode(subgraphSize - 1) >= supergraphNumNodes) throw new IllegalArgumentException("Subnode index out of bounds: "+subgraphNode(subgraphSize - 1))


  private def setToSortedArray():Array[Int] = {
    nodesSet.toArray.sorted
  }

  def toSupergraphNode(x:Int):Int = {
    if (x < 0 || x>= subgraphSize) throw new IllegalArgumentException
    subgraphNode(x)
  }

  def fromSupergraphNode(x:Int):Int = {
    if (x < 0 || x>= supergraphNumNodes) throw new IllegalArgumentException
    supergraphNode(x)
  }

  def getSemanticArcList() = {
    LOG.info(String.format("Getting a subgraph of size %s from a supergraph of size %s.",subgraphSize.toString,supergraphNumNodes.toString))

    val tmpArcList = new ListBuffer[(Int,Int,Float)]
    val iter = superGraph.nodeIterator()
    iter.next

    var lastVisited = 0
    (0 to subgraphSize-1).foreach(subIdx => {
      val currIdx = subgraphNode(subIdx)
      val step = currIdx-lastVisited
      iter.skip(step)
      lastVisited = currIdx

      val outdegree = iter.outdegree()
      val succs = iter.successorArray()
      val labels = iter.labelArray()
      (0 to outdegree-1).foreach(pos =>{
        val succIdx = succs(pos)
        val weight = labels(pos).getFloat
        if (supergraphNode(succIdx) >= 0){
          val t = (currIdx,succIdx,weight)
          val tr = (succIdx,currIdx,weight)
          tmpArcList += t
          tmpArcList += tr
        }
      })
    })

    LOG.info("Done.")
    tmpArcList
  }
}
