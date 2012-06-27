package org.dbpedia.spotlight.graph

import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.{DBpediaResourceOccurrence, DBpediaResource, SurfaceFormOccurrence}
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import es.yrbcn.graph.weighted.WeightedPageRankPowerMethod

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
class ReferentGraph(scoredSf2CandsMap: Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]], initialEvidences: Map[SurfaceFormOccurrence,Double], occGraph: ArcLabelledImmutableGraph, cooccGraph: ArcLabelledImmutableGraph) {
  private val LOG = LogFactory.getLog(this.getClass)

  private val s2c = scoredSf2CandsMap
  private val ie = initialEvidences
  private val og = occGraph
  private val cg = cooccGraph
  private val rg = buildReferentGraph()

  def buildReferentArcList () : Array[Array[Int]] = {
    LOG.info("Combining surface forms and candidates into one graph list")
  }

  def buildReferentGraph() : ArcLabelledImmutableGraph = {
    LOG.info("Creating the referent graph")
    val arcList = buildReferentArcList()
    val g= GraphUtils.buildArcLabelledGraph(arcList)
    LOG.info("Done!")
    g
  }

  def getRankedCands(){
    runPageRank()


  }

  def runPageRank(){
    val pr:WeightedPageRankPowerMethod  = new WeightedPageRankPowerMethod(rg)

    pr.stepUntil()

    LOG.info("Saving ranks...");

  }


  def subGraphIndexToUri(){

  }
}
