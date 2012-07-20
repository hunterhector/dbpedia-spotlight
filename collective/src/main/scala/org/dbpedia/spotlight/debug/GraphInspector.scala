package org.dbpedia.spotlight.debug

import java.io.File
import org.dbpedia.spotlight.graph.SemanticSubGraph
import org.dbpedia.spotlight.util.{GraphConfiguration, GraphUtils}
import org.apache.commons.logging.LogFactory
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 7/2/12
 * Time: 12:27 PM
 */

object GraphInspector {
  val LOG = LogFactory.getLog(this.getClass)
  val graphConfig = new GraphConfiguration("../conf/graph.properties")
  private val offline = "true" == graphConfig.getOrElse("org.dbpedia.spotlight.graph.offline","false")

  LOG.info("Preparing graphs...")
  private val baseDir = graphConfig.get("org.dbpedia.spotlight.graph.dir")
  private val occDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.occ.dir")
  private val occGraphBasename = graphConfig.get("org.dbpedia.spotlight.graph.occ.basename")
  private val cooccDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.coocc.dir")
  private val cooccGraphBasename = graphConfig.get("org.dbpedia.spotlight.graph.coocc.basename")
  private val occTransposeDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.occ.dir")
  private val occTransposeGraphBaseName = graphConfig.get("org.dbpedia.spotlight.graph.transpose.occ.basename")
  private val sgDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.dir")
  private val sgBasename = graphConfig.get("org.dbpedia.spotlight.graph.semantic.basename")

  private val owg = GraphUtils.loadAsArcLablelled(occDir,occGraphBasename,offline)
  private val rowg = GraphUtils.loadAsArcLablelled(occTransposeDir,occTransposeGraphBaseName, offline)
  private val cwg = GraphUtils.loadAsArcLablelled(cooccDir,cooccGraphBasename,offline)
  private val sg = GraphUtils.loadAsArcLablelled(sgDir,sgBasename,offline)

  LOG.info("Done")

  def checkStats(){
    LOG.info(String.format("In occ transpose graph, Number of nodes:  %s; Number of arcs: %s",rowg.numNodes().toString,rowg.numArcs().toString))
    LOG.info(String.format("In occ graph, Number of nodes:  %s; Number of arcs: %s",owg.numNodes().toString,owg.numArcs().toString))
    LOG.info(String.format("In coocc graph, Number of nodes:  %s; Number of arcs: %s",cwg.numNodes().toString,cwg.numArcs().toString))
    LOG.info(String.format("In semantic graph, Number of nodes: %s; Number of arcs: %s",sg.numNodes().toString,sg.numArcs().toString))
  }

  def checkFirstNode(){
    val iterOcc1 = owg.nodeIterator(0)
    try{
      LOG.info(String.format("Access iter()'s element: %s",iterOcc1.outdegree().toString))
    }catch{
      case e => LOG.info(String.format("An %s will be thrown if you access iter(0) directly",e))
    }

    val firstNodeIdx = iterOcc1.nextInt()
    LOG.info("So the first node index is "+firstNodeIdx)

    val outdegreeOcc1 = iterOcc1.outdegree()
    val labelsOcc1 = iterOcc1.labelArray()
    val succsOcc1 = iterOcc1.successorArray()
    LOG.info(String.format("It points to %s nodes.",outdegreeOcc1.toString))
    (0 to outdegreeOcc1-1).foreach(i => {
      LOG.info(String.format("Counts: %s, Succ: %s",labelsOcc1(i).getInt.toString,succsOcc1(i).toString))
    })

    val iterOcc2 = owg.nodeIterator(1)
    val secondNodeIdx = iterOcc2.nextInt()
    LOG.info("So the second node index is "+secondNodeIdx)

    val outdegreeOcc2 = iterOcc2.outdegree()
    val labelsOcc2 = iterOcc2.labelArray()
    val succsOcc2 = iterOcc2.successorArray()
    LOG.info(String.format("It points to %s nodes.",outdegreeOcc2.toString))
    (0 to outdegreeOcc2 -1).foreach(i=>{
      LOG.info(String.format("counts: %s, Succ: %s",labelsOcc2(i).getInt.toString,succsOcc2(i).toString))
    })
  }

  def checkNode(g:ArcLabelledImmutableGraph,x:Int){
    val iter = g.nodeIterator(x)
    val idx = iter.nextInt()
    LOG.info(String.format("Checking node %s",idx.toString))
    LOG.info("Outdegree is "+iter.outdegree)
  }

  def checkLastNode(){
    val lastNodeIdx = owg.numNodes()
    LOG.info("So the last node index is "+lastNodeIdx)

    val iterCo = cwg.nodeIterator(lastNodeIdx)
    val outdegreeCo = iterCo.outdegree()
    val labelsCo = iterCo.labelArray()
    val succsCo = iterCo.successorArray()
    LOG.info("Number of co-occurred nodes is "+outdegreeCo)
    (0 to outdegreeCo-1).foreach(i => {
      LOG.info(String.format("Counts: %s, Succs: %s",labelsCo(i).getInt.toString,succsCo(i).toString))
    })

    val iterOcc = owg.nodeIterator(lastNodeIdx)
    val outdegreeOcc = iterOcc.outdegree()
    val labelsOcc = iterOcc.labelArray()
    val succsOcc = iterOcc.successorArray()
    LOG.info(String.format("It points to %s nodes ",outdegreeOcc.toString))
    (0 to outdegreeOcc-1).foreach(i => {
      LOG.info(String.format("Counts: %s, Sucss: %s",labelsOcc(i).getInt.toString,succsOcc(i).toString))
    })

    val iterOccTran = rowg.nodeIterator(lastNodeIdx)
    val outdegreeOccTran = iterOccTran.outdegree()
    val labelsOccTran = iterOccTran.labelArray()
    val succsOccTran = iterOccTran.successorArray()
    LOG.info(String.format("It occurs in %s nodes ",outdegreeOccTran.toString))
    (0 to outdegreeOccTran-1).foreach(i => {
      LOG.info(String.format("Counts: %s, Sucss: %s",labelsOccTran(i).getInt.toString,succsOccTran(i).toString))
    })

    try {
      LOG.info("The next Int is actually "+iterCo.nextInt())
    }catch{
      case e => LOG.info(String.format("So you will note that nextInt won't exists, an %s will be thrown",e))
    }
  }

  def checkTraversal(g: ArcLabelledImmutableGraph){
    LOG.info("Let's see how long does it take to traverse the graph and successros/labels")
    val lastNodeIdx = g.numNodes()

    val iter = g.nodeIterator()
    (1 to lastNodeIdx).foreach(nodeCount =>{
      val currIdx = iter.nextInt()
      val outdegree = iter.outdegree()
      val succs = iter.successorArray()
      val labels = iter.labelArray()
      (0 to outdegree -1).foreach(pos => {
        val a = succs(pos)
        val b = labels(pos)
      })
      if (nodeCount%1000000 == 0)
        LOG.info(nodeCount+" nodes visited.")
    })
    LOG.info("Finished!")
  }

  def checkSubGraph(g: ArcLabelledImmutableGraph,set:Set[Int]) {

    val sg = new SemanticSubGraph(g,set)
    val arcList =  sg.getSemanticArcList()
    arcList.foreach(println)
  }

  def main(args:Array[String]) {
//    checkStats()
//    checkNode(owg,0)
//    checkNode(owg,1)
//    checkNode(owg,2)
//    checkNode(owg,3823752)
    checkNode(sg,249)
    checkSubGraph(sg,Set(0,1,2,3,187,249,3823752))
  }
}
