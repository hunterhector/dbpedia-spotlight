package org.dbpedia.spotlight.util

import org.apache.commons.logging.LogFactory
import scala.io.Source
import it.unimi.dsi.webgraph.labelling._
import scala.Predef._
import java.io.File
import java.lang.IllegalArgumentException
import it.unimi.dsi.webgraph.{Transform, ImmutableGraph, BVGraph, ArrayListMutableGraph}
import it.unimi.dsi.webgraph.examples.IntegerTriplesArcLabelledImmutableGraph
import es.yrbcn.graph.weighted._

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/11/12
 * Time: 5:00 PM
 *
 * Methods to access and to store graphs
 *
 * @author hector.liu
 */

object GraphUtils {
  val LOG = LogFactory.getLog(this.getClass)

  def buildGraph(integerListFile: File, numNodes: Int): ImmutableGraph = {
    val graph = new ArrayListMutableGraph()
    graph.addNodes(numNodes)
    LOG.info(String.format("Creating a graph with %s number of nodes", numNodes.toString))

    Source.fromFile(integerListFile).getLines().filterNot(line => line.trim() == "").foreach(
      line => {
        val fields = line.split("\\t")

        if (fields.length == 3) {
          val src = fields(0).toInt
          val target = fields(1).toInt

          try {
            graph.addArc(src, target)
          } catch {
            case illArg: IllegalArgumentException => {
              //LOG.warn(String.format("Duplicate inlinks from %s to %s ignored", fields(0), fields(1)))
            }
          }
          //LOG.info(String.format("Added arc from %s to %s",fields(0),fields(1)))

        } else {
          LOG.error(String.format("Wrong line formatting at: \n -> \t%s Input file should be triple seperated by tabs", line))
        }
      }
    )

    LOG.info("Done.")
    graph.immutableView()

  }

  def buildWeightedGraphFromFile(integerListFile: File): ArcLabelledImmutableGraph = {
    LOG.info(String.format("Creating a weigted graph"))

    val weightedArcArray = Source.fromFile(integerListFile).getLines().filterNot(line => line.trim() == "").filter(line => line.split("\t").length >= 3).map(line => new WeightedArc(line)).toArray

    val aig: ArcLabelledImmutableGraph = new WeightedBVGraph(weightedArcArray)

    LOG.info(String.format("Done. Created a weighted Graph with %s nodes", aig.numNodes().toString))

    aig
  }

  def buildWeightedGraphFromTriples(triples:List[(Int,Int,Float)]): ArcLabelledImmutableGraph = {
    LOG.info("Creating a weighted graph")

    val weightedArcArray = triples.map(tuple => new WeightedArc(tuple._1,tuple._2,tuple._3)).toArray

    val aig: ArcLabelledImmutableGraph = new WeightedBVGraph(weightedArcArray)

    LOG.info(String.format("Done. Created a weighted Graph with %s nodes", aig.numNodes().toString))

    aig
  }

  def buildArcLabelledGraph(integerListFile: File): ArcLabelledImmutableGraph = {
    LOG.info("Creating an Arc labelled graph")

    val arcArray = Source.fromFile(integerListFile).getLines().filterNot(line => line.trim() == "").filter(line => line.split("\t").length >= 3).map(line => line.split("\t").map(str => str.toFloat.toInt)).toArray

    val alg: ArcLabelledImmutableGraph = new IntegerTriplesArcLabelledImmutableGraph(arcArray)

    LOG.info(String.format("Done. Created a Arc Labelled Graph with %s nodes", alg.numNodes().toString))

    alg
  }

  def dumpLabelledGraph(g: ArcLabelledImmutableGraph) = {
    val nodeIterator: ArcLabelledNodeIterator = g.nodeIterator

    while (nodeIterator.hasNext) {
      val curr = nodeIterator.nextInt
      val suc = nodeIterator.successorArray
      val lab = nodeIterator.labelArray
      (suc, lab).zipped.foreach((s, l) => println(curr + "\t" + s + "\t" + l.getFloat))
    }
  }

  def storeGraph(g: ImmutableGraph, baseName: String) = {
    LOG.info("Storing Graph to " + baseName)
    BVGraph.store(g, baseName + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX)
    LOG.info("Graph generated.")
  }

  def storeWeightedGraph(g: ArcLabelledImmutableGraph, baseName: String) = {
    LOG.info("Storing Weighted Graph to " + baseName)

    LOG.info("Storing Labels")
    BitStreamArcLabelledImmutableGraph.store(g, baseName, baseName + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX)

    LOG.info("Compressing Graph")
    BVGraph.store(g, baseName + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX)

    LOG.info("Graph generated.")
  }

  def loadBVGraph(baseName: String) = {
    LOG.info("Loading the Graph from " + baseName)
    val g = BVGraph.load(baseName)
    LOG.info("Done.")
    g
  }

  def loadAsImmutable(baseName:String, offline: Boolean) = {
    LOG.info("Loading the graph from "+baseName+" as ImmutableGraph")
    val g = if (offline) ImmutableGraph.loadOffline(baseName) else ImmutableGraph.load(baseName)
    LOG.info("Done")
    g
  }

  def loadAsArcLablelled(baseName:String, offline: Boolean) = {
    LOG.info("Loading the graph from "+baseName+" as ArcLablelledGraph")
    val g = if (offline) ArcLabelledImmutableGraph.loadOffline(baseName) else ArcLabelledImmutableGraph.load(baseName)
    LOG.info("Done")
    g
  }

  def transpose(g:ArcLabelledImmutableGraph,batchSize:Int) = {
    LOG.info("Trasposing the graph...")
    val tg = Transform.transposeOffline(g,batchSize)
    LOG.info("Done.")
    tg
  }

  def main(args:Array[String]){
    LOG.info("Testing graph loading")
//    val immutableG = loadAsImmutable("graph/occs/occsGraph",false)
//    println(immutableG.randomAccess())
//    println(immutableG.numNodes())

//    val archG = loadAsArcLablelled("graph/occs/occsGraph",false)
//    val revArchG = Transform.transposeOffline(archG,100000)
//    println(revArchG.randomAccess())
//
//    storeWeightedGraph(revArchG,"/home/hector/Researches/nlp/DBpedia_Spotlight/dbpedia-spotlight/collective/graph/occs/occTransposeGraph")
    val revArchRandomG = loadAsArcLablelled("/home/hector/Researches/nlp/DBpedia_Spotlight/dbpedia-spotlight/collective/graph/occs/occTransposeGraph",false)
    println(revArchRandomG.randomAccess())
    println(revArchRandomG.numNodes())

    val i2 = revArchRandomG.nodeIterator(159)
    val outdegree = i2.outdegree()
    println(outdegree)
    val labelArr = i2.labelArray()
    val succ = i2.successorArray()
    (0 to outdegree).foreach(i => {
      println (labelArr(i))
      println (succ(i))
    })
  }
}