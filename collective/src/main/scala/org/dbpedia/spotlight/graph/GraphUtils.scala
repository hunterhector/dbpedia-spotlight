package org.dbpedia.spotlight.graph

import org.apache.commons.logging.LogFactory
import scala.io.Source
import it.unimi.dsi.webgraph.labelling._
import scala.Predef._
import java.io.File
import java.lang.IllegalArgumentException
import it.unimi.dsi.webgraph.{ImmutableGraph, BVGraph, ArrayListMutableGraph}
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
    LOG.info(String.format("Creating a graph with %s number of nodes",numNodes.toString) )

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

  def buildWeightedGraph (integerListFile: File) : ArcLabelledImmutableGraph = {
    LOG.info(String.format("Creating a weigted graph"))

    val WeightedArcArray = Source.fromFile(integerListFile).getLines().filterNot(line => line.trim() == "").filter(line => line.split("\t").length >= 3).map(line => new WeightedArc(line)).toArray

    val aig : ArcLabelledImmutableGraph = new WeightedBVGraph(WeightedArcArray)

    LOG.info(String.format("Done. Created a weighted Graph with %s nodes",aig.numNodes().toString))

    aig
  }

  def buildArcLabelledGraph (integerListFile: File) : ArcLabelledImmutableGraph = {
    LOG.info("Creating an Arc labelled graph")

    val arcArray = Source.fromFile(integerListFile).getLines().filterNot(line => line.trim() == "").filter(line => line.split("\t").length >= 3).map(line => line.split("\t").map(str => str.toFloat.toInt)).toArray

    val alg : ArcLabelledImmutableGraph = new IntegerTriplesArcLabelledImmutableGraph(arcArray)

    LOG.info(String.format("Done. Created a Arc Labelled Graph with %s nodes",alg.numNodes().toString))

    alg
  }

  def buildArcLabelledGraph (integerList: Array[Array[Int]]) : ArcLabelledImmutableGraph = {
    val alg : ArcLabelledImmutableGraph = new IntegerTriplesArcLabelledImmutableGraph(integerList)

    LOG.info(String.format("Done. Created a Arc Labelled Graph with %s nodes",alg.numNodes().toString))

    alg
  }

  def dumpLabelledGraph(g: ArcLabelledImmutableGraph) = {
    val nodeIterator: ArcLabelledNodeIterator = g.nodeIterator

    while (nodeIterator.hasNext) {
      val curr = nodeIterator.nextInt
      val suc = nodeIterator.successorArray
      val lab = nodeIterator.labelArray
      (suc,lab).zipped.foreach((s,l)=>println(curr + "\t" + s + "\t" + l.getFloat))
    }
  }

  def storeGraph(g: ImmutableGraph, baseName: String) = {
    LOG.info("Storing Graph to " + baseName)
    BVGraph.store(g, baseName + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX)
    LOG.info("Graph generated.")
  }

  def storeWeightedGraph(g:ArcLabelledImmutableGraph, baseName: String) = {
    LOG.info("Storing Weighted Graph to " + baseName)

    LOG.info("Storing Labels")
    BitStreamArcLabelledImmutableGraph.store(g,baseName,baseName+ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX)

    LOG.info("Compressing Graph")
    BVGraph.store(g, baseName + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX)

    LOG.info("Graph generated.")
  }

  def loadGraph(baseName:String) = {
    LOG.info("Loading the Graph from " + baseName)
    BVGraph.load(baseName)
    LOG.info("Done.")
  }

/*  def weightedPageRank(g: ArcLabelledImmutableGraph) ={
    val prCal = new WeightedPageRankPowerMethod(g)
    prCal.stepUntil(new WeightedPageRank.IterationNumberStoppingCriterion(10)) //test purpose
    LOG.info("Dumping the ranking result")



    prCal.stepUntil( WeightedPageRank.or(new WeightedPageRank.NormDeltaStoppingCriterion(0.1), new WeightedPageRank.IterationNumberStoppingCriterion(10)))



    print (prCal.rank)
  }*/
}