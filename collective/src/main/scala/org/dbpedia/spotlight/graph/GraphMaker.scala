package org.dbpedia.spotlight.graph

import java.io.File
import es.yrbcn.graph.weighted.WeightedBVGraph

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/13/12
 * Time: 6:14 PM
 */

/**
 * Just a Object to run the whole process of creating the graphs
 */
object GraphMaker {
  def main(args: Array[String]) {
    val baseDir = "graph/"

    createDir(baseDir)

    val uriMapFile = new File(baseDir+"uriMap.tsv")

    //Generating for occurrences files
    val occSubDir = baseDir+"occs/"
    createDir(occSubDir)
    val occsSrcFile = new File("/home/hector/Researches/nlp/DBpedia_Spotlight/dbpedia-spotlight/index/output/occs.tsv")
    val occInterListFile = new File(occSubDir+"occsIntegerList.tsv")
    val occBaseName = occSubDir + "occsGraph"

    //Generate the host map, number of nodes is useful to build a normal graph
//    val numberOfNodes = HostMap.parseToHostMap(occsSrcFile,uriMapFile)
    //Get the host map
    val hostMap = HostMap.load(uriMapFile)

    //parse the occSrcFile and store the parsed result as an IntegerList
    val wog = new WikipediaOccurrenceGraph
    wog.parseOccsList(occsSrcFile,hostMap, occInterListFile)

    //build a weighted graph and store
    val ocwg = GraphUtils.buildWeightedGraph(occInterListFile)
    GraphUtils.storeWeightedGraph(ocwg,occBaseName)

    //Generating for co-occurrences files
    val cooccSubDir = baseDir+"co-occs/"
    createDir(cooccSubDir)
    val cooccsSrcFile = new File("/home/hector/Researches/nlp/DBpedia_Spotlight/dbpedia-spotlight/index/output/co-occs-count.tsv")
    val cooccInterListFile = new File(cooccSubDir+"cooccsIntegerList.tsv")
    val cooccBaseName = cooccSubDir + "cooccsGraph"

    //parse the cooccsSrcFile and store the parsed result as an IntegerList
    val wcg = new WikipediaCooccurrencesGraph
    wcg.parseCooccsList(cooccsSrcFile,hostMap,cooccInterListFile)

    //build a weighted graph and store
    val cowg = GraphUtils.buildWeightedGraph(cooccInterListFile)
    GraphUtils.storeWeightedGraph(cowg,cooccBaseName)
  }

  private def createDir(dirName:String){
     val theDir = new File(dirName);
      if (!theDir.exists())
      {
        println("Creating directory: " + dirName);
        theDir.mkdir();
      }
  }
}