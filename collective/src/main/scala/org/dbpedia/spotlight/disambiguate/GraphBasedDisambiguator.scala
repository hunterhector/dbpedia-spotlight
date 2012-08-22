/*
 * Copyright 2012 DBpedia Spotlight Development Team
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  Check our project website for information on how to acknowledge the authors and how to contribute to the project: http://spotlight.dbpedia.org
 */

package org.dbpedia.spotlight.disambiguate

import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.lucene.LuceneManager
import java.io.{ByteArrayInputStream, File}
import org.dbpedia.spotlight.lucene.similarity.{CachedInvCandFreqSimilarity, JCSTermCache}
import org.dbpedia.spotlight.lucene.search.{MergedOccurrencesContextSearcher, LuceneCandidateSearcher}
import scala.collection.JavaConverters._
import org.dbpedia.spotlight.exceptions.{ItemNotFoundException, SearchException, InputException}
import org.dbpedia.spotlight.exceptions.SearchException
import org.dbpedia.spotlight.model._
import org.dbpedia.spotlight.graph.{HostMap, ReferentGraph}
import org.apache.lucene.index.Term
import org.dbpedia.spotlight.lucene.LuceneManager.DBpediaResourceField
import org.apache.lucene.search.similar.MoreLikeThis
import com.officedepot.cdap2.collection.{CompactHashMap, CompactHashSet}
import org.apache.lucene.search.ScoreDoc
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph
import org.dbpedia.spotlight.util.{GraphUtils, GraphConfiguration}
import it.unimi.dsi.webgraph.{ImmutableGraph, Transform}

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 5/30/12
 * Time: 3:56 PM
 */

/**
 * This class implements ParagraphDisambiguator. A graph will be constructed to leverage the overall disambiguation
 * decisions in the hope to improve the overall disambiguation performance
 *
 * Disambiguators implemented in the collective module can be operated on paragraphs. One occurrence cannot be
 * disambiguated collectively. So when doing evaluation, please use one that evaluate paragraph.
 * @author hectorliu
 */

class GraphBasedDisambiguator(val candidateSearcher: CandidateSearcher, val contextSearcher:MergedOccurrencesContextSearcher, val graphConfigFileName: String) extends ParagraphDisambiguator {

  val graphConfig = new GraphConfiguration(graphConfigFileName)

  private val LOG = LogFactory.getLog(this.getClass)

  LOG.info("Initializing disambiguator object ...")

  LOG.info("Loading graphs...")
  private val offline = "true" == graphConfig.getOrElse("org.dbpedia.spotlight.graph.offline","false")
  private val uriMapFile = new File(graphConfig.get("org.dbpedia.spotlight.graph.dir")+graphConfig.get("org.dbpedia.spotlight.graph.mapFile"))
  private val uri2IdxMap = HostMap.load(uriMapFile)

  LOG.info("Preparing graphs...")
  private val baseDir = graphConfig.get("org.dbpedia.spotlight.graph.dir")
  private val sgSubDir = baseDir+graphConfig.get("org.dbpedia.spotlight.graph.semantic.dir")
  private val sgBasename = graphConfig.get("org.dbpedia.spotlight.graph.semantic.basename")

  private val sg = GraphUtils.loadAsArcLablelled(sgSubDir,sgBasename,offline)

  /**
   * Every disambiguator has a name that describes its settings (used in evaluation to compare results)
   * @return a short description of the Disambiguator
   */
  def name = {
    this.getClass.getSimpleName
  }

  def query(text: Text, allowedUris: Array[DBpediaResource]) = {
    LOG.debug("Setting up query")
    val context = if (text.text.size < 250) text.text.concat(" "+text.text) else text.text

    val filter = new org.apache.lucene.search.TermsFilter()
    allowedUris.foreach( u => filter.addTerm(new Term(DBpediaResourceField.URI.toString,u.uri)))

    val mlt = new MoreLikeThis(contextSearcher.mReader)
    mlt.setFieldNames(Array(DBpediaResourceField.CONTEXT.toString))
    mlt.setAnalyzer(contextSearcher.getLuceneManager.defaultAnalyzer)

    val inputStream = new ByteArrayInputStream(context.getBytes("UTF-8"));
    val query = mlt.like(inputStream)
    contextSearcher.getHits(query, allowedUris.size, 50000, filter)
  }

  /**
   * Executes disambiguation per paragraph (collection of occurrences).
   * Can be seen as a classification task: unlabeled instances in, labeled instances out.
   *
   * @param paragraph
   * @return
   * @throws SearchException
   * @throws InputException
   */
  def disambiguate(paragraph: Paragraph) = {
    //TODO Could consider implement this method in ParagraphDisambiguatorJ
    // Actually this function could be the same for all disambiguators,
    // given that they all needs to implements bestK

    // return first from each candidate set
    bestK(paragraph, 5)
      .filter(kv =>
      kv._2.nonEmpty)
      .map(kv =>
      kv._2.head)
      .toList
  }

  /**
   * Executes disambiguation per paragraph, returns a list of possible candidates.
   * Can be seen as a ranking (rather than classification) task: query instance in, ranked list of target URIs out.
   *
   * @param paragraph
   * @param k
   * @return
   * @throws SearchException
   * @throws ItemNotFoundException    when a surface form is not in the index
   * @throws InputException
   */
  def bestK(paragraph: Paragraph, k: Int): Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]] = {

    LOG.debug("Running bestK for paragraph %s.".format(paragraph.id))

    if (paragraph.occurrences.size == 0) return Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]]()

    val scoredSf2Cands = getContextScore(paragraph)

    val rGraph = new ReferentGraph(sg, scoredSf2Cands, uri2IdxMap, graphConfig)

    rGraph.getResult(k)
  }

  /**
   * Get the initial context score of each possible candidates
   *
   * @param paragraph The Paragraph to be disambiguated
   * @return A map that each surface form occurrence is linked with a list of possible DBpediaResourceOccurrence with context score associated
   */
  private def getContextScore(paragraph: Paragraph) : CompactHashMap[SurfaceFormOccurrence,(List[DBpediaResourceOccurrence],Double)]  = {
    LOG.debug("Getting initial context scores.")
    val allCandidates = CompactHashSet[DBpediaResource]

    val sf2CandidatesMap = paragraph.occurrences.foldLeft(
      CompactHashMap[SurfaceFormOccurrence, List[DBpediaResource]]()
    )(
      (sf2Cands, sfOcc) => {
        val cands = getCandidates(sfOcc.surfaceForm).toList
        cands.foreach(r => allCandidates.add(r))
        sf2Cands += (sfOcc -> cands)
      })

    var hits : Array[ScoreDoc] = null

    try {
      hits = query(paragraph.text, allCandidates.toArray)
    } catch {
      case e: Exception => throw new SearchException(e)
      case r: RuntimeException => throw new SearchException(r)
      case _ => LOG.error("Unknown really scary error happened. You can cry now.")
    }

    val scores = hits
      .foldRight(Map[String,(DBpediaResource,Double)]())((hit,acc) => {
      val resource: DBpediaResource = contextSearcher.getDBpediaResource(hit.doc, Array(LuceneManager.DBpediaResourceField.URI.toString)) //this method returns resource.support=c(r)
      val score = hit.score.toDouble
      acc + (resource.uri -> (resource,score))
    })

    LOG.debug("Building compatible edges and collecting prior importances.")
    //build up a map for compatible edges
    val scoredSf2Cands = sf2CandidatesMap.foldLeft(CompactHashMap[SurfaceFormOccurrence,(List[DBpediaResourceOccurrence],Double)]()) (( edgesMap, sftoCands) => {
      val sfOcc = sftoCands._1
      val cands = sftoCands._2
      val candOccs = cands.map( shallowResource => {
        val (resource: DBpediaResource, supportConfidence: (Int, Double)) = scores.get(shallowResource.uri) match{
          case Some((fullResource, contextualScore)) => {
            (fullResource,(fullResource.support,contextualScore))
          }
          case _ => (shallowResource,(shallowResource.support,0.0))
        }
        Factory.DBpediaResourceOccurrence.from(sfOcc,resource, supportConfidence)
      })
      edgesMap += (sfOcc -> (candOccs,getSurfaceImportance(sfOcc.surfaceForm)))
    })
    scoredSf2Cands
  }

  /**
   * Find the initial importance for a surface form.
   *
   * @param sf the surface form to be examined
   * @return
   */
  def getSurfaceImportance(sf: SurfaceForm): Double = {
    //TODO currently only use ICF, could use the prior importance as the surfaceform evidence

    val cf = contextSearcher.getContextFrequency(sf)
    var icf = 0.0
    if (cf > 0){
      icf  = math.log(contextSearcher.getNumberOfResources/cf)
    }else{
      LOG.debug(String.format("Can't find context frequency for surface form [%s], set icf to 0",sf.toString))
    }

    LOG.debug(String.format("For the surface form: %s, context frequency is %s",sf.toString,icf.toString))
    val sfImportance = icf
    sfImportance
  }

  /**
   * Return possible candidates for one surface form
   * @param sf the ambiguous surface form to find candidates
   * @return Set of candidates that are possible underlying candidates
   */
  def getCandidates(sf: SurfaceForm): Set[DBpediaResource] = {
    var candidates = new java.util.HashSet[DBpediaResource]().asScala

    try {
      candidates = candidateSearcher.getCandidates(sf).asScala
    } catch {
      case se:
        SearchException => LOG.debug(se)
      case infe:
        ItemNotFoundException => LOG.debug(infe)
    }
    candidates.toSet
  }

}
