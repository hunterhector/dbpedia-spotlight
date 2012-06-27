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
import org.dbpedia.spotlight.graph.ReferentGraph
import org.apache.lucene.index.Term
import org.dbpedia.spotlight.lucene.LuceneManager.DBpediaResourceField
import org.apache.lucene.search.similar.MoreLikeThis
import com.officedepot.cdap2.collection.CompactHashSet
import org.apache.lucene.search.ScoreDoc
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 5/30/12
 * Time: 3:56 PM
 */

/**
 * This class implements ParagraphDisambiguator. A graph relation graph
 * will be constructed to leverage the overall disambiguation decisions.
 *
 * Disambiguators implemented in the collective module can only accept
 * paragraphs as parameters. One occurrence cannot be disambiguated
 * collectively
 *
 * @author hectorliu
 */

class GraphBasedDisambiguator(val factory: SpotlightFactory) extends ParagraphDisambiguator {

  val configuration = factory.configuration

  private val LOG = LogFactory.getLog(this.getClass)

  LOG.info("Initializing disambiguator object ...")

  //similar intialization copied from TwoStepDisambiguator
  val contextIndexDir = LuceneManager.pickDirectory(new File(configuration.getContextIndexDirectory))
  val contextLuceneManager = new LuceneManager.CaseInsensitiveSurfaceForms(contextIndexDir)
  // use this if all surface forms in the index are lower-cased
  val cache = JCSTermCache.getInstance(contextLuceneManager, configuration.getMaxCacheSize);
  contextLuceneManager.setContextSimilarity(new CachedInvCandFreqSimilarity(cache)) // set most successful Similarity
  contextLuceneManager.setDBpediaResourceFactory(configuration.getDBpediaResourceFactory)
  contextLuceneManager.setDefaultAnalyzer(configuration.getAnalyzer)
  val contextSearcher: MergedOccurrencesContextSearcher = new MergedOccurrencesContextSearcher(contextLuceneManager)

  var candidateSearcher: CandidateSearcher = null
  //TODO move to factory
  var candLuceneManager: LuceneManager = contextLuceneManager;
  if (configuration.getCandidateIndexDirectory != configuration.getContextIndexDirectory) {
    val candidateIndexDir = LuceneManager.pickDirectory(new File(configuration.getCandidateIndexDirectory))
    //candLuceneManager = new LuceneManager.CaseSensitiveSurfaceForms(candidateIndexDir)
    candLuceneManager = new LuceneManager(candidateIndexDir)
    candLuceneManager.setDBpediaResourceFactory(configuration.getDBpediaResourceFactory)
    candidateSearcher = new LuceneCandidateSearcher(candLuceneManager, true) // or we can provide different functionality for surface forms (e.g. n-gram search)
    LOG.info("CandidateSearcher initialized from %s".format(candidateIndexDir))
  } else {
    candidateSearcher = contextSearcher match {
      case cs: CandidateSearcher => cs
      case _ => new LuceneCandidateSearcher(contextLuceneManager, false) // should never happen
    }
  }


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
    mlt.setAnalyzer(contextLuceneManager.defaultAnalyzer)

    val inputStream = new ByteArrayInputStream(context.getBytes("UTF-8"));
    val query = mlt.like(inputStream)
    contextSearcher.getHits(query, allowedUris.size, 50000, filter)
  }

  /**
   * Executes disambiguation per paragraph (collection of occurrences).
   * Can be seen as a classification task: unlabeled instances in, labeled instances out.
   *
   * Will use a graph based method to leverage all entity disambiguation decision in paragraph together
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

    val scoredSf2Cands = getScoredCandidates(paragraph)

    val sfImportances = getSurfaceImportances(paragraph)

    //TODO: must be in config
    val offline = true
    val baseDir = "graph/"
    val occGraphBasename = baseDir+"occs/occsGraph"
    val cooccGraphBasename = baseDir+"co-occs/cooccsGraph"

    val occGraph: ArcLabelledImmutableGraph = if (offline) ArcLabelledImmutableGraph.loadOffline(occGraphBasename) else ArcLabelledImmutableGraph.loadSequential(occGraphBasename)
    val cooccGraph: ArcLabelledImmutableGraph = if (offline) ArcLabelledImmutableGraph.loadOffline(cooccGraphBasename) else ArcLabelledImmutableGraph.loadSequential(cooccGraphBasename)

    val rGraph = new ReferentGraph(scoredSf2Cands, sfImportances, occGraph, cooccGraph)

    null
  }

  def getScoredCandidates(paragraph: Paragraph) : Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]]  = {
    val allCandidates = CompactHashSet[DBpediaResource];

    val sf2CandidatesMap = paragraph.occurrences.foldLeft(
      Map[SurfaceFormOccurrence, List[DBpediaResource]]()
    )(
      (sf2Cands, sfOcc) => {
        val cands = getCandidates(sfOcc.surfaceForm).toList
        //debug
        cands.foreach(r => allCandidates.add(r))
        sf2Cands + (sfOcc -> cands)
      })

    var hits : Array[ScoreDoc] = null

    try {
      hits = query(paragraph.text, allCandidates.toArray)
    } catch {
      case e: Exception => throw new SearchException(e);
      case r: RuntimeException => throw new SearchException(r);
      case _ => LOG.error("Unknown really scary error happened. You can cry now.")
    }

    val scores = hits
      .foldRight(Map[String,(DBpediaResource,Double)]())((hit,acc) => {
      val resource: DBpediaResource = contextSearcher.getDBpediaResource(hit.doc, Array(LuceneManager.DBpediaResourceField.URI.toString)) //this method returns resource.support=c(r)
      val score = hit.score.toDouble
      acc + (resource.uri -> (resource,score))
    })

    //build up a map for compatible edges
    val scoredSf2Cands = sf2CandidatesMap.foldLeft(Map[SurfaceFormOccurrence,List[DBpediaResourceOccurrence]]()) (( edgesMap, sftoCands) => {
      val sf = sftoCands._1
      val cands = sftoCands._2
      val candOccs = cands.map( shallowResource => {
        val (resource: DBpediaResource, supportConfidence: (Int, Double)) = scores.get(shallowResource.uri) match{
          case Some((fullResource, contextualScore)) => {
            (fullResource,(fullResource.support,contextualScore))
          }
          case _ => (shallowResource,(shallowResource.support,0.0))
        }
        Factory.DBpediaResourceOccurrence.from(sf,resource, supportConfidence)
      })
      edgesMap + (sf -> candOccs)
    })

    scoredSf2Cands
  }

  //could be represented by TF.ICF, TF.IDF or Normalized ones
  //we implement TF.ICF here, because it is more likely to capture the importance of a surface form
  def getSurfaceImportances(paragraph: Paragraph): Map[SurfaceFormOccurrence,Double] = {

  }

  def getCandidates(sf: SurfaceForm): Set[DBpediaResource] = {
    var candidates = new java.util.HashSet[DBpediaResource]().asScala.toSet

    try {
      candidates = candidateSearcher.getCandidates(sf).asScala.toSet
    } catch {
      case se:
        SearchException => LOG.debug(se)
      case infe:
        ItemNotFoundException => LOG.debug(infe)
    }
    candidates
  }

}