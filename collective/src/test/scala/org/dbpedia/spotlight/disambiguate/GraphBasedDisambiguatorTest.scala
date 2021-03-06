package org.dbpedia.spotlight.disambiguate

import org.dbpedia.spotlight.disambiguate.GraphBasedDisambiguator
import org.junit.Test
import org.junit.Assert.assertEquals
import org.dbpedia.spotlight.model.{SpotlightFactory, SpotlightConfiguration}
import org.dbpedia.spotlight.model.SpotterConfiguration.SpotterPolicy
import org.dbpedia.spotlight.model.SpotlightConfiguration.DisambiguationPolicy

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 5/31/12
 * Time: 10:41 PM
 */

class GraphBasedDisambiguatorTest{
  val spotlightConfigFileName = "../conf/server.properties"
  val graphConfigFileName = "../conf/graph.properties"
  val spotterName = "Default"

  val config = new SpotlightConfiguration(spotlightConfigFileName)
  val factory = new SpotlightFactory(config)

  val spotterPolicy = SpotterPolicy.valueOf(spotterName)
  val spotter = factory.spotter(spotterPolicy)

  val disambiguator = new ParagraphDisambiguatorJ(new GraphBasedDisambiguator(factory.candidateSearcher,factory.contextSearcher,graphConfigFileName))

  @Test
  def testDisambiguatorName() {
      val name = "GraphBasedDisambiguatorRunner"
      println(disambiguator.name)
      assertEquals(name,disambiguator.name)
   }
}