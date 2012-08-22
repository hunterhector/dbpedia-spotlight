package org.dbpedia.spotlight.disambiguate

import scala.collection.JavaConversions._
import org.dbpedia.spotlight.model.SpotterConfiguration.SpotterPolicy
import org.dbpedia.spotlight.model.SpotlightConfiguration.DisambiguationPolicy
import org.dbpedia.spotlight.spot.Spotter
import org.dbpedia.spotlight.model._
import org.dbpedia.spotlight.exceptions.SearchException
import org.dbpedia.spotlight.util.GraphConfiguration
import org.apache.commons.logging.LogFactory

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

/**
 * A sample runner to show how things work, you could run this method to see if things are correctly set up
 * Two command line arguments need to be feed: 1. the server.properties that stores basic spotlight settings and
 * 2. the graph.properties file.
 *
 * The result will be simply dumped to standard out, showing both the original and graph-adjusted scores and the best candidate
 *
 * User: hector
 * Date: 5/29/12
 * Time: 4:25 PM
 */


object GraphBasedDisambiguatorRunner {
  val LOG = LogFactory.getLog(this.getClass)

  def main(args: Array[String]) {
    if(args.length != 2) LOG.error("Please specify server config file and graph config file location.")

    val serverConfigFileName = args(0)
    val graphConfigFileName = args(1)
    sampleRun(serverConfigFileName, "Default",graphConfigFileName)
  }

  def sampleRun(configFileName: String, spotterName: String, graphConfigFileName:String) {
    val config = new SpotlightConfiguration(configFileName)
    val factory = new SpotlightFactory(config)

    val spotterPolicy = SpotterPolicy.valueOf(spotterName)
    val spotter = factory.spotter(spotterPolicy)


    val disambiguator = new ParagraphDisambiguatorJ(new GraphBasedDisambiguator(factory.candidateSearcher,factory.contextSearcher,graphConfigFileName))
    LOG.info("Initialization done.")

    val k = 5
    val occList = process(shortCSAWSample, spotter, disambiguator, k)

    for (occ <- occList) {
      println("--------------------------")
      val surOcc = occ._1
      val candidates = occ._2
      println("Surface form spotted: " + surOcc.surfaceForm)
      if (candidates.length > 0){
        val originRanks = candidates.sortBy(_.contextualScore).reverse
        println(String.format("===Original best is: %s, Adjusted Best is: %s===",originRanks(0).resource,candidates(0).resource))
        println("Showing the best " + k + "(if any) candidates for this surface form:")
        candidates.foreach(candidate => println(String.format("\t-->%s,%s,Similarity Score:%s,Contextual Score:%s,Precentage of Second Rank:%s",candidate.surfaceForm, candidate.resource, candidate.similarityScore.toString, candidate.contextualScore.toString, candidate.percentageOfSecondRank.toString)))
      }
    }
  }

  def process(text: String, spotter: Spotter, disambiguator: ParagraphDisambiguatorJ, numOfResults: Int): List[(SurfaceFormOccurrence, java.util.List[DBpediaResourceOccurrence])] = {
    val spots: List[SurfaceFormOccurrence] = spotter.extract(new Text(text)).toList

    var bestKforOccs: List[(SurfaceFormOccurrence, java.util.List[DBpediaResourceOccurrence])] = List()

    if (spots.size == 0) return bestKforOccs

    try {
      bestKforOccs = disambiguator.bestK(Factory.paragraph.fromJ(spots), numOfResults).toList
    }
    catch {
      case e: UnsupportedOperationException => {
        throw new SearchException(e)
      }
    }
    bestKforOccs
  }

  val shortPassage = "Soccer Aid: England glory\n\nEngland regained the Soccer Aid crown" +
    "with a deserved 3-1 win over the Rest of the World at Old Trafford on Sunday evening.\n " +
    "Goals from former United striker Teddy Sheringham, actor Jonathan Wilkes and ex-Sunderland" +
    "star Kevin Phillips secured a third Soccer Aid win for the Three Lions who had earlier gone behind" +
    "to a sublime strike from Kasabian guitarist Sergio Pizzorno.\n"

  val passage = "Soccer Aid: England glory\n\nEngland regained the Soccer Aid crown" +
    "with a deserved 3-1 win over the Rest of the World at Old Trafford on Sunday evening.\n " +
    "Goals from former United striker Teddy Sheringham, actor Jonathan Wilkes and ex-Sunderland" +
    "star Kevin Phillips secured a third Soccer Aid win for the Three Lions who had earlier gone behind" +
    "to a sublime strike from Kasabian guitarist Sergio Pizzorno.\n\nJust over 67,000 fans - together" +
    "with some famous faces including Wayne Rooney - were packed inside the Reds' stadium for Soccer Aid " +
    "2012, which once again raised millions of pounds for global children’s charity UNICEF. The crowd " +
    "and peak-time television audience alike were treated to a highly entertaining evening involving a " +
    "host of football legends and some of the world’s biggest celebrities from the world of TV, film and " +
    "music.\n\nThe Rest of the World, who went into the game as defending champions, may have had the " +
    "advantage when it came to the number of former United players on their side – Edwin van der Sar " +
    "lined up in goal with Roy Keane and Jaap Stam starting together at the heart of the defence - but " +
    "it was England who made the early running.\n\nThe Three Lions had good pace on both wings with the " +
    "young legs of X Factor star Olly Murs, on the right, and JLS’s Aston Merrygold down the left. And " +
    "both had a couple of early sighters. A neat passing move ended with Merrygold striking an effort " +
    "straight at Van der Sar, before the Dutch stopper saved well with his feet after an excellent run " +
    "and shot from United fan Murs, coming off the right wing.\n\nPhillips blasted both a shot and a " +
    "free-kick over the bar soon after, before the Rest of the World"

    val shortCSAWSample = "Primary Navigation Secondary Navigation Search: Nearly 60 militants killed in southern Afghanistan Tue Oct 7, 9:14 AM ET"+
    "KABUL (Reuters) - U.S.-led coalition and Afghan security forces killed nearly 60 militants during separate clashes in southern"+
    "Afghanistan, the U.S. military and a police official said Tuesday."

    val CSAWSample="Primary Navigation Secondary Navigation Search: Nearly 60 militants killed in southern Afghanistan Tue Oct 7, 9:14 AM ET"+
    "KABUL (Reuters) - U.S.-led coalition and Afghan security forces killed nearly 60 militants during separate clashes in southern"+
    "Afghanistan, the U.S. military and a police official said Tuesday. Violence has surged in the war-torn country with some 3,800 people,a"+
    "third of them civilians, killed as a result of the conflict by the end of July this year, according to the United Nations. U.S.-led"+
    "coalition and Afghan security forces killed 43 militants during heavy fighting in Qalat district of southern Zabul province Sunday, the"+
    "U.S. military said in a statement Tuesday. 'ANSF (Afghan National Security Forces ) and coalition forces on a patrol received heavy"+
    "weapons, machine gun and sniper fire from militants in multiple locations,' the U.S. military said in a statement. The combined forces"+
    "responded with small arms fire , rocket propelled grenades and close air support , killing the militants, it said. No Afghan or U.S.-led"+
    "troops were killed or wounded during incident, it said. In a separate incident, Afghan and international troops killed 16 Taliban"+
    "insurgents and wounded six more during a gun battle in Nad Ali district of southern Helmand province on Monday, provincial police chief"+
    "Asadullah Sherzad told Reuters. (Writing by Jonathon Burch; Editing by Bill Tarrant)" 
}
