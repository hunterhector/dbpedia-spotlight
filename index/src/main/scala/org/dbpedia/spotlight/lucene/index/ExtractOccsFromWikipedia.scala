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

package org.dbpedia.spotlight.lucene.index

import java.io.{PrintStream, File}
import org.dbpedia.spotlight.log.SpotlightLog
import org.dbpedia.spotlight.string.ContextExtractor
import org.dbpedia.spotlight.util.IndexingConfiguration
import org.dbpedia.spotlight.filter.occurrences.{RedirectResolveFilter, UriWhitelistFilter, ContextNarrowFilter}
import org.dbpedia.spotlight.io._
import org.dbpedia.spotlight.model.DBpediaResourceOccurrence
import org.dbpedia.spotlight.BzipUtils
import org.dbpedia.extraction.util.Language
import scala.util.matching.Regex
import io.Source

/**
 * Saves Occurrences to a TSV file.
 * - Surface forms are taken from anchor texts
 * - Redirects are resolved
 *
 * TODO think about having a two file output, one with (id, sf, uri) and another with (id, context)
 * TODO allow reading from a bzipped wikiDump (needs upgrading our dependency on the DBpedia Extraction Framework)
 *
 * Used to be called SurrogatesUtil
 *
 * @author maxjakob
 * @author pablomendes (small fixes)
 */
object ExtractOccsFromWikipedia {
  def fixNamespaceError(pathToDumpFile: String) {
    val pattern = new Regex("""<ns>(\w*)</ns>""", "nameSpace")
    val fixedOccsStream = new PrintStream(pathToDumpFile + "_tmp", "UTF-8")
    val source = Source.fromFile(new File(pathToDumpFile),"UTF-8")
    var i = 0
    for (line <- source.getLines()) {
      try {
        if (i % 100000 == 0) SpotlightLog.info(this.getClass, "%s lines processed.", i.toString)
        i += 1
        val badNS = pattern.findFirstMatchIn(line.toString).get.group("nameSpace")
        if (badNS != null) {
          if (badNS.toInt == 828 ) {
            SpotlightLog.info(this.getClass, "Fix namespace 828")
            fixedOccsStream.println(line.replace("828","0"))
          }else if (badNS.toInt == 118  ){
            SpotlightLog.info(this.getClass, "Fix namespace 118")
            fixedOccsStream.println(line.replace("118","0"))
          }else if(badNS.toInt == 119){
            SpotlightLog.info(this.getClass, "Fix namespace 119")
            fixedOccsStream.println(line.replace("119","0"))
          }else if (badNS.toInt == 710){
            SpotlightLog.info(this.getClass, "Fix namespace 710")
            fixedOccsStream.println(line.replace("710","0"))
          }else if (badNS.toInt == 710){
            SpotlightLog.info(this.getClass, "Fix namespace 711")
            fixedOccsStream.println(line.replace("711","0"))
          }
          else {
            fixedOccsStream.println(line)
          }
        }
      } catch {
        case e: Exception => fixedOccsStream.println(line)
        case _ => fixedOccsStream.println(line)
      }
    }
    source.close()
    fixedOccsStream.close()
    val aFile = new File(pathToDumpFile)
    aFile.delete()
    val aTmpFile = new File(pathToDumpFile + "_tmp")
    aTmpFile.renameTo(new File(pathToDumpFile))
  }

  def main(args : Array[String]) {
    val indexingConfigFileName = args(0)
    val targetFileName = args(1)

    val config = new IndexingConfiguration(indexingConfigFileName)
    var wikiDumpFileName    = config.get("org.dbpedia.spotlight.data.wikipediaDump")
    val conceptURIsFileName = config.get("org.dbpedia.spotlight.data.conceptURIs")
    val redirectTCFileName  = config.get("org.dbpedia.spotlight.data.redirectsTC")
    val maxContextWindowSize  = config.get("org.dbpedia.spotlight.data.maxContextWindowSize").toInt
    val minContextWindowSize  = config.get("org.dbpedia.spotlight.data.minContextWindowSize").toInt
    val languageCode = config.get("org.dbpedia.spotlight.language_i18n_code")

    if (wikiDumpFileName.endsWith(".bz2")) {
      SpotlightLog.warn(this.getClass, "The DBpedia Extraction Framework does not support parsing from bz2 files. You can stop here, decompress and restart the process with an uncompressed XML.")
      SpotlightLog.warn(this.getClass, "If you do not stop the process, we will decompress the file into the /tmp/ directory for you.")
      wikiDumpFileName = BzipUtils.extract(wikiDumpFileName)
    }

// Use 4.0-Snapshot of the extraction framework
//    SpotlightLog.info(this.getClass, "Fixing invalid namespaces in the input dump %s ...", wikiDumpFileName)
//    fixNamespaceError(wikiDumpFileName)
//    SpotlightLog.info(this.getClass, "Done.")

    val conceptUriFilter = UriWhitelistFilter.fromFile(new File(conceptURIsFileName))

    val redirectResolver = RedirectResolveFilter.fromFile(new File(redirectTCFileName))

    val narrowContext = new ContextExtractor(minContextWindowSize, maxContextWindowSize)
    val contextNarrowFilter = new ContextNarrowFilter(narrowContext)

    val filters = (conceptUriFilter :: redirectResolver :: contextNarrowFilter :: Nil)

    val occSource : Traversable[DBpediaResourceOccurrence] = AllOccurrenceSource.fromXMLDumpFile(new File(wikiDumpFileName), Language(languageCode))
    //val filter = new OccurrenceFilter(redirectsTC = redirectsTCMap, conceptURIs = conceptUrisSet, contextExtractor = narrowContext)
    //val occs = filter.filter(occSource)

    val occs = filters.foldLeft(occSource){ (o,f) => f.filterOccs(o) }

    FileOccurrenceSource.writeToFile(occs, new File(targetFileName))

    SpotlightLog.info(this.getClass, "Occurrences saved to: %s", targetFileName)
  }
}