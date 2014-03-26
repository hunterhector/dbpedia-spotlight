package org.dbpedia.spotlight.io

import org.dbpedia.extraction.sources.WikiPage

object WikiPageUtil {
    def copyWikiPage(wikiPage: WikiPage, source: String) = {
// /*for 3.9*/    new WikiPage(wikiPage.title, wikiPage.redirect, wikiPage.id, wikiPage.revision, wikiPage.timestamp, wikiPage.contributorID,wikiPage.contributorName, source, wikiPage.format)
  /*for 3.8*/              new WikiPage(wikiPage.title, wikiPage.redirect, wikiPage.id, wikiPage.revision, wikiPage.timestamp, source)
    }
}

