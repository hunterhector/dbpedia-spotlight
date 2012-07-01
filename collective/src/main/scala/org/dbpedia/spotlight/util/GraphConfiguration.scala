package org.dbpedia.spotlight.util

import java.io.{FileOutputStream, FileInputStream, File}
import org.apache.commons.logging.LogFactory
import java.util.Properties
import org.dbpedia.spotlight.exceptions.ConfigurationException

/**
 * Created with IntelliJ IDEA.
 * User: hector
 * Date: 6/28/12
 * Time: 5:36 PM
 */

class GraphConfiguration(val configFile:File) {

  private val LOG = LogFactory.getLog(this.getClass)

  def this(fileName: String) {
    this(new File(fileName))
  }

  private val properties : Properties = new Properties()

  LOG.info("Loading configuration file "+configFile)
  properties.load(new FileInputStream(configFile))

  def save(configFile : File) {
    properties.store(new FileOutputStream(configFile),"")
    LOG.info("Saved configuration file"+configFile)
  }

  def save() {
    save(configFile)
  }

  def getOrElse(key: String, defaultValue : String) : String = {
    properties.getProperty(key,defaultValue)
  }

  def get(key : String) : String = {
    val value = getOrElse(key, null)
    if (value == null) {
       throw new ConfigurationException(key + "not specified in "+configFile)
    }
    value
  }

  def setNodeNumber(n:Int) {
     properties.setProperty("org.dbpedia.spotlight.graph.nodeNumber",n.toString)
     save()
  }

  def getNodeNumber:Int = {
     val r= get("org.dbpedia.spotlight.graph.nodeNumber").toInt
     if ( r < 0)
       throw new ConfigurationException("Nodenumber is not set correctly")

     r
  }
}