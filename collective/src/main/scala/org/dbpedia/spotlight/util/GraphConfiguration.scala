package org.dbpedia.spotlight.util

import java.io.{FileNotFoundException, FileOutputStream, FileInputStream, File}
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

  if (get("org.dbpedia.spotlight.graph.validation") == "index"){
    indexValidate
  } else{
    runValidate
  }

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
    var value = getOrElse(key, null)
    if (value == null) {
       throw new ConfigurationException(key + "not specified in "+configFile)
    }

    if (key.endsWith(".dir")) {
      if (!value.endsWith("/")){
        value += "/"
    }
    }

    value
  }

  //better not use this
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

  private def indexValidate:Boolean = {
    LOG.info("Validating for indexing, to validate run, change 'org.dbpedia.spotlight.graph.validation' to 'run'.")

    val filesToCheck = List(
     get("org.dbpedia.spotlight.graph.dir") ,
     get("org.dbpedia.spotlight.graph.occ.src") ,
     get("org.dbpedia.spotlight.graph.coocc.src")
    )
    filesToCheck.foreach( name =>{
      try{
        val test:File = new File(name)
      }catch{
        case fnfe : FileNotFoundException => LOG.error(String.format("A file pointed by the graph properties does not exist: %s",name))
        return false
      }
    }
    )
    true
  }

  private def runValidate:Boolean = {
    LOG.info("Validating for run, to validate indexing, change 'org.dbpedia.spotlight.graph.validation' to 'index'.")

    val filesToCheck = List(
     get("org.dbpedia.spotlight.graph.dir") ,
     get("org.dbpedia.spotlight.graph.dir")+get("org.dbpedia.spotlight.graph.mapFile") ,
     get("org.dbpedia.spotlight.graph.dir") + get("org.dbpedia.spotlight.graph.semantic.dir")+get("org.dbpedia.spotlight.graph.semantic.basename")+"properties"

    )
    filesToCheck.foreach( name =>{
      try{
        val test:File = new File(name)
      }catch{
        case fnfe : FileNotFoundException => LOG.error(String.format("A file pointed by the graph properties does not exist: %s",name))
        return false
      }
    }
    )
    true
  }



}