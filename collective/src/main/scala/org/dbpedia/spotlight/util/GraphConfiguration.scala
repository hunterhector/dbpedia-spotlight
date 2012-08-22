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


/**
 * Class deal with the configuration, also do validation of configuration to fail fast
 * @param configFile  The "properties" file about graph configs
 */
class GraphConfiguration(val configFile:File) {

  private val LOG = LogFactory.getLog(this.getClass)

  def this(fileName: String) {
    this(new File(fileName))
  }

  private val properties : Properties = new Properties()

  LOG.info("Loading configuration file "+configFile)
  properties.load(new FileInputStream(configFile))

  /**
   * Validate the configuration file.
   * if "org.dbpedia.spotlight.graph.validation" is set to value "index", then it will validate to see
   * whether all requirements are fulfilled for indexing. Otherwise it will validate to see whether
   * necessary files are included for running disambiguation.
   */
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

  /**
   * Get a parameter by key, if not found then default value
   * @param key
   * @param defaultValue
   * @return
   */
  def getOrElse(key: String, defaultValue : String) : String = {
    properties.getProperty(key,defaultValue)
  }

  /**
   * Get a property value given a key
   * @param key
   * @return the property value as String
   */
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

  //better not use this, it will rewrite the properties file so comments got deleted
/*  def setNodeNumber(n:Int) {
     properties.setProperty("org.dbpedia.spotlight.graph.nodeNumber",n.toString)
     save()
  }*/

  //please note that node numbers should be typed in.
  def getNodeNumber:Int = {
     val r= get("org.dbpedia.spotlight.graph.nodeNumber").toInt
     if ( r < 0)
       throw new ConfigurationException("Nodenumber is not set correctly")

     r
  }

  //validate index, only require occs.tsv and co-occs-count.tsv exists
  private def indexValidate  {
    LOG.info("Validating for indexing, to validate run, change 'org.dbpedia.spotlight.graph.validation' to 'run'.")

    val filesToCheck = List(
     get("org.dbpedia.spotlight.graph.occ.src") ,
     get("org.dbpedia.spotlight.graph.coocc.src")
    )
    filesToCheck.foreach( name =>{
      val test:File = new File(name)
      if (!test.exists()){
        throw new FileNotFoundException(String.format("A file pointed by the graph properties does not exist: %s",name))
      }
    }
    )
  }

  //validate running environment, need at least the core semantic file exists
  private def runValidate {
    LOG.info("Validating for run, to validate indexing, change 'org.dbpedia.spotlight.graph.validation' to 'index'.")

    val filesToCheck = List(
     get("org.dbpedia.spotlight.graph.dir") ,
     get("org.dbpedia.spotlight.graph.dir")+get("org.dbpedia.spotlight.graph.mapFile") ,
     get("org.dbpedia.spotlight.graph.dir") + get("org.dbpedia.spotlight.graph.semantic.dir")+get("org.dbpedia.spotlight.graph.semantic.basename")+".properties"

    )
    filesToCheck.foreach( name =>{
        val test:File = new File(name)
      if (!test.exists()){
        throw new FileNotFoundException(String.format("A file pointed by the graph properties does not exist: %s",name))
      }
    }
    )
  }
}