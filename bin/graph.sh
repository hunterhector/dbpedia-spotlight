# Running these commands will help you create all the graphs you need for experiments.
# Edit the parameters to fit into your setting if necessary

# However, it is also possible to download some of them online and directly run the disambiguator only

GRAPH_CONFIG_FILE= ../conf/graph.properties

# Allocate enough memory for generating graph
export JAVA_OPTS="-Xmx6G"
export MAVEN_OPTS="-Xmx6G"
export SCALA_OPTS="-Xmx6G"

# you have to run maven2 from the module that contains the indexing classes
cd ../collective
# graph will be saved in the directory below. Save it to a place you like
mkdir graph

# 1. Generate the basic graphs (occs and co-occs graph) from indexing result
mvn scala:run -DmainClass=org.dbpedia.spotlight.graph.GraphMaker -DaddArgs=../conf/graph.properties

# 2. Merge the graph
mvn scala:run -DmainClass=org.dbpedia.spotlight.graph.MWMergedGraph -DaddArgs=../conf/graph.properties