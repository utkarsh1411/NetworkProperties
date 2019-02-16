import sys
import time
import networkx as nx
import pandas
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy
from pyspark.sql.types import *

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	# YOUR CODE HERE
	originalOutput = []
	connComponents = g.connectedComponents().select('component').distinct().count()
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	
	if usegraphframe:
		# Get vertex list for serial iteration
		# YOUR CODE HERE
		verticeL = g.vertices.rdd.map(lambda row: row['id']).collect()
		
		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		# YOUR CODE HERE
		for ver in verticeL:
			gf = GraphFrame(g.verticeL.filter('id != "'+ ver +'"'), g.edges)
			NconnComponents = gf.connectedComponents().select('component').distinct().count()
			out = (ver, 1 if NconnComponents > connComponents else 0)
			originalOutput.append(out)
		
		
		df = sqlContext.createDataFrame(sc.parallelize(originalOutput), ['id','articulation'])
		return df
		
		
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
        # YOUR CODE HERE
		G = nx.Graph()
		G.add_edges_from(g.edges.map(lambda edge: (edge.src, edge.dst)).collect())
		verticeL = g.vertices.rdd.map(lambda row: row['id']).collect()
		G.add_nodes_from(verticeL)
		
		for ver in verticeL:
			NG = deepcopy(G)
			NG.remove_node(ver)
			NconnComponentsN = nx.number_connected_components(NG)
			out = (ver, 1 if NconnComponentsN > connComponents else 0)
			originalOutput.append(out)
		
		
		df2 = sqlContext.createDataFrame(sc.parallelize(originalOutput), ['id','articulation'])
		return df2
		

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
print("Writing distribution to file .csv")
df.filter("articulation = 1").toPandas().to_csv("Articulations.csv")

#Runtime for below is more than 2 hours
#print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
#init = time.time()
#df = articulations(g, True)
#print("Execution time: %s seconds" % (time.time() - init))
#print("Articulation points:")
#df.filter('articulation = 1').show(truncate=False)
