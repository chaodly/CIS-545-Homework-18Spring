
# coding: utf-8

# # Starting point for Spark on Google Cloud

# In[1]:


from emrspark import *

from pyspark.sql.types import *
import pyspark.sql.functions as F


conf.set("fs.s3n.awsAccessKeyId","AKIAJAH77LF4A32QBBRA")

conf.set("fs.s3n.awsSecretAccessKey","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")


spark = SparkSession.builder.config(conf=conf).appName('Graph HW3').getOrCreate()


# In[2]:


# TODO: read files, load graph_sdf, etc.
# Load data
a2q_sdf = spark.read.format("com.databricks.spark.csv").option("delimiter", ' ') .load("s3n://upenn-bigdataanalytics/data/sx-stackoverflow-a2q.txt")
c2q_sdf = spark.read.format("com.databricks.spark.csv").option("delimiter", ' ') .load("s3n://upenn-bigdataanalytics/data/sx-stackoverflow-c2q.txt")
c2a_sdf = spark.read.format("com.databricks.spark.csv").option("delimiter", ' ') .load("s3n://upenn-bigdataanalytics/data/sx-stackoverflow-c2a.txt")


# In[3]:


# Add as many cells as you like
graph_sdf = a2q_sdf.union(c2q_sdf)
graph_sdf = graph_sdf.union(c2a_sdf)

graph_sdf = graph_sdf.select(graph_sdf._c0.cast('int').alias('from_node'),                            graph_sdf._c1.cast('int').alias('to_node'))


# In[4]:


# Compute nodes_sdf
filteredF_sdf = graph_sdf[graph_sdf.from_node < 8].select(graph_sdf.from_node.alias('node')).distinct()
filteredT_sdf = graph_sdf[graph_sdf.to_node < 8].select(graph_sdf.to_node.alias('node')).distinct()
nodes_sdf = filteredF_sdf.union(filteredT_sdf).distinct().cache()


# In[5]:


def sdf_is_empty(sdf):
    try:
        sdf.take(1)
        return False
    except:
        return True


# In[6]:


def transitive_closure(graph_sdf, origins_sdf, depth):
    graph_sdf.cache()
    frontier_sdf = origins_sdf
    reachable_nodes_sdf = origins_sdf.cache()
    i = 0
    
    while ((not sdf_is_empty(frontier_sdf)) and (i < depth)):
        new_nodes_sdf = graph_sdf.join(frontier_sdf,graph_sdf.from_node == frontier_sdf.node).select('to_node').distinct()
        new_nodes_sdf = new_nodes_sdf.join(reachable_nodes_sdf, new_nodes_sdf.to_node == reachable_nodes_sdf.node,                                          'leftanti').withColumnRenamed('to_node', 'node')
        
        frontier_sdf = new_nodes_sdf.cache()
        reachable_nodes_sdf = reachable_nodes_sdf.union(new_nodes_sdf).cache()
        i += 1
        
    graph_sdf.unpersist()
    
    return reachable_nodes_sdf


# In[7]:


reachable_sdf = transitive_closure(graph_sdf, nodes_sdf, 3)


# ## Step 2.3 Results

# In[8]:


reachable_sdf.count()


# In[9]:


reachable_sdf.show()

