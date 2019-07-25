#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
# ms-python.python added
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'Learning/Spark Fundamentals I/Module 1'))
	print(os.getcwd())
except:
	pass
#%% [markdown]
# <a href="https://cognitiveclass.ai"><img src = "https://ibm.box.com/shared/static/9gegpsmnsoo25ikkbl4qzlvlyjbgxs5x.png" width = 400> </a>
#%% [markdown]
# <h1 align = "center"> Spark Fundamentals I - Introduction to Spark</h1>
# <h2 align = "center"> Getting Started</h2>
# <br align = "left">
# 
# **Related free online courses:**
# 
# Related courses can be found in the following learning paths:
# 
# - [Spark Fundamentals path](http://cocl.us/Spark_Fundamentals_Path)
# - [Big Data Fundamentals path](http://cocl.us/Big_Data_Fundamentals_Path) 
# 
# <img src = "http://spark.apache.org/images/spark-logo.png", height = 100, align = 'left'>
#%% [markdown]
#  ## Spark is built around speed and the ease of use. In these labs you will see for yourself how easy it is to get started using Spark. 
# 
# Spark’s primary abstraction is a distributed collection of items called a Resilient Distributed Dataset or RDD. In a subsequent lab exercise, you will learn more about the details of RDD. RDDs have actions, which return values, and transformations, which return pointers to new RDD.
# 
# This set of labs uses Cognitive Class Labs (formerly known as BDU Labs) to provide an interactive environment to develop applications and analyze data. It is available in either Scala or Python shells. Scala runs on the Java VM and is thus a good way to use existing Java libraries. In this lab exercise, we will set up our environment in preparation for the later labs.
# 
# After completing this set of hands-on labs, you should be able to:
# 
# 1. Perform basic RDD actions and transformations
# 2. Use caching to speed up repeated operations
# 
# 
# ### Using this notebook
# 
# This is an interactive environment where you can show your code through cells, and documentation through markdown.
# 
# Look at the top right corner. Do you see "Python 3"? This indicates that you are running Python in this notebook.
# 
# **To run a cell:** Shift + Enter
# 
# ### Try creating a new cell below.
# 
# **To create a new cell:** In the menu, go to _"Insert" > "Insert Cell Below"_. Or, click outside of a cell, and press "a" (insert cell above) or "b" (insert cell below).
#%% [markdown]
# # Lab Setup
# 
# Run the following cells to get the lab data.

#%%
# download the data from the IBM server
# this may take ~30 seconds depending on your internet speed
get_ipython().system('wget --quiet https://ibm.box.com/shared/static/j8skrriqeqw66f51iyz911zyqai64j2g.zip')
print("Data Downloaded!")

#%% [markdown]
# Let's unzip the data that we just downloaded into a directory dedicated for this course. Let's choose the directory **/resources/jupyter/labs/BD0211EN/**.

#%%
# this may take ~30 seconds depending on your internet speed
get_ipython().system('unzip -q -o -d /resources/jupyterlab/labs/BD0211EN/ j8skrriqeqw66f51iyz911zyqai64j2g.zip')
print("Data Extracted!")

#%% [markdown]
# The data is in a folder called **LabData**. Let's list all the files in the data that we just downloaded and extracted.

#%%
# list the extracted files
get_ipython().system('ls -1 /resources/jupyterlab/labs/BD0211EN/LabData')

#%% [markdown]
# Should have:
#     
# * followers.txt
# * notebook.log
# * nyctaxi100.csv
# * nyctaxi.csv
# * nyctaxisub.csv
# * nycweather.csv
# * pom.xml
# * README.md
# * taxistreams.py
# * users.txt
#%% [markdown]
# ### Starting with Spark
# 
# The notebooks provide code assist. For example, type in "sc." followed by the Tab key to get the list of options associated with the spark context:

#%%
sc.

#%% [markdown]
# To run a command as code, simple select the cell you want to run and either:
# 
# * Click the play button in the toolbar above
# * Press "_Shift+Enter_"
# 
# Let's run a basic command and check the version of Spark running:

#%%
sc.version

#%% [markdown]
# Add in the path to the *README.md* file in **LabData**.

#%%
readme = sc.textFile("/resources/jupyterlab/labs/BD0211EN/LabData/README.md")

#%% [markdown]
# Let’s perform some RDD actions on this text file. Count the number of items in the RDD using this command:

#%%
readme.count()

#%% [markdown]
# You should see that this RDD action returned a value of 103.
# 
# Let’s run another action. Run this command to find the first item in the RDD:

#%%
readme.first()

#%% [markdown]
# Now let’s try a transformation. Use the filter transformation to return a new RDD with a subset of the items in the file. Type in this command:

#%%
linesWithSpark = readme.filter(lambda line: "Spark" in line)

#%% [markdown]
# You can even chain together transformations and actions. To find out how many lines contains the word “Spark”, type in:

#%%
linesWithSpark = readme.filter(lambda line: "Spark" in line)
readme.filter(lambda line: "Spark" in line).count()

#%% [markdown]
# # More on RDD Operations
# 
# This section builds upon the previous section. In this section, you will see that RDD can be used for more complex computations. You will find the line from that "README.md" file with the most words in it.
# 
# Run the following cell.

#%%
readme.map(lambda line: len(line.split())).reduce(lambda a, b: a if (a > b) else b)

#%% [markdown]
# There are two parts to this. The first maps a line to an integer value, the number of words in that line. In the second part reduce is called to find the line with the most words in it. The arguments to map and reduce are Python anonymous functions (lambdas), but you can use any top level Python functions. In the next step, you’ll define a max function to illustrate this feature.
# 
# Define the max function. You will need to type this in:

#%%
def max(a, b):
 if a > b:
    return a
 else:
    return b

#%% [markdown]
# Now run the following with the max function:

#%%
readme.map(lambda line: len(line.split())).reduce(max)

#%% [markdown]
# Spark has a MapReduce data flow pattern. We can use this to do a word count on the readme file.

#%%
wordCounts = readme.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

#%% [markdown]
# Here we combined the flatMap, map, and the reduceByKey functions to do a word count of each word in the readme file.
# 
# To collect the word counts, use the _collect_ action.
# 
# #### It should be noted that the collect function brings all of the data into the driver node. For a small dataset, this is acceptable but, for a large dataset this can cause an Out Of Memory error. It is recommended to use collect() for testing only. The safer approach is to use the take() function e.g. print take(n)

#%%
wordCounts.collect()

#%% [markdown]
# ### <span style="color: red">YOUR TURN:</span> 
# 
# #### In the cell below, determine what is the most frequent word in the README, and how many times was it used?

#%%
# WRITE YOUR CODE BELOW

#%% [markdown]
# Highlight text field for answer:
# 
# <input type="text" size="80" value="wordCounts.reduce(lambda a, b: a if (a[1] > b[1]) else b)" style="color: white">
#%% [markdown]
# ## Using Spark caching
# 
# In this short section, you’ll see how Spark caching can be used to pull data sets into a cluster-wide in-memory cache. This is very useful for accessing repeated data, such as querying a small “hot” dataset or when running an iterative algorithm. Both Python and Scala use the same commands.
# 
# As a simple example, let’s mark our linesWithSpark dataset to be cached and then invoke the first count operation to tell Spark to cache it. Remember that transformation operations such as cache does not get processed until some action like count() is called. Once you run the second count() operation, you should notice a small increase in speed.
# 

#%%
print(linesWithSpark.count())


#%%
from timeit import Timer
def count():
    return linesWithSpark.count()
t = Timer(lambda: count())


#%%
print(t.timeit(number=50))


#%%
linesWithSpark.cache()
print(t.timeit(number=50))

#%% [markdown]
# It may seem silly to cache such a small file, but for larger data sets across tens or hundreds of nodes, this would still work. The second linesWithSpark.count() action runs against the cache and would perform significantly better for large datasets.
#%% [markdown]
# <div class="alert alert-success alertsuccess" style="margin-top: 20px">
# **Tip**: Enjoyed using Jupyter notebooks with Spark? Get yourself a free 
#     <a href="http://cocl.us/DSX_on_Cloud">IBM Cloud</a> account where you can use Data Science Experience notebooks
#     and have *two* Spark executors for free!
# </div>
#%% [markdown]
# ### Summary
# Having completed this exercise, you should now be able to log in to your environment and use the Spark shell to run simple actions and transformations for Scala and/or Python. You understand that Spark caching can be used to cache large datasets and subsequent operations on it will utilize the data in the cache rather than re-fetching it from HDFS.
#%% [markdown]
# This notebook is part of the free course on **Cognitive Class** called *Spark Fundamentals I*. If you accessed this notebook outside the course, you can take this free self-paced course, online by going to: http://cocl.us/Spark_Fundamentals_I
#%% [markdown]
# ### About the Authors:  
# Hi! It's [Alex Aklson](https://www.linkedin.com/in/aklson/), one of the authors of this notebook. I hope you found this lab educational! There is much more to learn about Spark but you are well on your way. Feel free to connect with me if you have any questions.
# <hr>

