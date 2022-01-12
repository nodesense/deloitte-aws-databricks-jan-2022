# Databricks notebook source
# upload the file into data bricks community
# read text file in spark
# find would count
# print result, write to text file

# COMMAND ----------

# read text file, load data into partittions
fileRdd = sc.textFile('/FileStore/tables/books/hello_world.txt')
print("partitions", fileRdd.getNumPartitions())

# COMMAND ----------

print (fileRdd.glom().collect())

# COMMAND ----------

# cleansing data
# extra space leading, trailing white space, empty lines -  remove them.

# transformation
# trim white space
# \ continue the line, NO SPACE After \
nonEmptyLinesRdd = fileRdd.map (lambda line:  line.strip() )\
                          .filter (lambda line: line != '')

print (nonEmptyLinesRdd.collect())

# COMMAND ----------

# convert the line into array/list of words
wordsRdd = nonEmptyLinesRdd.map (lambda line: line.split(" "))
print (wordsRdd.collect())

# COMMAND ----------

# flatMap - transformation
# flatMap - convert list of list of elements into list of elements
flatwordsRdd = wordsRdd.flatMap(lambda arr: arr)
print (flatwordsRdd.collect())

# COMMAND ----------

# tuple is pair of elements
# python tuple () - immutable, once created, no changes
# python list [] - mutable, we can append, modify, remove elements
t1 = (10, ) # tuple with one element
t2 = ("GK", 80) # tuple with two elements
print (type(t1))
print (t1[0])
print(t2[1])
# fail
#t2[1] = 90 # fail

# COMMAND ----------

# spark => (spark, 1)
wordPairRdd = flatwordsRdd.map (lambda word: (word, 1) )
print(wordPairRdd.collect())

# COMMAND ----------

# pyspark, rdd has few methods, who can treat first element in the tuple as key
# reduceByKey [group the words(key), apply operations on top the collection]
# acc - accumulator
# assume, the input (hello, 1) ('world', 1), (hello, 1), (spark, 1), (hello, 1)
# Table 
# (hello, 1) - first time, it won't call reduceByKey, instead place the result in table
# ('world', 1) - first time, it won't call reduceByKey, instead place the result in table
# (hello, 1) - not first time, now it will call reduceByKey lambda, by taking acc as param 1, count 1 as count param 2 (lambda 1, 1) => 1 + 1 [2], the value/result 2 is updated in the reduceByKey result
# (spark, 1) - first time, it won't call reduceByKey, instead place the result in table
# (hello, 1) not first time, call reducebyKey call lambda acc(2), 1 => 2 + 1 [result], 3 will eb updated in results
"""
word      acc/results

hello      3
world      1
spark      1
"""
wordCountRdd = wordPairRdd.reduceByKey(lambda acc, count: acc + count)
print(wordCountRdd.collect())

# COMMAND ----------

# write the results to a file system
# saveAsTextFile is an action method
# results shall be written parallelly per partition
wordCountRdd.saveAsTextFile("/FileStore/tables/word-count-results-deloitte")
print ("partittions ", wordCountRdd.getNumPartitions())
print(wordCountRdd.glom().collect())

# COMMAND ----------

