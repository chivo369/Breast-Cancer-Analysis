# -*- coding: utf-8 -*-
"""
Created on Sat Jul  7 23:16:56 2018

@author: tom
"""

#Spark UseCase Solution

from __future__ import print_function
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.readwriter import DataFrameWriter
from pyspark import SparkContext

if __name__ == "__main__":
    warehouse_location = abspath('spark-warehouse')
    
    spark = SparkSession.builder.appName("Cancer Analysis").\
    config("spark.sql.warehouse.dir", warehouse_location).\
    enableHiveSupport().getOrCreate()

    # Data Loading stage
    # In Spark-2.x.x, we can load a CSV file directly into the Spark SQL context as follows:
      
    df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/home/tom/bc/cancer.csv")


    # Here, we have used inferschema as an option so it will automatically infer the data type of the columns


    # Next, we will save the data in a table by registering it in a temp table as shown below.

    df.registerTempTable("cancer_analysis")

    # Selected columns in df is reanamed and type cast is applied 


    data = df.select(col("Complete TCGA ID").alias("id"),col("Gender").alias("gender"),\
                      col("Age at Initial Pathologic Diagnosis").alias("age").cast("int"),col("Tumor"),\
                      col("Node"),\
                      col("Vital Status").alias("status"))
    # The Data Loading part is completed
                      
                      
    # The insgihts from the data is shown below
    
    # Average age of each tumor stages
    data.groupBy("Tumor").avg("age").orderBy("Tumor").show()
    
    # Average age of each node stages
    data.groupBy("Node").avg("age").orderBy("Node").show()
    
    # Maximum age of each status category
    
    data.select("*").groupBy("status").max("age").show()
    
    # Minimum age of each status category
    
    data.select("*").groupBy("status").min("age").show()
    
    # Average age of each status category
    data.select("*").groupBy("status").avg("age").show()
    
    # Total number of status in each of the categories
    data.select("*").groupBy("status").count().show()
    
    data.select("*").groupBy("status","Node").max("age").show()
    data.select("*").groupBy("status","Node","Tumor").max("age").show()
    
    spark.stop()
