# Databricks notebook source
# Sample customer data with nulls and duplicates
data = """customer_id,first_name,last_name,email,phone
1,John,Doe,john.doe@example.com,1234567890
2,Jane,Smith,null,9876543210
3,Bob,null,bob.smith@example.com,
4,Alice,Johnson,alice.johnson@example.com,5551234567
2,Jane,Smith,null,9876543210
5,null,Williams,null,4445556666
1,John,Doe,john.doe@example.com,1234567890
null,null,null,null,null
"""

# Write to a volume location
dbutils.fs.put("/Volumes/first_catalog/default/second_volumn/data.csv", data, overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE first_catalog.default.customers (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     email STRING
# MAGIC );

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
data3 = [
    (1, 'John', 'john@example.com', '1234567890'),
    (2, 'Jane', 'jane@example.com', '9876543210'),
    (3, 'Alice', 'alice@example.com', '5551112222'),
    (4, 'Bob', 'bob@example.com', '4445556666'),
    (5, 'Charlie', 'charlie@example.com', '3332221111'),
    (6, 'David', 'david@example.com', '7778889999'),
    (7, 'Eva', 'eva@example.com', '6665554444'),
    (8, 'Frank', 'frank@example.com', '2223334444'),
    (9, 'Grace', 'grace@example.com', '1112223333'),
    (10, 'Henry', 'henry@example.com', '9998887777')
]

schema=StructType([StructField('id',IntegerType(),True),StructField('name',StringType(),True),StructField('email',StringType(),True),StructField('phone',StringType(),True)])
df_n=spark.createDataFrame(data3,schema)
df_n.show()


# COMMAND ----------

df_n.write.format('delta').mode("append").option('mergeSchema','true').saveAsTable('first_catalog.default.customers')

# COMMAND ----------

data3 = [
    (1, 'John', 'john@example.com'),
    (2, 'Jane', 'jane@example.com'),
    (3, 'Alice', 'alice@example.com'),
    (4, 'Bob', 'bob@example.com'),
    (5, 'Charlie', 'charlie@example.com'),
    (6, 'David', 'david@example.com'),
    (7, 'Eva', 'eva@example.com'),
    (8, 'Frank', 'frank@example.com'),
    (9, 'Grace', 'grace@example.com'),
    (10, 'Henry', 'henry@example.com')
]
df_new=spark.createDataFrame(data1,['id','name','email'])
df_new.write.format("delta").save('/Volumes/first_catalog/default/second_volumn/data1.csv')

# COMMAND ----------

df_insert.unionByName(df_n,allowMissingColumns=True).show()

# COMMAND ----------

df_insert=spark.read.format('delta').load('/Volumes/first_catalog/default/second_volumn/data1.csv/')
df_insert.display()

# COMMAND ----------

from pyspark.sql.functions import col
df_final=df_insert.withColumn("id",col("id").cast('int'))
df_final.display()

# COMMAND ----------

from pyspark.sql.functions import col

df_insert = df_insert.withColumn("id", col("id").cast("int"))

# COMMAND ----------

spark.table("first_catalog.default.customers").printSchema()
df_insert.printSchema()

# COMMAND ----------

df_insert.unionByName(df_n,allowMissingColumns=True)
df_insert.show()

# COMMAND ----------

df_insert.write.mode('append').option("mergeSchema","true").saveAsTable('first_catalog.default.customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from first_catalog.default.customers;

# COMMAND ----------

df_insert.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO first_catalog.default.customers
# MAGIC VALUES (1, 'John', 'john@example.com'),
# MAGIC        (2, 'Jane', 'jane@example.com');

# COMMAND ----------

from pyspark.sql.functions import * # all functions will be imported into current session
from pyspark.sql.types import * # all data types will be imported into current session
# how we will define pyspark schema definition for creating dataframe to avoid inferSchema.
# StructType is array data type to define multiple columns with data types information in an array 
# StructField is also a data type to define individual column information like column_name,data_type,nullability
schema1 = StructType([StructField('customer_id', IntegerType(), True), 
                      StructField('first_name', StringType(), True), 
                      StructField('last_name', StringType(), True), 
                      StructField('email', StringType(), True), 
                      StructField('phone', LongType(), True)])



# COMMAND ----------

# what are major spark features?
# lazy evaluation (data will be processed only when we use actions(save data or display data))
# in-memory processing -- data will be processed in RAM (RAM is faster than disk)
# distributed -- data will be splitted and processed on multiple slave/worker nodes(distributed processing)
# parallel processing (each node will have multiple VCPU will run parallely)

# Note
# Whenever we use inferSchema=True option then it is going to read all data from files to understand data types to create data types.
# so if we specify header=true it will read one row for columns
# if we specify inferSchema=True it will read all data from files to understand data types to create data types

# COMMAND ----------

df_customer = spark.read.csv("/Volumes/workspace/default/sample_data/customer_data.csv",header=True,nullValue="null",inferSchema=True)
df_customer.schema

# COMMAND ----------

df_customer = spark.read.csv("/Volumes/workspace/default/sample_data/customer_data.csv",header=True,nullValue="null",schema=schema1)

# COMMAND ----------

df_customer.schema # it will return dataframe metadata (pyspark Schema definition)

# COMMAND ----------

# df.dropna() for removing null rows 
# df.fillna() for filling null values with actual values 
# df.dropDuplicates(['customer_id'])  for removing duplicates based on customer id 
# here are some alias transformations u can use any one. (both are same)
# df.filter()/df.where()
# df.groupby()/df.groupBy()
# df.dropDuplicates()/df.drop_duplicates()
# df.fillna() /df.na.fill()
# df.dropna() / df.na.drop()
# df.dropna() defaults if any one column is having null it will remove that row
df1 = df_customer.dropDuplicates(['customer_id']).dropna(how='all').fillna({'first_name':'NA','last_name':'NA','email':'No-Email','phone':0}).withColumn("PART_ID",spark_partition_id())
df1.display()

# COMMAND ----------

# 1G/128MB = 8 partitions --- repartition(16) -- 64mb 
#Var=no_partitions (pass this value at runtime)
parts=5
df1.repartition(parts).display()

# COMMAND ----------

more than 4 transformations.
# answer from google --- rdd transformations -- map,flatmap, ()

# COMMAND ----------

# Wide transformations -- it will do shuffle(two stage operation)
1) groupBy
2) orderBy
3) repartition
4) joins 
5) substract/intersect
6) distinct
7) dropDuplciates

# Narrow Transformatioins  -- No shuffle (single stage)
1) select 
2) filter/where 
3) withColumn
4) withColumnRenamed
5) drop
6) selectExpr
7) coalesce 
8) fillna
9) dropna 
10) limit
11) createOrReplaceTempView


# COMMAND ----------

# if we want to increase no of partitions then we can use repartition() method
# if we want to decrease no of partitions then we can use coalesce() 

# COMMAND ----------

df2 = df1.repartition(4,"first_name") # hash partition
df3 = df2.coalesce(2)
# hash partition and round robin
# if we are not using column name that will be round robin/even distribution
df2.explain(mode="extended")

# COMMAND ----------

df2.display()

# COMMAND ----------

df_customer = spark.read.parquet("/Volumes/first_catalog/default/first_volumn")
df_customer.schema

# COMMAND ----------

df_customer.display()
