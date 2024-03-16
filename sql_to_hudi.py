# First Load all the required library and also Start Spark Session
# Load all the required library
from pyspark.sql import SparkSession


#Start Spark Session
spark = SparkSession.builder.appName("chapter8")\
        .config('spark.jars.packages', 'org.apache.hudi:hudi-spark3.2-bundle_2.12:0.13.0,mysql:mysql-connector-java:8.0.32')\
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')\
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')\
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
        .config('spark.sql.catalog.local.warehouse','warehouse')\
        .getOrCreate()

sqlContext = SparkSession(spark)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")


#Load CSV file into DataFrame
mysqldf = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://192.168.1.155:3306/dataeng") \
    .option("dbtable", "genericfood") \
    .option("user", "root1") \
    .option("password", "root") \
    .load()

#Checking dataframe schema
mysqldf.printSchema()


### Create HIVE temp view from data frame

mysqldf.createOrReplaceTempView("tempFood")


### Load filtered data into HUDI format (create initial table)

sqlContext.sql("SELECT * FROM tempFood").show(n=5)



sqlContext.sql("SELECT GROUP,count(*) FROM tempFood GROUP BY 1 ORDER BY 2 DESC").show(truncate=False)

newdf = sqlContext.sql("SELECT *,now() as ts FROM tempFood WHERE groupname = 'Herbs and Spices'") #We added now() to get  timestamp required for a hudi table

# table name and cdc column name
hudi_options = {'hoodie.table.name': 'hudi_food','hoodie.datasource.write.recordkey.field': 'foodname'}

# hudi_options_more = {
#     'hoodie.table.name': 'hudi_food',
#     'hoodie.datasource.write.recordkey.field': 'foodname',
#     'hoodie.datasource.write.partitionpath.field': 'groupname',
#     'hoodie.datasource.write.table.name': 'hudi_food',
#     'hoodie.datasource.write.operation': 'upsert',
#     'hoodie.datasource.write.precombine.field': 'ts',
#     'hoodie.upsurt.shuffle.parallelism': 2,
#     'hoodie.insert.shuffle.parallelism': 2
# }



# Path to save table
basePath = "/opt/spark/ETL/chapter8/hudi_food"

newdf.write.format("hudi").   \
    options(**hudi_options).\
    mode("append").\
    save(basePath)



#### Load filtered data again into HUDI format into same table

newdf1 = sqlContext.sql("SELECT *,now() as ts FROM tempFood WHERE groupname = 'Fruits'")

basePath = "/opt/spark/ETL/chapter8/hudi_food"

newdf1.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(basePath)


#### Read HUDI tables using Spark data frame

#Load delta file into DataFrame
hudidf = spark.read.format("hudi").load(basePath)

hudidf.printSchema()

#### Create Temp HIVE of HUDI tables

hudidf.createOrReplaceTempView("tempHUDI")


### Read from created temporal hive table
sqlContext.sql("SELECT * FROM tempHUDI").show()


sqlContext.sql("SELECT count(*) FROM tempHUDI").show()


sqlContext.sql("SELECT DISTINCT(subgroupname) FROM tempHUDI").show()

sqlContext.sql("SELECT subgroupname,count(*) FROM tempHUDI GROUP BY 1 ORDER BY 2 DESC ").show()

