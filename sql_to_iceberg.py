# First Load all the required library and also Start Spark Session
# Load all the required library
from pyspark.sql import SparkSession


#Start Spark Session
spark = SparkSession.builder.appName("iceberg")\
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.1.0,mysql:mysql-connector-java:8.0.32')\
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')\
        .config('spark.sql.catalog.spark_catalog.type', 'hive')\
        .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog')\
        .config('spark.sql.catalog.local.type','hadoop')\
        .config('spark.sql.catalog.local.warehouse','warehouse')\
        .config('spark.sql.defaultCatalog','local')\
        .getOrCreate()

sqlContext = SparkSession(spark)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")
# warehouse is the name of the data folder to create



# Read data from MySQL server into Spark

#Load CSV file into DataFrame
mysqldf = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://192.168.1.000:3306/DATAENG") \
    .option("dbtable", "genericfood") \
    .option("user", "root") \
    .option("password", "mysql") \
    .load()


#Checking dataframe schema
mysqldf.printSchema()


# Create HIVE temp view from data frame

mysqldf.createOrReplaceTempView("tempFood")


# Load filtered data into Iceberg format (create initial table)

sqlContext.sql("SELECT * FROM tempFood").show(n=20)

sqlContext.sql("SELECT groupname,count(*) FROM tempFood GROUP BY 1 ORDER BY 2 DESC").show(truncate=False)

newdf = sqlContext.sql("SELECT * FROM tempFood WHERE groupname = 'Herbs and Spices'")


 # Saving table to iceberg format

newdf.writeTo("iceberg_food").using("iceberg").create()


### Load another filtered data into Iceberg format into same table

newdf1 = sqlContext.sql("SELECT * FROM tempFood WHERE GROUP = 'Fruits'")


### Append loaded data into table

newdf1.write.format("iceberg").save("iceberg_food",mode='append')


### Read iceberg tables using Spark data frame

#Load delta file into DataFrame
icebergdf = spark.read.format("iceberg").load("iceberg_food")

icebergdf.show()

### Create Temp HIVE of iceberg tables


icebergdf.createOrReplaceTempView("tempIceberg")

### Query the read data from iceberg

sqlContext.sql("SELECT * FROM tempIceberg").show()

sqlContext.sql("SELECT count(*) FROM tempIceberg").show()

sqlContext.sql("SELECT DISTINCT(SUBGROUP) FROM tempIceberg").show()

sqlContext.sql("SELECT SUBGROUP,count(*) FROM tempIceberg GROUP BY SUBGROUP ORDER BY 2 DESC ").show()