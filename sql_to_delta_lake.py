from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_delta")\
        .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.33')\
        .getOrCreate()
sqlContext = SparkSession(spark)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")


#Load CSV file into DataFrame
mysqldf = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://192.168.1.000:3306/dataeng") \
    .option("dbtable", "genericfood") \
    .option("user", "root1") \
    .option("password", "root") \
    .load()

#Checking dataframe schema
mysqldf.printSchema()

# Create HIVE temp view from data frame

mysqldf.createOrReplaceTempView("tempFood")

# Load filtered data into Delta format (create initial table)

sqlContext.sql("SELECT * FROM tempFood").show(n=20)

sqlContext.sql("SELECT groupname,count(*) FROM tempFood GROUP BY 1 ORDER BY 2 DESC").show(truncate=False)


newdf = sqlContext.sql("SELECT * FROM tempFood WHERE groupname = 'Herbs and Spices'")

newdf.write.format("delta").save("onprem_warehouse",mode='append')

# Read Delta tables using Spark data frame

#Load delta file into DataFrame
deltadf = spark.read.format("delta").load("onprem_warehouse")

deltadf.printSchema()

# Create Temp HIVE of delta tables

deltadf.createOrReplaceTempView("tempDelta")

#Write query on read data from delta

sqlContext.sql("SELECT * FROM tempDelta").show()

sqlContext.sql("SELECT count(*) FROM tempDelta").show()

sqlContext.sql("SELECT count(*) FROM tempDelta where groupname = 'Herbs and Spices' ").show()

sqlContext.sql("SELECT groupname,count(*) FROM tempDelta where groupname = Fruits GROUP BY SUBGROUP ORDER BY 2 DESC ").show()









