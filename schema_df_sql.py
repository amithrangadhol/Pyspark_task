from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("demo").getOrCreate()

schema = StructType().add("_id",StructType()
                          .add("$oid",StringType()))\
    .add("bestsellers_date",StructType()
         .add("$date",StructType()
              .add("$numberLong",StringType()))).\
    add("published_date",StructType()
        .add("$date",StructType()
             .add("$numberLong",StringType()))).\
    add("amazon_product_url",StringType()).\
    add("author",StringType()).\
    add("description",StringType()).\
    add("price",StructType()
        .add("$numberDouble",StringType())).\
    add("rank_last_week",StructType()
        .add("$numberInt",StringType())).\
    add("weeks_on_list",StructType()
        .add("$numberInt",StringType()))

df = spark.read.json("/home/amith/Program/Pyspark/json/Dataframe_sql.json",multiLine=True)
df.printSchema()
#df = df.withColumn("")
df = df.select("_id.$oid","bestsellers_date.$date.$numberLong","published_date.$date.$numberLong",
               "amazon_product_url","author","description","price.$numberInt","rank_last_week.$numberInt","weeks_on_list.$numberInt")
#df = df.select("_id","bestsellers_date","published_date","amazon_product_url","author","description","price","rank_last_week","weeks_on_list")
#df.show(truncate=False)

df.write.option("header",True).format("csv").mode("overwrite").save("/home/amith/Program/Pyspark/json/df_sql")


















# schema = StructType().add("context",StructType()
#                           .add("sid",StringType())
#                           .add("did",StructType())
#                           .add("cdata",ArrayType(StructType()
#                                                  .add("type",StringType())
#                                                  .add("id",StringType()))))
# df = spark.read.json("/home/amith/Program/Pyspark/json/schema_sample_file.json",multiLine=True,schema=schema)
#
#
# df = df.withColumn("exploded",f.explode(df['context.cdata']))
# df = df.select("context.sid")
# df.show()
