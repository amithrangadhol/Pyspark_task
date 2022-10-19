from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("demo").getOrCreate()

schema = StructType().add("context",StructType()
                          .add("sid",StringType())
                          .add("did",StringType())
                          .add("cdata",ArrayType(StructType()
                                                 .add("type",StringType())
                                                 .add("id",StringType())))
                          .add("env",StringType())
                          .add("channel",StringType())
                          .add("rollup",StructType()
                               .add("l1",StringType()))
                          .add("pdata",StructType()
                               .add("ver",StringType())
                               .add("pid",StringType())
                               .add("id",StringType())))\
    .add("edata",StructType()
         .add("pass",StringType())
         .add("duration",FloatType())
         .add("index",FloatType())
         .add("item",StructType()
              .add("type",StringType())
              .add("uri",StringType())
              .add("desc",StringType())
              .add("params",ArrayType(StructType()
                                      .add("1",StringType())
                                      .add("2",StringType())
                                      .add("3",StringType())
                                      .add("eval",StringType())))
              .add("id",StringType())
              .add("exlength",FloatType())
              .add("title",StringType())
              .add("mmc",ArrayType(StringType()))
              .add("maxscore",FloatType())
              .add("mc",ArrayType(StringType())))
         .add("score",FloatType())
         .add("resvalues",ArrayType(StructType()
                                    .add("1",StringType())
                                    .add("2",StringType())
                                    .add("3",StringType()))))\
    .add("actor",StructType()
         .add("type",StringType())
         .add("id",StringType()))\
    .add("ver",StringType())\
    .add("object",StructType()
         .add("type",StringType())
         .add("ver",StringType())
         .add("rollup",StructType())
         .add("id",StringType()))\
    .add("tags",ArrayType(StringType()))\
    .add("mids",StringType())\
    .add("eid",StringType())\
    .add("syncts",IntegerType())\
    .add("@timestamp",StringType())\
    .add("flags",StructType()
         .add("ex_processed",BooleanType()))

df = spark.read.json("/home/amith/Program/Pyspark/json/schema_file.json",multiLine=True,schema=schema)
#df.printSchema()

df = df.withColumn("exploded",f.explode(col("context.cdata")))
#df = df.withColumn("exploded",f.explode(df["edata.item.params"]))
df = df.select("context.sid","context.did","exploded.type","exploded.id","context.pdata.id",
               "edata.pass","edata.duration","edata.item.uri","edata.score","actor.type","ver","object.id","tags",
               "@timestamp","flags.ex_processed")
# df = df.select("context.sid","context.did","exploded.type","exploded.id","context.pdata.ver","context.pdata.id",
#                "edata.pass","edata.duration")
#df = df.select("edata.pass")
df.show(truncate=False)


