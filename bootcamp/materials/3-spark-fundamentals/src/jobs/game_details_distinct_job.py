from pyspark.sql import SparkSession





def do_game_details_distinct_transformation(spark, dataframe):
    query = """
    SELECT DISTINCT * 
    from game_details;"""
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_details_distinct") \
      .getOrCreate()
    output_df = do_game_details_distinct_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("game_details_distinct")