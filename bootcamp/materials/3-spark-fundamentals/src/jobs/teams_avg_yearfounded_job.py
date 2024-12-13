from pyspark.sql import SparkSession





def do_teams_avg_yearfounded_transformation(spark, dataframe):
    query = """
    SELECT avg(yearfounded) as mean_yearfounded
    FROM bootcamp.teams"""
    dataframe.createOrReplaceTempView("teams")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("teams_avg_yearfounded") \
      .getOrCreate()
    output_df = do_teams_avg_yearfounded_transformation(spark, spark.table("teams"))
    output_df.write.mode("overwrite").insertInto("teams_avg_yearfounded")