// PySpark Testing Homework
// - Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL

//  The first converted query answer the following question:
//   What is the mean of yearfounded?

// PostgreSQL query
spark.sql("""SELECT avg(yearfounded) as mean_yearfounded
FROM bootcamp.teams""").show()

// SparkSQL query
teams.agg(avg("yearfounded").as("mean_yearfounded")).show()


//  The second converted query answer the following question:
//    
//  Deduplicate `game_details` so there's no duplicates

// PostgreSQL query
spark.sql("""SELECT DISTINCT * from game_details;""").show()

// SparkSQL query
game_details
.distinct()
.show()



// - Create new PySpark jobs in `src/jobs` for these queries
// Done, please look at the 2 new files in the folder


// - Create tests in `src/tests` folder with fake input and expected output data
// TODO