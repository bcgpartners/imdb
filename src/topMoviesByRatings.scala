
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object topMoviesByRatings {
  def main(args: Array[String]): Unit = {

    //Data Location
    val dataDir = args(0)

    val sc = new SparkContext(new SparkConf())

    val spark = SparkSession
      .builder
      .appName("TopMoviesByRatings")
      .getOrCreate()

    //Define schema
    import org.apache.spark.sql.types._

    val schematitleRatings = StructType(
      List(
        StructField("tconst", StringType, false),
        StructField("averageRating", DoubleType, false),
        StructField("numVotes", LongType, false)
      )
    )

    val schematitleBasics = StructType(
      List(
        StructField("tconst", StringType, false),
        StructField("titleType", StringType, false),
        StructField("primaryTitle", StringType, false),
        StructField("originalTitle", StringType, false),
        StructField("isAdult", StringType, false),
        StructField("startYear", StringType, false),
        StructField("endYear", StringType, false),
        StructField("runtimeMinutes", LongType, false),
        StructField("genres", StringType, false)
      )
    )

    val schematitlePrincipal = StructType(
      List(
        StructField("tconst", StringType, false),
        StructField("ordering", IntegerType, false),
        StructField("nconst", StringType, false),
        StructField("category", StringType, false),
        StructField("job", StringType, false),
        StructField("characters", StringType, false)
      )
    )

    val schemanameBasics = StructType(
      List(
        StructField("nconst", StringType, false),
        StructField("primaryName", StringType, false),
        StructField("birthYear", StringType, false),
        StructField("deathYear", StringType, false),
        StructField("primaryProfession", StringType, false),
        StructField("knownForTitles", StringType, false)
      )
    )

    //Repartitioning gz files to increase parallelism

    val titleRatings = spark.read.format("csv"
    ).option("sep", "\t"
    ).option("header", "true"
    ).option("inferSchema", "true"
    ).schema(schematitleRatings
    ).load(dataDir + "/title.ratings.tsv.gz"
    ).repartition(sc.defaultParallelism * 3
    ).where("numVotes >=50")

    val titleBasics = spark.read.format("csv"
    ).option("sep", "\t"
    ).option("header", "true"
    ).option("inferSchema", "true"
    ).schema(schematitleBasics
    ).load(dataDir + "/title.basics.tsv.gz"
    ).repartition(sc.defaultParallelism * 3
    ).where("titleType='movie'")

    val titlePrincipal = spark.read.format("csv"
    ).option("sep", "\t"
    ).option("header", "true"
    ).option("inferSchema", "true"
    ).schema(schematitlePrincipal
    ).load(dataDir + "/title.principals.tsv.gz"
    ).repartition(sc.defaultParallelism * 3)

    val nameBasics = spark.read.format("csv"
    ).option("sep", "\t"
    ).option("header", "true"
    ).option("inferSchema", "true"
    ).schema(schemanameBasics
    ).load(dataDir + "/name.basics.tsv.gz"
    ).repartition(sc.defaultParallelism * 3)

    //Persisting this dataframe as it will be used more than once
    val moviesWithFiftyOrMoreRatings = titleRatings.join(titleBasics, titleRatings.col("tconst") === titleBasics.col("tconst")
    ).drop(titleBasics("tconst")).persist

    //Calculating literal value i.e.average number of votes
    val averageNumberOfVotes = moviesWithFiftyOrMoreRatings.select(avg("numVotes")
    ).collect()(0).getDouble(0)

    /*Final Output - Retrieve top 20 movies with a minimum of 50 votes with the ranking determined by:
  (numVotes/averageNumberOfVotes) * averageRating*/

    moviesWithFiftyOrMoreRatings.withColumn("score", col("numVotes") / lit(averageNumberOfVotes) * col("averageRating")
    ).orderBy(desc("score")).limit(20
    ).join(titlePrincipal, moviesWithFiftyOrMoreRatings.col("tconst") === titlePrincipal.col("tconst")
    ).join(nameBasics, titlePrincipal.col("nconst") === nameBasics.col("nconst")
    ).drop(titlePrincipal.col("tconst")
    ).orderBy(desc("score"), asc("tconst")
    ).selectExpr("primaryTitle", "originalTitle", "primaryName as mostOftenCredited").collect.foreach(println)

    spark.stop()

  }
}