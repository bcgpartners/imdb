# Top Movie Ratings from IMDB
Files are downloaded and processed from IMDB site https://datasets.imdbws.com/
Scores are calculated using *(numVotes/averageNumberOfVotes) * averageRating*

## How to run this application
To execute this application please follow these steps

1. Download gz files from  https://datasets.imdbws.com/
2. Download the executeable JAR file from repository location *test/out/artifacts/movieRatings.jar*
3. Use spark-submit utility to execute the application

```
spark-submit --class "topMoviesByRatings" --master local[*] movieRatings.jar DOWNLOAD_DIRECTORY_PATH

```
#### Note
The last argument is the path where you downloaded the imdb files
