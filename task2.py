# Calculate the average rating of each genre
# In order to run this, we use spark-submit, below is the
# spark-submit  \
#   --master local[2] \
    #   AverageRatingPerGenre.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
from task2_utils import *
from datetime import datetime
import argparse


if __name__ == "__main__":
    sc = SparkContext(appName="Controversial Trending Videos Identification")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='')
    parser.add_argument("--output", help="the output path",
                        default='task2_output')
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    video_data = sc.textFile(input_path + "AllVideos_short.csv")

    video_infos = video_data.map(extractVideoInfo)
    result = video_infos.aggregateByKey((datetime.strptime('9999-09-09' , '%Y-%m-%d'), datetime.strptime('9999-09-09' , '%Y-%m-%d'), 0, 0, 0, 0, "Unknown"), mergeInfo, mergeInfoCombiner, 1 ).map(mapResult)
    result.saveAsTextFile(output_path)


    # movieRatings = ratings.map(extractRating)
    # movieGenre = movieData.flatMap(pairMovieToGenre) # we use flatMap as there are multiple genre per movie
    # genreRatings = movieGenre.join(movieRatings).values()
    # genreRatingsAverage = genreRatings.aggregateByKey((0.0,0), mergeRating, mergeCombiners, 1).map(mapAverageRating)
    # genreRatingsAverage.saveAsTextFile(output_path)
