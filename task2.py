# Calculate the average rating of each genre
# In order to run this, we use spark-submit, below is the
# spark-submit  \
#   --master local[2] \
    #   AverageRatingPerGenre.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
# from task2_utils import *
from datetime import datetime
import argparse

def extractVideoInfo(record):
    parts = record.strip().split(",")

    if len(parts) != 12 or parts[0] == "video_id":
        return("video_id|country", None)

    video_id = parts[0]
    country = parts[11]
    date = parts[1]
    likes = parts[6]
    dislikes = parts[7]
    category = parts[3]

    return ("{}|{}".format(video_id, country), "{}|{}|{}|{}".format(date, likes, dislikes, category))

def mergeInfo(accumulatedInfo, currentInfo):
    date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category = accumulatedInfo

    if(currentInfo is None):
        return accumulatedInfo

    currentInfoParts = currentInfo.split("|")

    if len(currentInfoParts) != 4:
        return accumulatedInfo

    currentInfoDate = datetime.strptime("20"+currentInfoParts[0] , '%Y.%d.%m')
    currentInfoCategory = currentInfoParts[3]

    try:
        currentInfoLikes = int(currentInfoParts[1])
        currentInfoDisLikes = int(currentInfoParts[2])
    except:
        return accumulatedInfo

    if currentInfoDate < date1:
        date2 = date1
        date1 = currentInfoDate
        date2_like = date1_like
        date2_dislike = date1_dislike
        date1_like = currentInfoLikes
        date1_dislike = currentInfoDisLikes
        category = currentInfoCategory

    if date1 < currentInfoDate < date2:
        date2 = currentInfoDate
        date2_like = currentInfoLikes
        date2_dislike = currentInfoDisLikes

    return (date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category)


def mergeInfoCombiner(accumulatedInfo1, accumulatedInfo2):
    date11, date12, date11_like, date11_dislike, date12_like, date12_dislike, category1 = accumulatedInfo1
    date21, date22, date21_like, date21_dislike, date22_like, date22_dislike, category2 = accumulatedInfo2

    pair1 = (date11, date11_like, date11_dislike, category1)
    pair2 = (date12, date12_like, date12_dislike, category1)
    pair3 = (date21, date21_like, date21_dislike, category2)
    pair4 = (date22, date22_like, date22_dislike, category2)

    pairList = [pair1, pair2, pair3, pair4]
    pairList = pairList.sort(key=lambda tup: tup[0])

    smallestPair1 = pairList[0]
    smallestPair2 = pairList[1]

    return(smallestPair1[0], smallestPair2[0], smallestPair1[1], smallestPair1[2], smallestPair2[1], smallestPair2[2], smallestPair1[3])

def mapResult(line):
    key, accumulatedInfo = line
    date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category = accumulatedInfo

    video_id = key.split("|")[0]
    country = key.split("|")[1]

    value = (date2_dislike - date1_dislike) - (date2_like - date1_like)

    if(video_id == "video_id"):
        return (video_id, 9999999, "category", "country")

    return (video_id, value, category, country)


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
    result = video_infos.reduceByKey((datetime.strptime('9999.09.09' , '%Y.%d.%m'), datetime.strptime('9999.09.09' , '%Y.%d.%m'), 0, 0, 0, 0, "Unknown"), mergeInfo,  1 ).map(mapResult)
    final = sc.parallelize(result.sortBy(lambda r: r[1], False).take(10))
    final.saveAsTextFile(output_path)
    final.foreach(print)



    # movieRatings = ratings.map(extractRating)
    # movieGenre = movieData.flatMap(pairMovieToGenre) # we use flatMap as there are multiple genre per movie
    # genreRatings = movieGenre.join(movieRatings).values()
    # genreRatingsAverage = genreRatings.aggregateByKey((0.0,0), mergeRating, mergeCombiners, 1).map(mapAverageRating)
    # genreRatingsAverage.saveAsTextFile(output_path)
