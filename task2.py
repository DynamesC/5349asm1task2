# Calculate the average rating of each genre
# In order to run this, we use spark-submit, below is the
# spark-submit  \
#   --master local[2] \
    #   task2.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
from datetime import datetime
import argparse

"""
extract useful info from the original csv file, and out put key-value pairs
with video_id|country as key and date|likes|dislikes|category as value
"""
def extractVideoInfo(record):
    parts = record.strip().split(",")

    """Check that the line is of the correct format
    If line is malformed, we ignore the line and continue to the next line
    And if it is the first line of csv file we also ignore it
    """
    if len(parts) != 12 or parts[0] == "video_id":
        return("video_id|country", None)

    video_id = parts[0]
    country = parts[11]
    date = parts[1]
    likes = parts[6]
    dislikes = parts[7]
    category = parts[3]

    return ("{}|{}".format(video_id, country), "{}|{}|{}|{}".format(date, likes, dislikes, category))

"""
for each video in each country, Maintain a tuple of 7 elements with format
(Datetime, Datetime, int, int, int, int, string) meaning
(earlist date seen, second earlist date seen, likes for earlist date, dislikes for earlist date,
likes for second earlist date, dislikes for second earlist date, category of first seen date)
"""

def mergeInfo(accumulatedInfo, currentInfo):
    date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category = accumulatedInfo

    #if that information is from the first line of csv
    if(currentInfo is None):
        return accumulatedInfo

    currentInfoParts = currentInfo.split("|")

    if len(currentInfoParts) != 4:
        return accumulatedInfo

    #create datetime object
    currentInfoDate = datetime.strptime("20"+currentInfoParts[0] , '%Y.%d.%m')
    currentInfoCategory = currentInfoParts[3]

    #try convert string to int, and extrtact the like number and dislike number
    try:
        currentInfoLikes = int(currentInfoParts[1])
        currentInfoDisLikes = int(currentInfoParts[2])
    except:
        return accumulatedInfo

    #if this date is earlier than recorded earlist date
    #the recorded earlist date and corresponding indormation is moved to the second earlist
    if currentInfoDate < date1:
        date2 = date1
        date1 = currentInfoDate
        date2_like = date1_like
        date2_dislike = date1_dislike
        date1_like = currentInfoLikes
        date1_dislike = currentInfoDisLikes
        category = currentInfoCategory

    #if this date is earlier than the recorded second earlist one but later than the recorded earlist one
    if date1 < currentInfoDate < date2:
        date2 = currentInfoDate
        date2_like = currentInfoLikes
        date2_dislike = currentInfoDisLikes

    return (date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category)

#merge two accumulatedInfo by returning the earlist 2 dates and information among 4
def mergeInfoCombiner(accumulatedInfo1, accumulatedInfo2):
    date11, date12, date11_like, date11_dislike, date12_like, date12_dislike, category1 = accumulatedInfo1
    date21, date22, date21_like, date21_dislike, date22_like, date22_dislike, category2 = accumulatedInfo2

    pair1 = (date11, date11_like, date11_dislike, category1)
    pair2 = (date12, date12_like, date12_dislike, category1)
    pair3 = (date21, date21_like, date21_dislike, category2)
    pair4 = (date22, date22_like, date22_dislike, category2)

    pairList = [pair1, pair2, pair3, pair4]
    pairList.sort(key=lambda tup: tup[0])

    smallestPair1 = pairList[0]
    smallestPair2 = pairList[1]

    return(smallestPair1[0], smallestPair2[0], smallestPair1[1], smallestPair1[2], smallestPair2[1], smallestPair2[2], smallestPair1[3])

#retrieve the result
def mapResult(line):
    key, accumulatedInfo = line
    date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category = accumulatedInfo

    #get video id and country from key
    video_id = key.split("|")[0]
    country = key.split("|")[1]

    #calculate the difference between the growing of dislike and like
    value = (date2_dislike - date1_dislike) - (date2_like - date1_like)

    #handling the case where the video_id and country pair is only seen once
    if (date2_like == 0 and date2_dislike == 0):
        value = 0

    #handle the first line
    if(video_id == "video_id" ):
        return (video_id, 0, "category", "country")

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
    video_data = sc.textFile(input_path)
    video_infos = video_data.map(extractVideoInfo)
    result = video_infos.aggregateByKey((datetime.strptime('9999.09.09' , '%Y.%d.%m'), datetime.strptime('9999.09.09' , '%Y.%d.%m'), 0, 0, 0, 0, "Unknown"), mergeInfo, mergeInfoCombiner, 1 ).map(mapResult)
    final = sc.parallelize(result.sortBy(lambda r: r[1], False).take(10))
    final.saveAsTextFile(output_path)
    # test_record = "SbOwzAl9ZfQ,17.14.11,24,Entertainment,2017-11-13T06:06:22.000Z,310130,4182,361,1836,False,False,MX\n"
    # test_extracted_record = extractVideoInfo(test_record)
    # print(test_extracted_record)
    # test_tuple = (datetime.strptime('9999.09.09' , '%Y.%d.%m'), datetime.strptime('9999.09.09' , '%Y.%d.%m'), 0, 0, 0, 0, "Unknown")
    # tuple1 = mergeInfo(test_tuple, test_extracted_record[1])
    # print(tuple1)
    # tuple2 = mergeInfo(tuple1, "17.14.12|1|1|Hi")
    # print(tuple2)
    # tuple3 = mergeInfo(tuple2, "17.14.10|2|2|HO")
    # print(tuple3)
    # test_combine_tuple = (datetime.strptime('2017.09.11' , '%Y.%d.%m'), datetime.strptime('2017.10.11' , '%Y.%d.%m'), 4, 3, 9, 6, "Unknown")
    # test_combined_tuple1 = mergeInfoCombiner(tuple3, test_combine_tuple)
    # print(test_combined_tuple1)
    # print(mapResult(("asdf|USA", test_combined_tuple1)))



    # movieRatings = ratings.map(extractRating)
    # movieGenre = movieData.flatMap(pairMovieToGenre) # we use flatMap as there are multiple genre per movie
    # genreRatings = movieGenre.join(movieRatings).values()
    # genreRatingsAverage = genreRatings.aggregateByKey((0.0,0), mergeRating, mergeCombiners, 1).map(mapAverageRating)
    # genreRatingsAverage.saveAsTextFile(output_path)
