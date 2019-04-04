import csv

def extractVideoInfo(record):
    parts = record.strip().split(",")

    if len(record) != 12 or parts[0] == "video_id":
        continue

    video_id = parts[0]
    country = parts[11]
    date = parts[1]
    likes = parts[6]
    dislikes = parts[7]
    category = parts[3]

    return ("{}|{}".format(video_id, country), "{}|{}|{}|{}".format(date, likes, dislikes, category))

def mergeInfo(accumulatedInfo, currentInfo):
    date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category = accumulatedInfo

    currentInfoParts = currentInfo.split("|")

    if len(currentInfoParts != 4):
        return accumulatedInfo

    currentInfoDate = datetime.strptime(currentInfoParts[0] , '%Y-%m-%d')
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
    pairList.sort(key=lambda tup: tup[0])

    smallestPair1 = pairList[0]
    smallestPair2 = pairList[1]

    return(smallestPair1[0], smallestPair2[0], smallestPair1[1], smallestPair1[2], smallestPair2[1], smallestPair2[2], smallestPair1[3])

def mapResult(line):
    key, value = line
    date1, date2, date1_like, date1_dislike, date2_like, date2_dislike, category = accumulatedInfo

    video_id = key.split("|")[0]
    country = key.split("|")[1]

    value = (date2_dislike - date1_dislike) - (date2_like - date1_like)

    return (video_id, "{}|{}|{}".format(value, category, country))

# """
# This module includes a few functions used in computing average rating per genre
# """
# def pairMovieToGenre(record):
#     """This function converts entries of movies.csv into key,value pair of the following format
#     (movieID, genre)
#     since there may be multiple genre per movie, this function returns a list of tuples
#     Args:
#         record (str): A row of CSV file, with three columns separated by comma
#     Returns:
#         The return value is a list of tuples, each tuple contains (movieID, genre)
#     """
#     for row in csv.reader([record]):
#         if len(row) != 3:
#             continue
#         movieID, genreList = row[0],row[2]
#         return [(movieID, genre) for genre in genreList.split("|")]
#
# def extractRating(record):
#     """ This function converts entries of ratings.csv into key,value pair of the following format
#     (movieID, rating)
#     Args:
#         record (str): A row of CSV file, with four columns separated by comma
#     Returns:
#         The return value is a tuple (movieID, genre)
#     """
#     try:
#         userID, movieID, rating, timestamp = record.split(",")
#         rating = float(rating)
#         return (movieID, rating)
#     except:
#         return ()
#
# def mapToPair(line):
#     """ This function converts tuples of (genre, rating) into key,value pair of the following format
#     (genre,rating)
#
#     Args:
#         line (str): A touple of  (genre, rating)
#     Returns:
#         The return value is a tuple  (genre, rating)
#     """
#     genre, rating = line
#     return (genre, rating)
#
#
# def mergeRating(accumulatedPair, currentRating):
#     """This funtion update a current  summary (ratingTotal, ratingCount) with a new rating value.
#
#     Args:
#         accumulatedPair (tuple): a tuple of (ratingTotal, ratingCount)
#         currentRating (float):a new rating value,
#     Returns:
#         The return value is an updated tuple of (ratingTotal, ratingCount)
#
#     """
#     ratingTotal, ratingCount = accumulatedPair
#     ratingTotal += currentRating
#     ratingCount += 1
#     return (ratingTotal, ratingCount)
#
#
# def mergeCombiners(accumulatedPair1, accumulatedPair2):
#     """This function merges two intermedate summaries of the format (ratingTotal, ratingCount)
#
#     Args:
#         accumulatedPair1 (tuple): a tuple of (ratingTotal, ratingCount)
#         accumulatedPair2 (fuple): a tuple of (ratingTotal, ratingCount)
#     Returns:
#         The return value is an updated tuple of (ratingTotal, ratingCount)
#     """
#     ratingTotal1, ratingCount1 = accumulatedPair1
#     ratingTotal2, ratingCount2 = accumulatedPair2
#     return (ratingTotal1+ratingTotal2, ratingCount1+ratingCount2)
#
#
# def mapAverageRating(line):
#     """This function compute the average with a given sum and count for a genre
#     Args:
#         line (tuple): a tuple of (genre, (ratingTotal,ratingCount))
#     Returns:
#         The return value is a tuple of (genre, average_rating)
#     """
#
#     genre, ratingTotalCount = line
#     ratingAverage = ratingTotalCount[0]/ratingTotalCount[1]
#     return (genre, ratingAverage)
