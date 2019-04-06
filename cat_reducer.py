#!/usr/bin/python3

import sys



def read_map_output(file):
    """ Return an iterator for key, value pair extracted from file (sys.stdin).
    Input format:  key \t value
    Output format: (key, value)
    """
    for line in file:
        temp_list = line.strip().split("\t", 1)
        yield temp_list[0], temp_list[1]


def cat_reducer():

    #current processing catrgory
    current_cat = ""
    #maintain a dictionary with video_id as key and a list of its trending countries as value
    current_id_country_dict = {}
    file = sys.stdin

    #message is a string with format "video_id|country"
    for category, message in read_map_output(file):
        message_parts = message.split("|",1)
        video_id = message_parts[0]
        country = message_parts[1]

        #if a category's processing is finished, calculate the result and print it
        if (current_cat != category) and current_cat != "":
            #a list of integers, each integer represents a the number of trending countries for a video
            vals = []

            for id in current_id_country_dict:
                # get the trending country number from the dictionary
                num_countries = len(current_id_country_dict[id])
                vals.append(num_countries)

            #calculate the result
            average = sum(vals)*1.0/len(vals)
            print("{}: {:.2f}".format(current_cat, average))

        # if a ner category is going to be processed, no matter it is the first one or not
        # the current_cat and dictionary are re-initialized
        if (current_cat != category):
            current_cat = category
            current_id_country_dict = {}

        # if this video_id is previously seen in this category
        if video_id in current_id_country_dict:
            counrty_list = current_id_country_dict[video_id]

            # if the country is not seen before, add it to the corresponding list
            if country not in counrty_list:
                counrty_list.append(country)

        # if it is the first time a video_id is seen in this category
        if video_id not in current_id_country_dict:
            current_id_country_dict[video_id] = [country]

    vals = []

    for id in current_id_country_dict:
        num_countries = len(current_id_country_dict[id])
        vals.append(num_countries)

    average = sum(vals)*1.0/len(vals)
    print("{}: {:.2f}".format(current_cat, average))


if __name__ == "__main__":
    cat_reducer()
