import pandas as pd
import os
import numpy as np
import json


def CommentsGroupedByCategories():
    """The idea is to create a json file where key will be the category and values all the comments belonging to
    that category of business as a text"""
    list_categories = ['Restaurants', 'Nightlife', "Bars", "Breakfast & Brunch", "Sandwiches", "Pizza", "Italian"]

    # ------------------------------- creating the business and comments dataframes ---------------------------------
    file_comments, file_business = os.listdir("../yelp_dataset/open_business_reviews.json"), os.listdir(
        "../yelp_dataset/open_business.json")

    df_comments = pd.DataFrame()
    df_business = pd.DataFrame()

    for file in file_comments:
        df = pd.read_json("../yelp_dataset/open_business_reviews.json/" + file, lines=True)
        df_comments = pd.concat([df_comments, df])

    for file in file_business:
        df = pd.read_json("../yelp_dataset/open_business.json/" + file, lines=True)
        df_business = pd.concat([df_business, df])

    df_business['categories'] = df_business['categories'].astype(str)
    df_business['categories'] = df_business['categories'].map(lambda a: a.split(","))
    df_business['categories'] = df_business['categories'].apply(striper)

    for category in list_categories:
        df_business_filtered = filterDfByListElement([category], df_business, 'categories')

        df_comments_filtered = pd.merge(df_comments, df_business_filtered, on="business_id", how="inner")
        with open("comments_{}.json".format(category), 'w') as file:
            text = " ".join(list(df_comments_filtered.text.values))
            json.dump(text, file, indent=4)


def filterDfByListElement(filteringElt, df, column):
    """Function taking a list a list of elements, a dataframe and a column
    dropping rows if the specified column does not contain any element of the filtering list"""
    indexToDrop = []
    for i in range(len(df)):
        try:
            categories = df.loc[i][column]
            if filteringElt not in list(categories):
                indexToDrop.append(i)
        except KeyError:
            pass

    df = df.drop(indexToDrop)
    df = df.reset_index(drop=True)
    return df


def striper(list):
    """function striping each element of a list"""
    return [elt.lstrip() for elt in list]


if __name__ == "__main__":
    CommentsGroupedByCategories()
