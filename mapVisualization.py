import pandas as pd
import os
import statistics
import itertools as it
import networkx as nx
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap
import matplotlib.lines as mlines


def main():
    plt.figure(figsize=(15, 20))
    df = loadData()

    # transform the group by into a dic where key is id and the value is a list of business_id
    # in order to be able to draw edges between business linked by a common user
    dic = df.groupby('user_id')['business_id'].apply(lambda x: x.tolist()).to_dict()

    m = Basemap(projection='merc',
                llcrnrlon=-115.4,  # -130 to get all the US
                llcrnrlat=35.9,  # 22 to get all the US
                urcrnrlon=-114.9,  # -66 to get all the US
                urcrnrlat=36.3,  # 50 to get all the US
                lat_ts=0,
                resolution='l',
                suppress_ticks=True)

    mx, my = m(df['long'].values, df['lat'].values)

    pos = {}

    for count, elt in enumerate(df["business_id"]):
        pos[elt] = ((float(mx[count]), float(my[count])))

    G = nx.Graph()

    for key in dic.keys():
        if len(dic[key]) > 1 and len(dic[key]) < 3:
            edges_to_draw = list(it.combinations(dic[key], 2))  # getting all 2-uples combination of the businesses
            G.add_edges_from(edges_to_draw)

    print(len(G.edges()))

    node_size = [G.degree(elt) * 10 for elt in list(G.nodes())]
    print(node_size)
    node_color = aggregateColors(node_size)
    nx.draw_networkx_nodes(G=G, pos=pos, node_list=G.nodes(), node_color=node_color, alpha=0.8, node_size=node_size)
    nx.draw_networkx_edges(G=G, pos=pos, edge_color='b', alpha=0.2, arrows=False)

    m.drawcountries(linewidth=1.5)
    m.drawstates(linewidth=0.2)
    m.drawcoastlines(linewidth=1)

    plt.tight_layout()
    plt.savefig("map_1.png", format="png", dpi=300)
    plt.show()


def loadData():
    df_business = pd.read_json("../yelp_dataset/yelp_academic_dataset_business.json", lines=True)

    df_users_business = pd.DataFrame()
    # loading json file with key users and values business they have written a comment about
    folder = os.listdir("../yelp_dataset/elite_2017_and_business.json")

    for file in folder:
        df = pd.read_json("../yelp_dataset/elite_2017_and_business.json/" + file, lines=True)
        df_users_business = pd.concat([df_users_business, df])

    # filtering the business that have less than 100 elite visitors
    df_users_business = df_users_business.groupby("business_id").filter(lambda x: len(x) > 100)

    list_business = list(df_users_business.business_id.values)

    # retrieving the latitude and longitude of each business
    long, lat = [], []
    for business in list_business:
        longitude = df_business[df_business.business_id == business].longitude.values[0]
        latitude = df_business[df_business.business_id == business].latitude.values[0]

        long.append(longitude)
        lat.append(latitude)

    # df_users_business = df_users_business[:2000]
    df_users_business['long'] = pd.Series(long, index=df_users_business.index)
    df_users_business['lat'] = pd.Series(lat, index=df_users_business.index)

    return df_users_business


def aggregateColors(size_list):
    mini = min(size_list)
    maxi = max(size_list)
    med = statistics.median(size_list)

    color_list = []
    for elt in size_list:
        if elt <= med:
            x = int(255*(elt - mini)/(med+1 - mini))
            color_list.append((1, x/255, 0))
        else:
            x = int(255*(elt - med)/(maxi - med))
            color_list.append(((255 - x)/255, 1, 0))

    return color_list


if __name__ == "__main__":
    main()
