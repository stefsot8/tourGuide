import webbrowser

import numpy as np
from pandas.io.json import json_normalize
import pandas as pd
import json
import folium
from geopy.geocoders import Nominatim
import requests

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

with open('newyork_data.json') as json_data:
    newyork_data = json.load(json_data)

if __name__ == "__main__":
    neighborhoods_data = newyork_data['features']
    # define the dataframe columns
    column_names = ['Borough', 'Neighborhood', 'Latitude', 'Longitude']
    # instantiate the dataframe
    neighborhoods = pd.DataFrame(columns=column_names)
    for data in neighborhoods_data:
        borough = neighborhood_name = data['properties']['borough']
        neighborhood_name = data['properties']['name']

        neighborhood_latlon = data['geometry']['coordinates']
        neighborhood_lat = neighborhood_latlon[1]
        neighborhood_lon = neighborhood_latlon[0]

        neighborhoods = neighborhoods.append({'Borough': borough,
                                              'Neighborhood': neighborhood_name,
                                              'Latitude': neighborhood_lat,
                                              'Longitude': neighborhood_lon}, ignore_index=True)
    print(neighborhoods)
    neighborhoods.to_csv('neighborhoods.csv')
    print('The dataframe has {} boroughs and {} neighborhoods.'.format(
        len(neighborhoods['Borough'].unique()),
        neighborhoods.shape[0]
    )
    )
    # mapview
    address = 'New York City, NY'
    geolocator = Nominatim(user_agent="ny_explorer")
    location = geolocator.geocode(address)
    latitude = location.latitude
    longitude = location.longitude
    print('The geograpical coordinate of New York City are {}, {}.'.format(latitude, longitude))
    # create map of New York using latitude and longitude values
    map_newyork = folium.Map(location=[latitude, longitude], zoom_start=10)
    # add markers to map
    for lat, lng, borough, neighborhood in zip(neighborhoods['Latitude'], neighborhoods['Longitude'],
                                               neighborhoods['Borough'], neighborhoods['Neighborhood']):
        label = '{}, {}'.format(neighborhood, borough)
        label = folium.Popup(label, parse_html=True)
        folium.CircleMarker(
            [lat, lng],
            radius=5,
            popup=label,
            color='blue',
            fill=True,
            fill_color='#3186cc',
            fill_opacity=0.7,
            parse_html=False).add_to(map_newyork)

    output_file = "map.html"
    map_newyork.save(output_file)
    webbrowser.open(output_file, new=2)
