import pandas as pd
import requests
from neo4j import GraphDatabase


class Neo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting)

    @staticmethod
    def _create_and_return_greeting(tx):
        result = tx.run("CREATE (a:Store)"
                        "SET a.name = $store_name, a.id = $store_id, a.type = $store_type,a.latitude = "
                        "$store_latitude,a.longitude = $store_longitude,a.address = $store_address,a.locality = "
                        "$store_locality,a.neighborhood = $store_neighborhood", store_name=store_name,
                        store_id=store_id,
                        store_type=store_type, store_latitude=store_latitude, store_longitude=store_longitude,
                        store_address=store_address, store_locality=store_locality,
                        store_neighborhood=neighborhood_name)


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
neighborhoods = pd.read_csv('neighborhoods.csv')

i = 0

if __name__ == '__main__':
    for i in range(len(neighborhoods)):
        neighborhood_latitude = neighborhoods.loc[i, 'Latitude']  # neighborhood latitude value
        neighborhood_longitude = neighborhoods.loc[i, 'Longitude']  # neighborhood longitude value
        neighborhood_name = neighborhoods.loc[i, 'Neighborhood']  # neighborhood name
        print(neighborhood_name, neighborhood_longitude, neighborhood_latitude)
        url = 'https://api.foursquare.com/v3/places/search?ll={},{}&radius=1000&limit=50'.format(
            neighborhood_latitude,
            neighborhood_longitude)
        headers = {
            "Accept": "application/json",
            "Authorization": "fsq3lCc8A1YdWtMFr5dNnLj3jTcb1M56KTv9FRNfefY5mk0="
        }
        response = requests.request("GET", url, headers=headers)
        print(response.text)
        data = [response.json()]
        # with open(neighborhood_name + ".json", 'w') as json_file:
        #     json.dump(data, json_file)
        i += 1

        useful_data = data[0]['results']
        print(useful_data)
        p = 0
        for data in useful_data:
            if "name" in useful_data[p]:
                store_name = useful_data[p]['name']
            else:
                store_name = "missing"
            if "fsq_id" in useful_data[p]:
                store_id = useful_data[p]['fsq_id']
            else:
                store_id = "missing"
            try:
                if "name" in useful_data[p]['categories'][0]:
                    store_type = useful_data[p]['categories'][0]['name']
            except:
                store_type = "missing"
            if "main" in useful_data[p]['geocodes']:
                store_latitude = useful_data[p]['geocodes']['main']['latitude']
                store_longitude = useful_data[p]['geocodes']['main']['longitude']
            else:
                store_latitude = "missing"
                store_longitude = "missing"
            if "formatted_address" in useful_data[p]['location']:
                store_address = useful_data[p]['location']['formatted_address']
            else:
                store_address = "missing"
            if "locality" in useful_data[p]['location']:
                store_locality = useful_data[p]['location']['locality']
            else:
                store_locality = "missing"
            p += 1
            greeter = Neo4j("bolt://localhost:7687", "neo4j", "pass123")
            greeter.print_greeting()
            greeter.close()
