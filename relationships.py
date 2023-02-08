from neo4j import GraphDatabase
import haversine as hs

results = []


class Neo4j:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print("Connection Established!")

    def close(self):
        self.driver.close()
        print("Disconnected!")

    def get_results(self, order):
        with self.driver.session() as session:
            session.write_transaction(self.db_results, order)

    def add_relationships(self, order):
        with self.driver.session() as session:
            session.write_transaction(self.relations, order)

    @staticmethod
    def relations(tx, order):
        tx.run(order)
        print('ok')

    @staticmethod
    def db_results(tx, order):
        result = tx.run(order)
        for node in result:
            second = tx.run(order)
            id1 = node.get('n.id')
            lat1 = node.get('n.latitude')
            lon1 = node.get('n.longitude')
            loc1 = (lat1, lon1)
            for node2 in second:
                if node2 != node:
                    id2 = node2.get('n.id')
                    lat2 = node2.get('n.latitude')
                    lon2 = node2.get('n.longitude')
                    loc2 = (lat2, lon2)
                    kms = hs.haversine(loc1, loc2)
                    dist = round(kms, 2)
                    if dist <= 2.00:
                        order2 = 'match (a),(b) where a.id="'+id1+'"and b.id="'+id2+'"create (a)-[r:Distance {name:'+str(dist)+'}]->(b)'
                        db_connector.add_relationships(order2)
                else:
                    pass


if __name__ == "__main__":
    borough = 'Queens'   # insert manually borough name to split one huge process
    order = 'match (n) where n.borough="' + borough + '"return distinct n.id, n.latitude, n.longitude '
    db_connector = Neo4j("bolt://localhost:7687", "neo4j", "Ss132333")
    db_connector.get_results(order)
    db_connector.close()
    print(results)
