import json

from neo4j import GraphDatabase


class HelloWorldExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            session.execute_write(self._create_and_return_greeting, message)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run(message)
        for record in result:
            print(record.get("n").values())


if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "Ss132333")
    order = "MATCH (n) RETURN return distinct n.name, n.type,n.address, n.locality, n.neighborhood, n.latitude, n.longitude"
    greeter.print_greeting(order)
    greeter.close()
