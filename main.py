from neo4j import GraphDatabase


class HelloWorldExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting)
            # print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx):
        result = tx.run("MATCH (n) RETURN n")
        for node in result:
            print(node)
        return result.single()


if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "pass123")
    greeter.print_greeting()
    greeter.close()
