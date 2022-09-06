import csv
from flask import Flask, request
from flask import render_template
from neo4j import GraphDatabase


app = Flask(__name__)
results = []
cent_results = []


@app.route('/', methods=['GET', 'POST'])
def homepage():
    if request.method == 'POST':
        borough = request.form['boroughs']
        order = 'match (n) where n.locality="' + borough + '"return distinct n.name, n.type,n.address, n.locality, ' \
                                                           'n.neighborhood, n.latitude, n.longitude '
        db_connector = Neo4j("bolt://localhost:7687", "neo4j", "pass123")
        db_connector.print_results(order)
        db_connector.close()
        return render_template("page2.html", results=results)
    else:
        return render_template('welcome.html')


@app.route('/page2', methods=['GET', 'POST'])
def page2():
    order1 = 'CALL gds.graph.project("myGraph","Store","Distance",{relationshipProperties:"name"})'
    order2 = 'CALL gds.pageRank.stream("myGraph") YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, ' \
             'score ORDER BY score DESC, name ASC '
    db_connector = Neo4j("bolt://localhost:7687", "neo4j", "pass123")
    # db_connector.centrality(order1)
    db_connector.centrality(order2)
    db_connector.close()
    return render_template('centrality.html', cent_results=cent_results)


class Neo4j:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print("Connection Established!")

    def close(self):
        self.driver.close()
        print("Disconnected!")

    def print_results(self, order):
        with self.driver.session() as session:
            session.write_transaction(self.db_results, order)

    def centrality(self, order):
        with self.driver.session() as session:
            session.write_transaction(self.centrality_results, order)

    @staticmethod
    def db_results(tx, order):
        result = tx.run(order)
        results.clear()
        with open('result.csv', 'w'):
            pass
        for node in result:
            data = (node.values())
            results.append(data)
            with open('result.csv', 'a') as file:
                writer = csv.writer(file)
                writer.writerow(data)

    @staticmethod
    def centrality_results(tx, order):
        cent_result = tx.run(order)
        cent_results.clear()
        for line in cent_result:
            cent_results.append(line)


if __name__ == "__main__":
    app.run(debug=True)
