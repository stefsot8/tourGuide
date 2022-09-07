import csv
from flask import Flask, request
from flask import render_template
from neo4j import GraphDatabase

app = Flask(__name__)
results = []
cent_results = []
sim_results = []


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


@app.route('/centrality', methods=['GET', 'POST'])
def centralitypage():
    order1 = 'CALL gds.graph.project("myCentralityGraph","Store","Distance",{relationshipProperties:"name"})'
    order2 = 'CALL gds.pageRank.stream("myCentralityGraph") YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name,' \
             'gds.util.asNode(nodeId).type as type,gds.util.asNode(nodeId).address as address,gds.util.asNode(' \
             'nodeId).neighborhood as neighborhood,score ORDER BY score DESC, name ASC limit 10'
    db_connector = Neo4j("bolt://localhost:7687", "neo4j", "pass123")
    try:
        db_connector.centrality(order1)
    except:
        pass
    db_connector.centrality(order2)
    db_connector.close()
    return render_template('centrality.html', cent_results=cent_results)


@app.route('/similarity', methods=['GET', 'POST'])
def similaritypage():
    order1 = 'CALL gds.graph.project("mySimilarityGraph","Store",{Distance: {type: "Distance",properties: {strength:{' \
             'property:"name",defaultValue: 1.0}}}}); '
    order2 = 'CALL gds.nodeSimilarity.stream("mySimilarityGraph") YIELD node1, node2, similarity RETURN ' \
             'gds.util.asNode(node1).name AS Store1,gds.util.asNode(node1).address AS Address1, gds.util.asNode(' \
             'node2).name AS Store2,gds.util.asNode(node2).address AS Address2,gds.util.asNode(node1).type AS Type,' \
             'similarity ORDER BY similarity DESCENDING, Store1, Store2 limit 60 '
    db_connector = Neo4j("bolt://localhost:7687", "neo4j", "pass123")
    try:
        db_connector.similarity(order1)
    except:
        pass
    db_connector.similarity(order2)
    db_connector.close()
    return render_template('similarity.html', sim_results=sim_results)


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

    def similarity(self, order):
        with self.driver.session() as session:
            session.write_transaction(self.similarity_results, order)

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

    @staticmethod
    def similarity_results(tx, order):
        sim_result = tx.run(order)
        sim_results.clear()
        i = 1
        for line in sim_result:
            if i % 2 == 0:  # remove same duplicate results
                sim_results.append(line)
            i += 1


if __name__ == "__main__":
    app.run(debug=True)
