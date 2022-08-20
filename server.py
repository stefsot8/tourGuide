import csv
from flask import Flask, request
from flask import render_template
from neo4j import GraphDatabase


app = Flask(__name__)
results = []


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
    return render_template('page2.html')


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

    @staticmethod
    def db_results(tx, order):
        result = tx.run(order)
        with open('result.csv', 'w'):
            pass
        for node in result:
            data = (node.values())
            results.append(data)
            with open('result.csv', 'a') as file:
                writer = csv.writer(file)
                writer.writerow(data)


if __name__ == "__main__":
    app.run(debug=True)
