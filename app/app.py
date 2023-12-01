from flask import Flask
import json
import random


app = Flask(__name__)


@app.route('/api/transactions', methods=['GET'])
def transactions():
    jsf = open('../data/transactions.json','r+')
    transactions = json.load(jsf)

    return transactions#[random.randrange(0, len(transactions) - 1)]

@app.route('/api/customers', methods=['GET'])
def customers():
    jsf = open('../data/customers.json','r+')
    customers = json.load(jsf)

    return customers #[random.randrange(0, len(customers) - 1)]

@app.route('/api/external-data', methods=['GET'])
def externalData():
    jsf = open('../data/external_data.json','r+')
    ex_data = json.load(jsf)

    return ex_data#[random.randrange(0, len(ex_data) - 1)]
