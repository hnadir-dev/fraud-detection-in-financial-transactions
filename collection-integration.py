from pyhive import hive
import requests
from datetime import datetime
import threading

def getUrl(subUrl):
    return f'http://127.0.0.1:5000/api{subUrl}'

customers_url = getUrl('/customers')
transactions_url = getUrl('/transactions')
external_data_url = getUrl('/external-data')

hive_host = "localhost"
hive_port = 10000

connection = hive.Connection(host=hive_host, port=hive_port, username="root")

cursor = connection.cursor()

#--DROP DATABASE IF EXISTS financedb cascade;
query = '''
CREATE DATABASE IF NOT EXISTS financedb;

CREATE TABLE if not exists financedb.transactions (
    transaction_id STRING,
    date_time TIMESTAMP,
    amount DOUBLE,
    currency STRING,
    merchant_details STRING,
    customer_id STRING,
    transaction_type STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/financedb.db/transactions';

CREATE TABLE IF NOT EXISTS financedb.customers (
    customer_id STRING,
    account_history ARRAY<STRING>,
    demographics STRUCT<age: INT, location: STRING>,
    behavioral_patterns DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/financedb.db/customers';

CREATE TABLE IF NOT EXISTS financedb.blacklist_info (
    merchant_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/financedb.db/blacklist_info';

CREATE TABLE IF NOT EXISTS financedb.credit_scores (
    customer_id STRING,
    score INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/financedb.db/credit_scores';

CREATE TABLE IF NOT EXISTS financedb.fraud_reports (
    customer_id STRING,
    report_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/financedb.db/fraud_reports'
'''


def createDatabaseAndTables():
    for q in query.split(';'):
        cursor.execute(q)

transactions_res = requests.get(transactions_url)
customers_res = requests.get(customers_url)
external_data_res = requests.get(external_data_url)

def threadInsertTransactions():
    if transactions_res.status_code == 200:
        tran_data = transactions_res.json()
        for t in tran_data:
            datetime_object = datetime.strptime(t['date_time'], "%Y-%m-%dT%H:%M:%S.%f")
            cursor.execute('INSERT INTO financedb.transactions values("{}","{}",{},"{}","{}","{}","{}","{}")'.format(t['transaction_id'],datetime_object,t['amount'],t['currency'],t['merchant_details'],t['customer_id'],t['transaction_type'],t['location']))

def threadInsertCustomers():
    if customers_res.status_code == 200:
        cust_data = customers_res.json()
        for c in cust_data:
            cursor.execute("INSERT INTO financedb.customers(customer_id, account_history, demographics, behavioral_patterns) select '{}',ARRAY{},named_struct{},{}".format(c['customer_id'],tuple(c['account_history']),('age',c['demographics']['age'],'location',c['demographics']['location']),c['behavioral_patterns']['avg_transaction_value']))

def threadInsertblacklistInfo():
    if external_data_res.status_code == 200:
        ext_data = external_data_res.json()['blacklist_info']
        for e in ext_data:
            cursor.execute("INSERT INTO financedb.blacklist_info select '{}'".format(e))

def threadInsertCreditScores():
    if external_data_res.status_code == 200:
        ext_data = external_data_res.json()['credit_scores']
        for _id, score in ext_data.items():
            cursor.execute("INSERT INTO financedb.credit_scores select '{}',{}".format(_id,score))

def threadInsertFraudReports():
    if external_data_res.status_code == 200:
        ext_data = external_data_res.json()['fraud_reports']
        for _id, _count in ext_data.items():
            cursor.execute("INSERT INTO financedb.fraud_reports select '{}',{}".format(_id,_count))

if __name__ == "__main__":

    createDatabaseAndTables()
    threadInsertTransactions()
    threadInsertCustomers()
    threadInsertblacklistInfo()
    threadInsertCreditScores()
    threadInsertFraudReports()