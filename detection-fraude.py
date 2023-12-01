from pyhive import hive

hive_host = "localhost"
hive_port = 10000

connection = hive.Connection(host=hive_host, port=hive_port, username="root")

cursor = connection.cursor()

# requêtes pour détecter des montants de transaction anormalement élevés
query_1 = '''
SELECT
    t.transaction_id,
    t.date_time,
    t.amount,
    t.currency,
    t.merchant_details,
    t.customer_id,
    t.transaction_type,
    t.location
FROM
    financedb.transactions t
WHERE
    t.amount > 2321 * 2 --(SELECT AVG(amount) * 2 FROM financedb.transactions)
'''

# les fréquence élevée de transactions dans un court laps de temps
query_2 = '''
SELECT
    customer_id,
    COUNT(transaction_id) AS transaction_count,
    MIN(date_time) AS start_time,
    MAX(date_time) AS end_time
FROM (
    SELECT
        customer_id,
        transaction_id,
        date_time,
        LAG(date_time) OVER (PARTITION BY customer_id ORDER BY date_time) AS prev_time
    FROM
        financedb.transactions
) t
WHERE
    DATEDIFF(date_time, prev_time) <= 60
GROUP BY
    customer_id, date_time;
'''

cursor.execute(query_1) # '(select AVG(t2.amount) as avr_amount from financedb.transactions t2)'

for item in cursor.fetchall():
    print(item)

    
    