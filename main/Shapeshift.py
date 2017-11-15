import MySQLdb
import requests

def create_table():
    db = MySQLdb.connect(host="localhost", user="root", passwd="admin", db="scraper")
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS shapeshift (symbol varchar(45) NOT NULL, fee double DEFAULT NULL, PRIMARY KEY (symbol))")
    db.close()

def update_shapeshift_fees():
    db = MySQLdb.connect(host="localhost", user="root", passwd="admin", db="scraper")
    cur = db.cursor()

    # Delete old values
    cur.execute("TRUNCATE TABLE shapeshift")

    # Request all request pairs with fees from Shapeshift
    shapeshift_data = requests.get("https://shapeshift.io/marketinfo/").json()

    # Write data to MySQL DB
    for exchange in shapeshift_data:
        currency = exchange["pair"].split('_')[1]
        cur.execute("SELECT 1 FROM shapeshift WHERE symbol = %s LIMIT 1", currency)
        data = cur.fetchone()
        if data is None:
            try:
                cur.execute("INSERT INTO shapeshift (symbol, fee) VALUES (%s, %s)", (currency, exchange["minerFee"]))
                db.commit()
            except:
                db.rollback()
    db.close()
