import MySQLdb
import requests

def update_coinmarketcap_data():
    db = MySQLdb.connect(host="localhost", user="root", passwd="admin", db="scraper")
    cur = db.cursor()

    # Delete old values
    cur.execute("TRUNCATE TABLE coinmarketcap")

    # Request all request pairs with fees from Shapeshift
    coinmarketcap_data = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0").json()

    # Write data to MySQL DB
    for currencyData in coinmarketcap_data:
        try:
            cur.execute("INSERT INTO coinmarketcap (symbol, name, value) VALUES (%s, %s, %s)", (currencyData["symbol"], currencyData["name"], currencyData["price_usd"]))
            db.commit()
        except:
            db.rollback()
    db.close()
