import MySQLdb
import requests
import traceback

def create_table():
    db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017", db="scraper")
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS coinmarketcap (symbol varchar(45) NOT NULL, name varchar(45) DEFAULT NULL, value double DEFAULT NULL, PRIMARY KEY (symbol))")
    db.close()

def update_coinmarketcap_data():
    db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017", db="scraper")
    cur = db.cursor()

    # Delete old values
    cur.execute("TRUNCATE TABLE coinmarketcap")

    # Request all request pairs with fees from Shapeshift
    coinmarketcap_data = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0").json()

    # Write data to MySQL DB
    #arr = ["BTC", "ETH", "LTC"]
    for currencyData in coinmarketcap_data:
        #if currencyData["symbol"] in arr:
        try:
            cur.execute("INSERT INTO coinmarketcap (symbol, name, value) VALUES (%s, %s, %s)", (currencyData["symbol"], currencyData["name"], currencyData["price_usd"]))
            db.commit()
            #arr.remove(currencyData["symbol"])
        except:
            traceback.print_exc()
            db.rollback()
    db.close()
