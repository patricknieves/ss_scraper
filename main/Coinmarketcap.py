import requests
import traceback
import Database_manager


def update_coinmarketcap_data():
    # Delete old values
    Database_manager.cur.execute("TRUNCATE TABLE coinmarketcap")

    # Request all request pairs with fees from Shapeshift
    coinmarketcap_data = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0").json()

    # Write data to MySQL DB
    for currencyData in coinmarketcap_data:
        try:
            Database_manager.cur.execute("INSERT INTO coinmarketcap (symbol, name, value) VALUES (%s, %s, %s)", (currencyData["symbol"], currencyData["name"], currencyData["price_usd"]))
            Database_manager.db.commit()
        except:
            #traceback.print_exc()
            Database_manager.db.rollback()
