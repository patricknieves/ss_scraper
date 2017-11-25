import MySQLdb
import requests
import traceback
import datetime
import Database_manager


def get_last_transactions(previous_exchanges):

    # Request last 50 Transactions from Shapeshift
    new_exchanges = requests.get("https://shapeshift.io/recenttx/50").json()

    if new_exchanges:
        duration = datetime.datetime.utcfromtimestamp(new_exchanges[1]["timestamp"]) - datetime.datetime.utcfromtimestamp(new_exchanges[-1]["timestamp"])
        print ("Time dif (All): " + str(duration))
        # Take BTC, ETH and LTC exchanges only
        filtered_new_exchanges = [exchange for exchange in new_exchanges if
                                  "BTC" == exchange["curIn"] or "ETH" == exchange["curIn"] or "LTC" == exchange["curIn"]]

        # Take new exchanges only
        if previous_exchanges:
            i = 0
            new = True
            while new is True and i < len(filtered_new_exchanges):
                new = previous_exchanges[0]["timestamp"] < filtered_new_exchanges[i]["timestamp"] or \
                      previous_exchanges[0]["timestamp"] == filtered_new_exchanges[i]["timestamp"] and (
                          previous_exchanges[0]["curIn"] != filtered_new_exchanges[i]["curIn"] or
                          previous_exchanges[0]["curOut"] !=
                          filtered_new_exchanges[i]["curOut"] or previous_exchanges[0]["amount"] !=
                          filtered_new_exchanges[i]["amount"])
                i = i + 1
            if new is False and i != 0:
                count_old = len(filtered_new_exchanges)
                filtered_new_exchanges = filtered_new_exchanges[:(i - 1)]
                print (str((count_old - len(filtered_new_exchanges))) + " Txs removed from " + str(count_old))
            else:
                print ("Nothing filtered")

        # Write data to MySQL DB
        print ("Insert " + str(len(filtered_new_exchanges)) + " Transactions")
        for exchange in reversed(filtered_new_exchanges):
            try:
                #Get corresponding Shapeshift Fee and Coinmarketcap data
                Database_manager.cur.execute("SELECT fee FROM shapeshift WHERE symbol = %s", [exchange["curOut"]])
                fee_exchange = Database_manager.cur.fetchone()[0]
                Database_manager.cur.execute("SELECT value FROM coinmarketcap WHERE symbol = %s", [exchange["curIn"]])
                dollarvalue_from = Database_manager.cur.fetchone()[0]
                Database_manager.cur.execute("SELECT value FROM coinmarketcap WHERE symbol = %s", [exchange["curOut"]])
                dollarvalue_to = Database_manager.cur.fetchone()[0]
                Database_manager.cur.execute(
                    "INSERT INTO exchanges (currency_from, currency_to, amount_from, time_exchange, fee_exchange, dollarvalue_from, dollarvalue_to) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (exchange["curIn"], exchange["curOut"], exchange["amount"],
                     datetime.datetime.utcfromtimestamp(exchange["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'), fee_exchange, dollarvalue_from, dollarvalue_to))
                Database_manager.db.commit()
                exchange["id"] = Database_manager.cur.lastrowid
            except:
                print("Problem saving Transaction. Transaction was deleted")
                traceback.print_exc()
                filtered_new_exchanges.remove(exchange)
                Database_manager.db.rollback()

        return {"new_exchanges": filtered_new_exchanges, "duration": duration}

def update_shapeshift_fees():
    # Delete old values
    Database_manager.cur.execute("TRUNCATE TABLE shapeshift")

    # Request all request pairs with fees from Shapeshift
    shapeshift_data = requests.get("https://shapeshift.io/marketinfo/").json()

    # Write data to MySQL DB
    for exchange in shapeshift_data:
        currency = exchange["pair"].split('_')[1]
        Database_manager.cur.execute("SELECT 1 FROM shapeshift WHERE symbol=%s LIMIT 1", [currency])
        data = Database_manager.cur.fetchone()
        if data is None:
            try:
                Database_manager.cur.execute("INSERT INTO shapeshift (symbol, fee) VALUES (%s, %s)", (currency, exchange["minerFee"]))
                Database_manager.db.commit()
            except:
                traceback.print_exc()
                Database_manager.db.rollback()
