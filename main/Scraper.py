import MySQLdb
import requests
import datetime
import time


def main():
    previous_exchanges = []
    # TODO delete method at the end
    delete_all_data()
    # TODO convert to while true? oder hohe Zahl?
    for number in range(1):
        new_exchanges = get_last_transactions(previous_exchanges)
        if new_exchanges:
            previous_exchanges = new_exchanges
            # TODO call all Searchers in different Threads
            get_ethereum_transaction(new_exchanges)
        else:
            # Sleep for x seconds
            time.sleep(120)


# Delete all data in DB (not used)
def delete_all_data():
    cur.execute("TRUNCATE TABLE exchanges")


def get_last_transactions(previous_exchanges):
    # Request last 50 Transactions from Shapeshift
    new_exchanges = requests.get("https://shapeshift.io/recenttx/50").json()

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
            filtered_new_exchanges = filtered_new_exchanges[:(i - 1)]

    # Write data to MySQL DB
    for exchange in filtered_new_exchanges:
        print exchange["curIn"] + " " + exchange["curOut"] + " " + str(exchange["amount"]) + " " + str(
            exchange["timestamp"])
        try:
            cur.execute(
                "INSERT INTO exchanges (currency_from, currency_to, amount_from, time_exchange) VALUES (%s, %s, %s, %s)",
                (exchange["curIn"], exchange["curOut"], exchange["amount"],
                 datetime.datetime.fromtimestamp(exchange["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')))
            db.commit()
            exchange["id"] = cur.execute("SELECT LAST_INSERT_ID()")
        except:
            db.rollback()

    return filtered_new_exchanges


def get_ethereum_transaction(new_exchanges):
    # Take ETH exchanges only
    ethereum_exchanges = [exchange for exchange in new_exchanges if "ETH" == exchange["curIn"]]
    if ethereum_exchanges:
        # Request last block number
        last_block_number = requests.get("https://etherchain.org/api/blocks/count").json()["data"][0][
            "count"]  # oder doch [1]?
        for number in range(30):
            # Sleep for 7 seconds
            time.sleep(7)
            # Get Block
            transactions = requests.get("https://etherchain.org/api/block/" + last_block_number - number + "/tx").json()[
                "data"]
            # Check if Block much older than Exchanges
            if (datetime.datetime.fromtimestamp(new_exchanges[-1]["timestamp"]).strftime('%Y-%m-%d %H:%M:%S') - transactions[1]["time"].replace("T", " ")[:-5]).total_seconds() > 120:
                return

            for transaction in transactions:
                for exchange in new_exchanges:
                    if exchange["amount"] == transaction["amount"] / 1E+18:
                        exchange_details = requests.get(
                            "https://shapeshift.io/txStat/" + transaction["recipient"]).json()
                        if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange[
                            "curOut"]:
                            # Update DB
                            try:
                                cur.execute(
                                    "UPDATE exchanges SET  amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s time_from = %s WHERE id = %s",
                                    (exchange_details["outgoingCoin"], transaction["gasUsed"]/transaction["price"], transaction["recipient"], exchange_details["withdraw"], transaction["hash"], exchange_details["transaction"], transaction["time"].replace("T", " ")[:-5], exchange["id"]))
                                db.commit()
                                search_corresponding_transaction("ETH", exchange_details["transaction"], exchange["id"])
                            except:
                                db.rollback()
                            new_exchanges.remove(exchange)
                            # Quit search if no more new exchanges
                            if not new_exchanges:
                                return
                            # Search in next transaction
                            break


def search_corresponding_transaction(currency, tx_hash, exchange_id):
    if currency == "ETH":
        transaction = requests.get("https://api.blockcypher.com/v1/btc/main/txs/" + tx_hash).json()["data"][0] # Oder doch [1]?
        cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["time"].replace("T", " ")[:-5], transaction["gasUsed"] / transaction["price"], exchange_id))
    elif currency == "BTC":
        transaction = requests.get("https://api.blockcypher.com/v1/btc/main/txs/" + tx_hash).json()
        cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["received"].replace("T", " ")[:-5], transaction["fees"] / 100000000, exchange_id))
    elif currency == "LTC":
        transaction = requests.get("https://api.blockcypher.com/v1/ltc/main/txs/" + tx_hash).json()
        cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["received"].replace("T", " ")[:-5], transaction["fees"] / 100000000, exchange_id))


db = MySQLdb.connect(host="localhost", user="root", passwd="admin", db="scraper")
cur = db.cursor()
if __name__ == "__main__": main()
