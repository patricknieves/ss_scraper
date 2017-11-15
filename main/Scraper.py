from __future__ import division

import MySQLdb
import requests
import datetime
import time
import Coinmarketcap
import Shapeshift

def main():
    #Update Coinmarketcap data and Shapeshift Fees
    #Coinmarketcap.update_coinmarketcap_data()
    #Shapeshift.update_shapeshift_fees()

    previous_exchanges = []
    # TODO delete method at the end
    delete_all_data()

    for number in range(3):
        new_exchanges = get_last_transactions(previous_exchanges)
        if new_exchanges:
            previous_exchanges = new_exchanges
            print ("Starting loop: " + str(datetime.datetime.now()))
            start_time = time.time()
            get_ethereum_transaction(new_exchanges)
            get_litecoin_transaction(new_exchanges)
            get_bitcoin_transaction(new_exchanges)
            print ("Finished loop: " + str(datetime.datetime.now()))
            elapsed_time = time.time() - start_time
            if elapsed_time < 180:
                time.sleep(180 - elapsed_time)
        else:
            # Sleep for x seconds
            time.sleep(180)


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
    for exchange in reversed(filtered_new_exchanges):
        print exchange["curIn"] + " " + exchange["curOut"] + " " + str(exchange["amount"]) + " " + str(
            exchange["timestamp"])
        try:
            #Get corresponding Shapeshift Fee and Coinmarketcap data
            cur.execute("SELECT fee FROM shapeshift WHERE symbol = %s", exchange["curOut"])
            fee_exchange = cur.fetchone()[0]
            cur.execute("SELECT value FROM coinmarketcap WHERE symbol = %s", exchange["curIn"])
            dollarvalue_from = cur.fetchone()[0]
            cur.execute("SELECT value FROM coinmarketcap WHERE symbol = %s", exchange["curOut"])
            dollarvalue_to = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO exchanges (currency_from, currency_to, amount_from, time_exchange, fee_exchange, dollarvalue_from, dollarvalue_to) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (exchange["curIn"], exchange["curOut"], exchange["amount"],
                 datetime.datetime.utcfromtimestamp(exchange["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'), fee_exchange, dollarvalue_from, dollarvalue_to))
            db.commit()
            exchange["id"] = cur.lastrowid
        except:
            db.rollback()

    return filtered_new_exchanges


def get_ethereum_transaction(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "ETH" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        last_block_number = int(requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_blockNumber?token=Wh9YuEIhi7tqseXn8550").json()["result"], 16)
        for number in range(60):
            # Sleep for 7 seconds
            #time.sleep(3)
            # Get Block
            print (str(last_block_number - number))
            block = requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_getBlockByNumber?params=%5B%22" + hex(last_block_number - number) + "%22%2C%20true%5D&token=Wh9YuEIhi7tqseXn8550").json()["result"]
            transactions = block["transactions"]
            if transactions:
                # Check if Block much older than Exchanges
                time_oldest_transaction = datetime.datetime.utcfromtimestamp(filtered_new__exchanges[-1]["timestamp"])
                time_block = datetime.datetime.utcfromtimestamp(int(block["timestamp"], 16))
                if ((time_oldest_transaction - time_block)).total_seconds() > 180:
                    return

                for transaction in transactions:
                    for exchange in filtered_new__exchanges:
                        time_exchange = datetime.datetime.utcfromtimestamp(exchange["timestamp"])
                        # BC1 Tx must happen before SS Tx (SS Tx Time is when money recieved)
                        if exchange["amount"] == int(transaction["value"], 16) / 1E+18 and ((time_exchange - time_block)).total_seconds() > 0:
                            exchange_details = requests.get(
                                "https://shapeshift.io/txStat/" + transaction["to"]).json()
                            if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange[
                                "curOut"]:
                                # Update DB
                                cur.execute(
                                    "UPDATE exchanges SET amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s, time_from = %s WHERE id = %s",
                                    (exchange_details["outgoingCoin"], int(transaction["gas"], 16)*(int(transaction["gasPrice"], 16) / 1E+18), exchange_details["address"], exchange_details["withdraw"], transaction["hash"], exchange_details["transaction"], time_block.strftime('%Y-%m-%d %H:%M:%S'), exchange["id"]))
                                db.commit()
                                search_corresponding_transaction(exchange_details["outgoingType"], exchange_details["transaction"], exchange["id"])
                                filtered_new__exchanges.remove(exchange)
                                # Quit search if no more new exchanges
                                if not filtered_new__exchanges:
                                    return
                                # Search in next transaction
                                break

def get_bitcoin_transaction(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "BTC" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        last_block_number = requests.get("https://blockchain.info/de/latestblock").json()["height"]
        for number in range(4):
            # Sleep for 7 seconds
            #time.sleep(3)
            # Get Block
            print (str(last_block_number - number))
            block = requests.get("https://blockchain.info/de/block-height/" + (str(last_block_number - number)) + "?format=json").json()["blocks"][0]
            transactions = block["tx"]
            if transactions:
                # Check if Block much older than Exchanges
                time_oldest_transaction = datetime.datetime.utcfromtimestamp(filtered_new__exchanges[-1]["timestamp"])
                time_block = datetime.datetime.utcfromtimestamp(block["time"])
                # For Bitcoin not useful maybe bacause very old Tx can be in new Block as Blocks are mined sometimes after 1 hour
                if ((time_oldest_transaction - time_block)).total_seconds() > 900:
                    return

                for transaction in transactions:
                    for out in transaction["out"]:
                        for exchange in filtered_new__exchanges:
                            time_exchange = datetime.datetime.utcfromtimestamp(exchange["timestamp"])
                            time_transaction = datetime.datetime.utcfromtimestamp(transaction["time"])
                            # BC1 Tx must happen before SS Tx (SS Tx Time is when money recieved)
                            if exchange["amount"]*100000000 == out["value"] and ((time_exchange - time_transaction)).total_seconds() > -180:
                                exchange_details = requests.get(
                                    "https://shapeshift.io/txStat/" + out["addr"]).json()
                                if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange[
                                    "curOut"]:
                                    # Get fee_from
                                    fee_from = requests.get("https://api.blockcypher.com/v1/btc/main/txs/" + transaction["hash"]).json()["fees"] / 100000000

                                    # Update DB
                                    try:
                                        cur.execute(
                                            "UPDATE exchanges SET  amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s, time_from = %s WHERE id = %s",
                                            (exchange_details["outgoingCoin"], fee_from, exchange_details["address"], exchange_details["withdraw"], transaction["hash"], exchange_details["transaction"], time_transaction.strftime('%Y-%m-%d %H:%M:%S'), exchange["id"]))
                                        db.commit()
                                        search_corresponding_transaction(exchange_details["outgoingType"], exchange_details["transaction"], exchange["id"])
                                    except:
                                        db.rollback()
                                    filtered_new__exchanges.remove(exchange)
                                    # Quit search if no more new exchanges
                                    if not filtered_new__exchanges:
                                        return
                                    # Search in next transaction
                                    break

def get_litecoin_transaction(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "LTC" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        #time.sleep(320)
        last_block_number = requests.get("https://chain.so/api/v2/get_info/LTC").json()["data"]["blocks"]
        for number in range(29):
            # Sleep for 5 seconds
            if (number%10 == 9):
                time.sleep(50)
            # Get Block
            print (str(last_block_number - number))
            block = requests.get("https://chain.so/api/v2/block/LTC/" + (str(last_block_number - number))).json()["data"]
            transactions = block["txs"]
            if transactions:
                # Check if Block much older than Exchanges
                time_oldest_transaction = datetime.datetime.utcfromtimestamp(filtered_new__exchanges[-1]["timestamp"])
                time_block = datetime.datetime.utcfromtimestamp(block["time"])
                if ((time_oldest_transaction - time_block)).total_seconds() > 1200:
                    return

                for transaction in transactions:
                    for out in transaction["outputs"]:
                        for exchange in filtered_new__exchanges:
                            time_exchange = datetime.datetime.utcfromtimestamp(exchange["timestamp"])
                            # BC1 Tx must happen before SS Tx (SS Tx Time is when money recieved)
                            if exchange["amount"] == float(out["value"]) and ((time_exchange - time_block)).total_seconds() > -300:
                                exchange_details = requests.get(
                                    "https://shapeshift.io/txStat/" + out["address"]).json()
                                if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange["curOut"]:

                                    # Update DB
                                    try:
                                        cur.execute(
                                            "UPDATE exchanges SET  amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s, time_from = %s WHERE id = %s",
                                            (exchange_details["outgoingCoin"], transaction["fee"], exchange_details["address"], exchange_details["withdraw"], transaction["txid"], exchange_details["transaction"], time_block.strftime('%Y-%m-%d %H:%M:%S'), exchange["id"]))
                                        db.commit()
                                        search_corresponding_transaction(exchange_details["outgoingType"], exchange_details["transaction"], exchange["id"])
                                    except:
                                        db.rollback()
                                    filtered_new__exchanges.remove(exchange)
                                    # Quit search if no more new exchanges
                                    if not filtered_new__exchanges:
                                        return
                                    # Search in next transaction
                                    break

def search_corresponding_transaction(currency, tx_hash, exchange_id):
    if currency == "ETH":
        transaction = requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_getTransactionByHash?params=%5B%22" + tx_hash + "%22%5D&token=Wh9YuEIhi7tqseXn8550").json()["result"]
        block = requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_getBlockByNumber?params=%5B%22" + transaction["blockNumber"] + "%22%2C%20true%5D&token=Wh9YuEIhi7tqseXn8550").json()["result"]
        cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (datetime.datetime.utcfromtimestamp(int(block["timestamp"], 16)).strftime('%Y-%m-%d %H:%M:%S'), int(transaction["gas"], 16)*(int(transaction["gasPrice"], 16) / 1E+18), exchange_id))
    elif currency == "BTC":
        transaction = requests.get("https://api.blockcypher.com/v1/btc/main/txs/" + tx_hash).json()
        cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["received"].replace("T", " ")[:-5], (transaction["fees"] / 100000000), exchange_id))
    elif currency == "LTC":
        transaction = requests.get("https://api.blockcypher.com/v1/ltc/main/txs/" + tx_hash).json()
        cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["received"].replace("T", " ")[:-5], (transaction["fees"] / 100000000), exchange_id))


db = MySQLdb.connect(host="localhost", user="root", passwd="admin", db="scraper")
cur = db.cursor()
if __name__ == "__main__": main()
