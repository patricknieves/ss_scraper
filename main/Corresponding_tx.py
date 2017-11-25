from __future__ import division
import requests
import traceback
import datetime
import Database_manager
import Tor


def search_corresponding_transaction(currency, tx_hash, exchange_id):
    for attempt in range(5):
        try:
            if currency == "ETH":
                transaction = requests.get("https://etherchain.org/api/tx/" + str(tx_hash)).json()["data"][0]
                Database_manager.cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["time"].replace("T"," ")[:-5], (transaction["gasUsed"]*(transaction["price"]/ 1E+18)), exchange_id))
            elif currency == "BTC":
                transaction = requests.get("https://chain.so/api/v2/tx/BTC/" + str(tx_hash)).json()["data"]
                Database_manager.cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (datetime.datetime.utcfromtimestamp(transaction["time"]).strftime('%Y-%m-%d %H:%M:%S'), transaction["fee"], exchange_id))
            elif currency == "LTC":
                transaction = requests.get("https://chain.so/api/v2/tx/LTC/" + str(tx_hash)).json()["data"]
                Database_manager.cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (datetime.datetime.utcfromtimestamp(transaction["time"]).strftime('%Y-%m-%d %H:%M:%S'), transaction["fee"], exchange_id))
            Database_manager.db.commit()
        except:
            Tor.change_ip()
        else:
            break
    else:
        search_corresponding_transaction2(currency, tx_hash, exchange_id)

def search_corresponding_transaction2(currency, tx_hash, exchange_id):
    for attempt in range(5):
        try:
            if currency == "ETH":
                transaction = requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_getTransactionByHash?params=%5B%22" + str(tx_hash) + "%22%5D&token=Wh9YuEIhi7tqseXn8550").json()["result"]
                block = requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_getBlockByNumber?params=%5B%22" + str(transaction["blockNumber"]) + "%22%2C%20true%5D&token=Wh9YuEIhi7tqseXn8550").json()["result"]
                Database_manager.cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (datetime.datetime.utcfromtimestamp(int(block["timestamp"], 16)).strftime('%Y-%m-%d %H:%M:%S'), int(transaction["gas"], 16)*(int(transaction["gasPrice"], 16) / 1E+18), exchange_id))
            elif currency == "BTC":
                transaction = requests.get("https://api.blockcypher.com/v1/btc/main/txs/" + str(tx_hash)).json()
                Database_manager.cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["received"].replace("T", " ")[:-5], (transaction["fees"] / 100000000), exchange_id))
            elif currency == "LTC":
                transaction = requests.get("https://api.blockcypher.com/v1/ltc/main/txs/" + str(tx_hash)).json()
                Database_manager.cur.execute("UPDATE exchanges SET  time_to = %s, fee_to = %s WHERE id = %s", (transaction["received"].replace("T", " ")[:-5], (transaction["fees"] / 100000000), exchange_id))
            Database_manager.db.commit()
        except:
            Tor.change_ip()
        else:
            break
    else:
        print("Counldn't get the corresponding Transaction for " + str(currency))
        traceback.print_exc()