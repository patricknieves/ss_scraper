from __future__ import division
import requests
import traceback
import datetime
import Database_manager
import Tor
import Corresponding_tx


#Search with Chain.so
def get_litecoin_transaction(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "LTC" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        for attempt in range(5):
            try:
                last_block_number = requests.get("https://chain.so/api/v2/get_info/LTC").json()["data"]["blocks"]
            except:
                Tor.change_ip()
            else:
                break
        else:
            for attempt in range(5):
                try:
                    last_block_number = requests.get("https://api.blockcypher.com/v1/ltc/main").json()["height"]
                except:
                    Tor.change_ip()
                else:
                    break
            else:
                print("Counldn't get last number of block for LTC")
                traceback.print_exc()
                return filtered_new__exchanges

        for number in range(60):
            # Get Block
            #if (number%10 == 9):
            Tor.change_ip()
            for attempt in range(5):
                try:
                    block = requests.get("https://chain.so/api/v2/block/LTC/" + (str(last_block_number - number))).json()["data"]
                    transactions = block["txs"]
                except:
                    Tor.change_ip()
                else:
                    break
            else:
                print("Counldn't get block from Chain.so: " + str(last_block_number - number))
                traceback.print_exc()
                return filtered_new__exchanges
                #pass

            if transactions:
                # Check if Block much older than Exchanges
                time_oldest_transaction = datetime.datetime.utcfromtimestamp(filtered_new__exchanges[-1]["timestamp"])
                time_block = datetime.datetime.utcfromtimestamp(block["time"])
                if ((time_oldest_transaction - time_block)).total_seconds() > 1200:
                    return filtered_new__exchanges

                for transaction in transactions:
                    for out in transaction["outputs"]:
                        for exchange in filtered_new__exchanges:
                            time_exchange = datetime.datetime.utcfromtimestamp(exchange["timestamp"])
                            # BC1 Tx must happen before SS Tx (SS Tx Time is when money recieved)
                            if exchange["amount"] == float(out["value"]) and ((time_exchange - time_block)).total_seconds() > -300:
                                # Change Ip Address
                                Tor.change_ip()
                                exchange_details = requests.get(
                                    "https://shapeshift.io/txStat/" + out["address"]).json()
                                if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange["curOut"]:

                                    # Update DB
                                    try:
                                        Database_manager.cur.execute(
                                            "UPDATE exchanges SET  amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s, time_from = %s, block_nr = %s WHERE id = %s",
                                            (exchange_details["outgoingCoin"], transaction["fee"], exchange_details["address"], exchange_details["withdraw"], transaction["txid"], exchange_details["transaction"], time_block.strftime('%Y-%m-%d %H:%M:%S'), (last_block_number - number), exchange["id"]))
                                        Database_manager.db.commit()
                                        Corresponding_tx.search_corresponding_transaction(exchange_details["outgoingType"], exchange_details["transaction"], exchange["id"])
                                    except:
                                        print("Problem updating found Transaction for: " + str(exchange["curIn"] + " " + str(exchange_details["address"])))
                                        traceback.print_exc()
                                        Database_manager.db.rollback()
                                    filtered_new__exchanges.remove(exchange)
                                    # Quit search if no more new exchanges
                                    if not filtered_new__exchanges:
                                        return filtered_new__exchanges
                                    # Search in next transaction
                                    break
    return filtered_new__exchanges