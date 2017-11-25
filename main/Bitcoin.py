from __future__ import division
import requests
import traceback
import datetime
import Database_manager
import Tor
import Corresponding_tx


def get_bitcoin_transaction(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "BTC" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        for attempt in range(10):
            try:
                last_block_number = requests.get("https://blockchain.info/de/latestblock").json()["height"]
            except:
                Tor.change_ip()
            else:
                break
        else:
            print("Counldn't get last number of block from Blockchain")
            traceback.print_exc()
            return

        for number in range(5):
            # Get Block
            # print (str(last_block_number - number))
            Tor.change_ip()
            transactions = None
            for attempt in range(5):
                try:
                    block = requests.get("https://blockchain.info/de/block-height/" + (str(last_block_number - number)) + "?format=json").json()["blocks"][0]
                    transactions = block["tx"]
                except:
                    Tor.change_ip()
                else:
                    break
            else:
                print("Counldn't get block from Blockchain: " + str(last_block_number - number))
                traceback.print_exc()
                pass

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
                            if exchange["amount"]*100000000 == out["value"] and ((time_exchange - time_transaction)).total_seconds() > -500:
                                # Change Ip Address
                                Tor.change_ip()
                                exchange_details = requests.get(
                                    "https://shapeshift.io/txStat/" + out["addr"]).json()
                                if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange[
                                    "curOut"]:
                                    # Get fee_from
                                    Tor.change_ip()
                                    for attempt in range(5):
                                        try:
                                            fee_from = requests.get("https://chain.so/api/v2/tx/BTC/" + str(transaction["hash"])).json()["data"]["fee"]
                                        except:
                                            Tor.change_ip()
                                        else:
                                            break
                                    else:
                                        for attempt in range(5):
                                            try:
                                                fee_from = requests.get("https://api.blockcypher.com/v1/btc/main/txs/" + str(transaction["hash"])).json()["fees"] / 100000000
                                            except:
                                                Tor.change_ip()
                                            else:
                                                break
                                        else:
                                            print("Counldn't get fee for BTC: " + str(transaction["hash"]))
                                            traceback.print_exc()
                                            pass
                                    # Update DB
                                    try:
                                        Database_manager.cur.execute(
                                            "UPDATE exchanges SET  amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s, time_from = %s WHERE id = %s",
                                            (exchange_details["outgoingCoin"], fee_from, exchange_details["address"], exchange_details["withdraw"], transaction["hash"], exchange_details["transaction"], time_transaction.strftime('%Y-%m-%d %H:%M:%S'), exchange["id"]))
                                        Database_manager.db.commit()
                                        Corresponding_tx.search_corresponding_transaction(exchange_details["outgoingType"], exchange_details["transaction"], exchange["id"])
                                    except:
                                        print("Problem updating found Transaction for: " + str(exchange["curIn"] + " " + str(exchange_details["address"])))
                                        traceback.print_exc()
                                        Database_manager.db.rollback()
                                    filtered_new__exchanges.remove(exchange)

                                    # Quit search if no more new exchanges
                                    if not filtered_new__exchanges:
                                        return
                                    # Search in next transaction
                                    break
