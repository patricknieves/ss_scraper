from __future__ import division
import requests
import traceback
import datetime
import Database_manager
import Tor
import Corresponding_tx


#Etherchain
def get_ethereum_transaction(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "ETH" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        for attempt in range(10):
            try:
                last_block_number = requests.get("https://etherchain.org/api/blocks/count").json()["data"][0]["count"]
            except:
                Tor.change_ip()
            else:
                break
        else:
            print("Counldn't get last number of block from Etherchain")
            traceback.print_exc()
            return filtered_new__exchanges

        for number in range(90):
            # Get Block
            # print (str(last_block_number - number))
            Tor.change_ip()
            transactions = None

            for attempt in range(5):
                try:
                    transactions = requests.get("https://etherchain.org/api/block/" + (str(last_block_number - number)) + "/tx").json()["data"]
                except:
                    Tor.change_ip()
                else:
                    break
            else:
                print("Counldn't get block from Etherchain: " + str(last_block_number - number))
                traceback.print_exc()
                pass

            if transactions:
                # Check if Block much older than Exchanges
                time_oldest_transaction = datetime.datetime.utcfromtimestamp(filtered_new__exchanges[-1]["timestamp"])
                time_block = datetime.datetime.strptime(transactions[0]["time"].replace("T"," ")[:-5],"%Y-%m-%d %H:%M:%S")
                if ((time_oldest_transaction - time_block)).total_seconds() > 180:
                    return filtered_new__exchanges

                for transaction in transactions:
                    for exchange in filtered_new__exchanges:
                        time_exchange = datetime.datetime.utcfromtimestamp(exchange["timestamp"])
                        # BC1 Tx must happen before SS Tx (SS Tx Time is when money recieved)
                        if exchange["amount"] == transaction["amount"]/1E+18 and ((time_exchange - time_block)).total_seconds() > 0:
                            # Change Ip Address
                            Tor.change_ip()
                            exchange_details = requests.get(
                                "https://shapeshift.io/txStat/" + transaction["recipient"]).json()
                            if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange[
                                "curOut"]:
                                # Update DB
                                try:
                                    Database_manager.cur.execute(
                                        "UPDATE exchanges SET amount_to=%s, fee_from=%s, address_from=%s, address_to=%s, hash_from=%s, hash_to=%s, time_from=%s, block_nr=%s WHERE id=%s",
                                        (exchange_details["outgoingCoin"],(transaction["gasUsed"]*(transaction["price"]/ 1E+18)),exchange_details["address"],exchange_details["withdraw"],transaction["hash"],exchange_details["transaction"],transaction["time"].replace("T"," ")[:-5], (last_block_number - number),exchange["id"]))
                                    Database_manager.db.commit()
                                    Corresponding_tx.search_corresponding_transaction(exchange_details["outgoingType"], exchange_details["transaction"], exchange["id"])
                                except:
                                    print("Problem updating found Transaction for: " + str(exchange["curIn"] + " " + str(exchange_details["address"])))
                                    traceback.print_exc()
                                    Database_manager.db.rollback()

                                # Not deleting, cause might to found with infura
                                #filtered_new__exchanges.remove(exchange)

                                # Quit search if no more new exchanges
                                if not filtered_new__exchanges:
                                    return filtered_new__exchanges
                                # Search in next transaction
                                break
    return filtered_new__exchanges


#Infura (Doesn't work so well because of API Key)
def get_ethereum_transaction_infura(new_exchanges):
    # Take ETH exchanges only
    filtered_new__exchanges = [exchange for exchange in new_exchanges if "ETH" == exchange["curIn"]]
    if filtered_new__exchanges:
        # Request last block number
        for attempt in range(10):
            try:
                last_block_number = int(requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_blockNumber?token=Wh9YuEIhi7tqseXn8550").json()["result"], 16)
            except:
                Tor.change_ip()
            else:
                break
        else:
            print("Counldn't get last number of block from Infura")
            traceback.print_exc()
            return
        for number in range(60):
            # Get Block
            # print (str(last_block_number - number))
            Tor.change_ip()
            transactions = None

            for attempt in range(5):
                try:
                    block = requests.get("https://api.infura.io/v1/jsonrpc/mainnet/eth_getBlockByNumber?params=%5B%22" + hex(last_block_number - number) + "%22%2C%20true%5D&token=Wh9YuEIhi7tqseXn8550").json()["result"]
                    transactions = block["transactions"]
                except:
                    Tor.change_ip()
                else:
                    break
            else:
                print("Counldn't get block from Etherchain: " + str(last_block_number - number))
                traceback.print_exc()
                pass

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
                            # Change Ip Address
                            Tor.change_ip()
                            exchange_details = requests.get("https://shapeshift.io/txStat/" + transaction["to"]).json()
                            if exchange_details["status"] == "complete" and exchange_details["outgoingType"] == exchange[
                                "curOut"]:
                                # Update DB
                                try:
                                    Database_manager.cur.execute(
                                        "UPDATE exchanges SET amount_to = %s, fee_from = %s, address_from = %s, address_to = %s, hash_from = %s, hash_to = %s, time_from = %s WHERE id = %s",
                                        (exchange_details["outgoingCoin"], int(transaction["gas"], 16)*(int(transaction["gasPrice"], 16) / 1E+18), exchange_details["address"], exchange_details["withdraw"], transaction["hash"], exchange_details["transaction"], time_block.strftime('%Y-%m-%d %H:%M:%S'), exchange["id"]))
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