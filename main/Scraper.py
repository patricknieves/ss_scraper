import datetime
import time
import Coinmarketcap
import Shapeshift
import Bitcoin
import Ether
import Litecoin
import Database_manager

def main():
    # Create MySQL Database and connect
    Database_manager.initialize_db()
    # Create MySQL Tables
    Database_manager.create_table_shapeshift()
    Database_manager.create_table_cmc()
    Database_manager.create_table_exchanges()

    previous_exchanges = []

    # Dalete all data from DB
    Database_manager.delete_all_data()

    # Update Coinmarketcap data
    last_time_updated_cmc = update_cmc(None)
    # Update Shapeshift
    last_time_updated_ss = update_ssf(None)

    while True:
        result = Shapeshift.get_last_transactions(previous_exchanges)
        if result:
            new_exchanges = result["new_exchanges"]
            # After every loop: Wait the half of the duration of retrieved 50 Txs
            duration_to_wait = result["duration"].total_seconds()/2
            if new_exchanges:
                print ("Starting loop: " + str(datetime.datetime.now()))
                start_time = time.time()

                # Update Coinmarketcap data
                last_time_updated_cmc = update_cmc(last_time_updated_cmc)
                # Update Shapeshift
                last_time_updated_ss = update_ssf(last_time_updated_ss)

                previous_exchanges = new_exchanges

                print ("Search for Ethereum Txs...")
                left_transactions_eth = Ether.get_ethereum_transaction(new_exchanges)
                if left_transactions_eth:
                    Ether.get_ethereum_transaction_infura(left_transactions_eth)
                print ("Search for Litecoin Txs...")
                left_transactions_ltc = Litecoin.get_litecoin_transaction(new_exchanges)
                if left_transactions_ltc:
                    time.sleep(30)
                    Litecoin.get_litecoin_transaction(left_transactions_ltc)
                print ("Search for Bitcoin Txs...")
                left_transactions_btc = Bitcoin.get_bitcoin_transaction(new_exchanges)
                if left_transactions_btc:
                    time.sleep(30)
                    Bitcoin.get_bitcoin_transaction(left_transactions_btc)
                print ("Finished loop: " + str(datetime.datetime.now()))
                elapsed_time = time.time() - start_time
                if elapsed_time < duration_to_wait:
                    print ("Done! Wait " + str(duration_to_wait - elapsed_time) + " seconds")
                    time.sleep(duration_to_wait - elapsed_time)

            else:
                print ("No Transactions. Wait " + str(duration_to_wait) + " seconds")
                time.sleep(duration_to_wait)
        else:
            print ("Retrieving Transactions from Shapeshift didn't work")
            time.sleep(180)

def update_cmc(last_time_updated_cmc):
    # Update Coinmarketcap data every 10 min
    if not last_time_updated_cmc or (time.time() - last_time_updated_cmc) > 600:
        print ("Updating CMC Data")
        Coinmarketcap.update_coinmarketcap_data()
        return time.time()
    else:
        return last_time_updated_cmc

def update_ssf(last_time_updated_ss):
    # Update Coinmarketcap data every 30 min
    if not last_time_updated_ss or (time.time() - last_time_updated_ss) > 1800:
        print ("Updating Shapeshift Fees")
        Shapeshift.update_shapeshift_fees()
        return time.time()
    else:
        return last_time_updated_ss


if __name__ == "__main__": main()