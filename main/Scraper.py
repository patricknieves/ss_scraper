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

    last_time_updated_cmc = None
    last_time_updated_ss = None

    start_time = time.time()
    elapsed_time = 0

    # Run a whole day
    while elapsed_time < 24*60*60:
        start_time_loop = time.time()
        # Update Coinmarketcap data
        last_time_updated_cmc = update_cmc(last_time_updated_cmc)
        # Update Shapeshift
        last_time_updated_ss = update_ssf(last_time_updated_ss)

        result = Shapeshift.get_last_transactions(previous_exchanges)
        if result:
            new_exchanges = result["new_exchanges"]
            if new_exchanges:
                previous_exchanges = new_exchanges
            # After every loop: Wait the half of the duration of retrieved 50 Txs
            duration_to_wait = result["duration"].total_seconds()/2
            elapsed_time_loop = time.time() - start_time_loop
            if elapsed_time < duration_to_wait:
                print ("Done! Wait " + str(duration_to_wait - elapsed_time_loop) + " seconds")
                time.sleep(duration_to_wait - elapsed_time_loop)
        else:
            print ("Retrieving Transactions from Shapeshift didn't work")
            time.sleep(120)

        elapsed_time = time.time() - start_time

def update_cmc(last_time_updated_cmc):
    # Update Coinmarketcap data every 5 min
    if not last_time_updated_cmc or (time.time() - last_time_updated_cmc) > 300:
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