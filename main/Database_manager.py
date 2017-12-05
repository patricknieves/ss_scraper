import MySQLdb
import atexit


def initialize_db():
    create_database()
    global db
    db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017", db="scraper")
    global cur
    cur = db.cursor()
    atexit.register(closeConnection)


def create_database():
    db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017")
    cur = db.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS scraper")
    db.close()


def create_table_exchanges():
    cur.execute("CREATE TABLE IF NOT EXISTS exchanges ("
                "id int(11) NOT NULL AUTO_INCREMENT,"
                "currency_from varchar(45) DEFAULT NULL,"
                "currency_to varchar(45) DEFAULT NULL,"
                "amount_from float DEFAULT NULL,"
                "amount_to float DEFAULT NULL,"
                "fee_from float DEFAULT NULL,"
                "fee_to float DEFAULT NULL,"
                "fee_exchange float DEFAULT NULL,"
                "address_from varchar(120) DEFAULT NULL,"
                "address_to varchar(120) DEFAULT NULL,"
                "hash_from varchar(120) DEFAULT NULL,"
                "hash_to varchar(120) DEFAULT NULL,"
                "time_from datetime DEFAULT NULL,"
                "time_to datetime DEFAULT NULL,"
                "time_exchange datetime DEFAULT NULL,"
                "block_nr int(11) DEFAULT NULL,"
                "dollarvalue_from float DEFAULT NULL,"
                "dollarvalue_to float DEFAULT NULL,"
                "PRIMARY KEY (id))")


def create_table_shapeshift():
    db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017", db="scraper")
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS shapeshift (symbol varchar(45) NOT NULL, fee double DEFAULT NULL, PRIMARY KEY (symbol))")
    db.close()


def create_table_cmc():
    db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017", db="scraper")
    cur = db.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS shapeshift (symbol varchar(45) NOT NULL, fee double DEFAULT NULL, PRIMARY KEY (symbol))")
    db.close()


# Delete all found exchanges in DB
def delete_all_data():
    cur.execute("TRUNCATE TABLE exchanges")


def closeConnection():
    print "Scraper stopped!"
    db.close()