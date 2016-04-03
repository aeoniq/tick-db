import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'employees'

TABLES = {}
TABLES['ssf'] = (
    "CREATE TABLE `ssf` ("
    "  `trade_no` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,"
    "  `update_timestamp` timestamp,"
    "  `con_symbol` varchar(15),"
    "  `file_serial_no` int(10) UNSIGNED NOT NULL,"
    "  `timestamp` datetime NOT NULL,"
    "  `exchange_timestamp` datetime,"
    "  `last` decimal(10,3),"
    "  `trade_price` decimal(10,3),"
    "  `trade_volume` int(12),"
    "  `best_bid` decimal(10,3),"
    "  `bid_size` int(12),"
    "  `best_ask` decimal(10,3),"
    "  `ask_size` int(12),"
    "  `turnover` decimal(13,3),"
    "  `calc_vwap` decimal(10,3),"
    "  `flow` decimal(13,3),"
    "  `trade_flag` enum('Main Board Automatching', 'Block Trade', 'Odd Lot'),"
    "  `uptick` int(2),"
    "  PRIMARY KEY (`trade_no`)"
    ") ENGINE=InnoDB")

TABLES['ssf_raw_files'] = (
    "CREATE TABLE `ssf_raw_files` ("
    "  `file_serial_no` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
    "  `date` date,"
    "  `filename` varchar(150),"
    # stock symbol
    "  `symbol` varchar(15),"
    "  `contract` varchar(10),"
    "  `setting1` varchar(14),"
    "  `has_data` boolean,"
    "  `num_rows` int(10),"
    "  PRIMARY KEY (`file_serial_no`)"
    "  ) ENGINE=InnoDB")

def create_all_tables(cursor):
    for name, ddl in TABLES.items():
        try:
            print("Creating table {}: ".format(name), end='')
            print(ddl)
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

#cnx.close()
