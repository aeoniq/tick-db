import mysql.connector
from mysql.connector import errorcode
from read_transactions_from_excel import TickDataRows
import fileUtils
import datetime

# example
ssf_fname = 'C:\\Users\\Peerapong\\Documents\\market data\\SSF\\ADVANC\\ADVANCfxU4_7-3-2014.xlsx'
test_folder = 'C:\\Users\\Peerapong\\Documents\\market data\\lot 20141209\\SSF\\ADVANC\\'
test_folder2 = 'C:\\Users\\Peerapong\\Documents\\market data\\lot 20141209\\SSF\\SPALI\\'

# common human errors
# 1. d-m-y vs. m-d-y
# 2. missing column

config = {
  'user': 'peerapong',
  'password': 'halgtrade',
  'host': '127.0.0.1',
  'database': 'testdb',
  'raise_on_warnings': True,
}

InsertStatementHeader = "INSERT INTO "

InsertSSFFileMetadataHeader = "INSERT INTO %s "
" (date, filename,"
" ticker,"
" contract,"
" setting1,"
" has_data, "
" num_rows"
") VALUES "

# currently not used
def ssf_column_config():
    col_num = {}
    col_num['timestamp'] = 1
    col_num['exchange_timestamp'] = -1
    col_num['last'] = 2
    col_num['trade_price'] = 3
    col_num['trade_volume'] = 4
    col_num['best_bid'] = 5
    col_num['bid_size'] = 6
    col_num['best_ask'] = 7
    col_num['ask_size'] = 8
    col_num['turnover'] = 9
    col_num['calc_vwap'] = 10
    col_num['flow'] = 11
    col_num['trade_flag'] = 12
    col_num['uptick'] = -1
    return col_num

def ssf_column_names():
    names = [
        'con_symbol',
        'file_serial_no',
        'timestamp',
        'last',
        'trade_price',
        'trade_volume',
        'best_bid',
        'bid_size',
        'best_ask',
        'ask_size',
        'turnover',
        'calc_vwap',
        'flow',
        'trade_flag'
        ]
    return names
        

def get_column_string_and_value(col_names, vals):
    col_str = '('
    first = True
    num_cols = len(col_names)
    out_vals = []
    for i in range(0,num_cols):
        if vals[i] is not None:
            out_vals.append(vals[i])
            if first:
                first = False
            else:
                col_str = col_str + ", "
            col_str = col_str + col_names[i]
    col_str = col_str + ")"
    return (col_str, tuple(out_vals))

# year_insertion is the character that will be inserted at position 1
#    in contract_spec
def contract_to_symbol(stock_ticker, contract_spec, year_insertion = "1"):
    cspec = contract_spec.upper()
    return stock_ticker + cspec[0] + year_insertion + cspec[1:]

def string_to_date(date_str):
    parts = date_str.split("-")
    if len(parts) != 3:
        raise ValueError
    else:
        if len(parts[0]) == 4:
            return datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
        elif len(parts[2]) == 4:
            #return datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))
            return datetime.date(int(parts[2]), int(parts[0]), int(parts[1]))
        else:
            print(parts)
            raise ValueError

def get_ssf_metadata_string_and_values(filename, setting1, has_data, num_rows):
    # break down file name
    parts = filename.split("_")
    names = parts[0].split("fx")
    con_symbol = contract_to_symbol(names[0], names[1])
    if len(parts) < 2:
        raise ValueError
    if len(setting1) <= 0:
        setting1 = "NULL"
    date_str = parts[1].split(".")[0]
    date = string_to_date(date_str)
    column_string = "(date, filename, symbol, contract, setting1, has_data, num_rows)"
    format_string = "(%s, \"" + filename + "\", %s, %s, " + setting1 + ", " + has_data + ", " + num_rows + ")"
    return ((date, names[0].upper(), names[1].upper()), column_string, format_string, con_symbol)

# error code
# 1: date mismatch
def validate_ssf_data(filename, row):
    parts = filename.split("_")
    if len(parts) < 2:
        raise ValueError
    date_str = parts[1].split(".")[0]
    date = string_to_date(date_str)
    rowdate = row[0]
    if date.day == rowdate.day and date.month == rowdate.month and date.year == rowdate.year:
        return (True, 0)
    else:
        return (False, 1)

# cursor must be a buffered cursor
# abort_if_filename_already_exists - abort the execution if the filename already exists in the table ssf_files_table_name
# return value
#    -2: data validation failed (returned False)
#   -13: filename already exists in the table specified by ssf_files_table_name 
# TODO: 
def import_ssf_to_mysql(filename, cursor, ssf_table_name, ssf_files_table_name = "", abort_if_filename_already_exists = True, validate_data = False, validate_data_method = []):
    path_tokens = filename.split("\\")
    if  "/" in filename:
        raise ValueError
    fname = path_tokens[len(path_tokens)-1]
    values, col_str, format_str, con_symbol = get_ssf_metadata_string_and_values(fname, "", "NULL", "NULL")

    serial = "NULL"
    if len(ssf_files_table_name) > 0:
        stmt = "INSERT INTO " + ssf_files_table_name + " " + col_str + " VALUES " \
        + format_str + ";"
        #print(stmt)
        cursor.execute(stmt, values)
        # get file_serial_no
        stmt = "SELECT file_serial_no from " + ssf_files_table_name \
        + " WHERE filename = \"" + fname + "\" ORDER BY file_serial_no DESC;"
        print(stmt)
        cursor.execute(stmt)
        num_results = 0
        for result in cursor:
            print(result)
            if num_results > 0:
                if abort_if_filename_already_exists:
                    return -13
            else:
                serial = result[0]
            num_results = num_results + 1
    
    num_rows = 0
    for row in TickDataRows(filename):
        num_rows = num_rows + 1
        result = [col.value for col in row]
        if validate_data:
            if validate_data_method(filename, result):
                pass
            else:
                return -2
        values = [con_symbol, serial]
        values[len(values):] = result
        print('to insert:', values)
        col_str, vals = get_column_string_and_value(ssf_column_names(), values)
        format_str = "(%s"
        for i in range(1, len(vals)):
            format_str = format_str + ", %s"
        format_str = format_str + ")"
        stmt = InsertStatementHeader + ssf_table_name + " " + col_str + " VALUES " + format_str
        print(stmt)
        try:
            cursor.execute(stmt, vals)
        except mysql.connector.errors.DatabaseError as err:
            if err.errno == 1265:
                pass
            else:
                raise

    if len(ssf_files_table_name) > 0:
        if num_rows > 0:
            has_data = True
        else:
            has_data = False
        update_stmt = "UPDATE " + ssf_files_table_name + " SET has_data = %s" \
                      + ", num_rows = " + str(num_rows)
        cursor.execute(update_stmt, (has_data,))
    return num_rows

def TickDataNameFilter(fileName):
    if len(fileName) > 6:
        if fileName.endswith("xlsx"):
            return True
    return False

def ImportAllDataFilesToMySQL(folderName, sqlConn, insertMethod, fileNameFilter, numberOfFilesProcessed):
    numberOfFilesProcessed = 0
    fileL = fileUtils.listFiles(folderName);
    cursor = sqlConn.cursor(buffered=True)
    abort_all = False
    for fileName in fileL:
        if fileNameFilter(fileName):
            print(fileName)
            try:
                num_rows = insertMethod(folderName+fileName, cursor)
                if num_rows < 0:
                    print("Error importing file '", fileName, "'", ". Error number = ", num_rows, sep='')
                    abort_all = True
            except ValueError as e:
                print("ValueError encountered while importing file '", fileName, "'")
                print("ValueError: {0}".format(e))
                abort_all = True
            if abort_all:
                print("Rolling back all changes...")
                sqlConn.rollback();
                print("Aborting operation!")
                return False
            numberOfFilesProcessed = numberOfFilesProcessed + 1
    return True

def is_valid_ssf_data(filename, row):
    isValid, code = validate_ssf_data(filename, row)
    return isValid

def ImportSSFFileToMySqlDefault(full_filename, cursor):
    return import_ssf_to_mysql(full_filename, cursor, "ssf", "ssf_raw_files", True, True, is_valid_ssf_data)

def insert_test(filename):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True)
    numrows = import_ssf_to_mysql(filename, cursor, "ssf", "ssf_raw_files", True, is_valid_ssf_data)
    if numrows < 0:
        print("error code = ", numrows)
        cnx.rollback()
    else:
        print(numrows, ' rows inserted')
        cnx.commit()
    cnx.close()
