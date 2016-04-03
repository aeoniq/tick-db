from openpyxl import load_workbook
import datetime

stock_fname = 'C:\\Users\\Peerapong\\Documents\\market data\\SET100 20150508\\SET100\\ADVANC\\ADVANC.BK_1-8-2015.xlsx'

# ** assume first column must be a datetime
def TickDataRows(filename):
    wb = load_workbook(filename, True)
    num_lines_read = 0
    ws = wb.active
    for row in ws.rows:
        num_lines_read = num_lines_read + 1
        if row[0].value == "Timestamp":
            #print('skip header')
            pass
        elif type(row[0].value) == datetime.datetime:
            yield row
        else:
            print('invalid row ', num_lines_read)
            
def example_StockTickDataIterator():
    i = 0
    for row in TickDataRows(stock_fname):
        i = i + 1
        result = [col.value for col in row]
        print(result)
        if i > 40:
            break
