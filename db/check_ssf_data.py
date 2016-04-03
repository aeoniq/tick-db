import insert_data
from read_transactions_from_excel import TickDataRows
import fileUtils
import datetime

test_folder = 'C:\\Users\\Peerapong\\Documents\\market data\\lot 20141209\\SSF\\ADVANC\\'
test_folder2 = 'C:\\Users\\Peerapong\\Documents\\market data\\lot 20141209\\SSF\\SPALI\\'


def check_ssf_data(fileList, outFileName):
    out = open(outFileName, 'wt', encoding="utf8")
    #g.write(
    for filename in fileList:
        path_tokens = filename.split("\\")
        if  "/" in filename:
            raise ValueError
        fname = path_tokens[len(path_tokens)-1]
        print(fname)
        values, col_str, format_str, con_symbol = insert_data.get_ssf_metadata_string_and_values(fname, "", "NULL", "NULL")
        
        num_rows = 0
        for row in TickDataRows(filename):
            num_rows = num_rows + 1
            result = [col.value for col in row]
            isValid, errorCode = insert_data.validate_ssf_data(filename, result)
            if isValid:
                pass
            else:
                out.write(filename + "," + str(errorCode+10) + "," + str(result[0]))
            break;
        if num_rows == 0:
            out.write(filename + ",1,")
    out.close()


def check_ssf_in_folder(folderName, outFileName):
    fileList = fileUtils.listFiles(folderName)
    fileList = [(folderName + fname) for fname in fileList]
    #print(fileList)
    print("Checking ssf files in folder ", folderName)
    check_ssf_data(fileList, outFileName)
    print("All done.")
