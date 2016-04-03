import insert_data


def check_ssf_data(filename, outFileName):
    out = open(outFileName, 'wt', encoding="utf8")
    #g.write(
    path_tokens = filename.split("\\")
    if  "/" in filename:
        raise ValueError
    fname = path_tokens[len(path_tokens)-1]
    values, col_str, format_str, con_symbol = insert_data.get_ssf_metadata_string_and_values(fname, "", "NULL", "NULL")

    serial = "NULL"
    
    num_rows = 0
    for row in TickDataRows(filename):
        num_rows = num_rows + 1
        result = [col.value for col in row]
        isValid, errorCode = insert.validate_data_method(filename, result):
        break;
    out.close()
