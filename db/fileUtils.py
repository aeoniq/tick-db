from os import walk

def listFiles(folderName):
    f = []
    for (dirpath, dirnames, filenames) in walk(folderName):
        f.extend(filenames)
    return f


    #f = open("C:\\Users\\Him\\Documents\\4492 comp app\\midterm\\student code\\task3_5442726726_Question.bas");
    #str = f.read();
    #print(str)
