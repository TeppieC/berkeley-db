'''
Copyright 2016 Zhaorui Chen, Zhenyang Li, Jiaxuan Yue

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''
# For sublime: check indent with spaces, and set indent as 4 spaces

# For the 3rd query, range search in based on the primary key of 
# the records --- key value. Therefore if the main index is a B-tree
# then it can be used for this range search.

# imports
from bsddb3 import db
import random
import os
import sys
import time
from subprocess import call

# set the path
PATH_DIRECTORY = "tmp/zhaorui_db/"
DA_FILE = PATH_DIRECTORY+"berkeley_db"

DB_SIZE = 100000
SEED = 10000000

# helper functions
def get_random():
    return random.randint(0, 63)

def get_random_char():
    return chr(97 + random.randint(0, 25))

def databaseExist(dbType):
    if dbType==1:
        return os.path.isfile('./'+DA_FILE+'_btree')
    elif dbType==2:
        return os.path.isfile('./'+DA_FILE+'_hash')
    elif dbType==3:
        return os.path.isfile('./'+DA_FILE+'_index')

def createPopulateDatabase(dbType):
    if databaseExist(dbType):
        print('Database already exist')
        return

    if dbType==1:
        # create a b-tree database
        database = db.DB()
        try:
            database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_CREATE)
        except:
            print('Error creating file')
        random.seed(SEED)
    elif dbType==2:
        # create a hash database
        database = db.DB()
        try:
            database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_CREATE)
        except:
            print('Error creating file')
        random.seed(SEED)
    elif dbType==3:
        # create a index database
        database = db.DB()
        database2 = db.DB()
        try:
            database.open(DA_FILE+'_index', None, db.DB_BTREE, db.DB_CREATE)
            database2.open(DA_FILE+'_secindex', None, db.DB_BTREE, db.DB_CREATE)
        except:
            print('Error creating file')
        random.seed(SEED)

    # insert into keys and values
    for index in range(DB_SIZE):
        # generate random key with random length
        krng = 64 + get_random()
        key = ""
        for i in range(krng):
            key += str(get_random_char())
        # generate random value with random length
        vrng = 64 + get_random()
        value = ""
        for i in range(vrng):
            value += str(get_random_char())
        # encoding
        key = key.encode(encoding='UTF-8')
        value = value.encode(encoding='UTF-8')
        database.put(key, value);

    #database.put(b'teppie',b'chen') ###################### test use
    #database.put(b'teppif',b'chen1')
    #database.put(b'teppid',b'chen2')

    print('Successfully populated the database')
    try:
        database.close()
        if dbType==3:
            database2.close()
    except Exception as e:
        print(e)


def retrieveWithKey(dbType):
    if not databaseExist(dbType):
        print('Database not exist, please select 1 to populate a new database')
        return

    database = db.DB()
    if dbType == 1:
        database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
    elif dbType==2:
        database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_RDONLY)
    elif dbType==3:
        database2 = db.DB()
        database.open(DA_FILE+'_index', None, db.DB_BTREE, db.DB_RDONLY)
        database2.open(DA_FILE+'_secindex', None, db.DB_BTREE, db.DB_RDONLY)

    key = input("Please input a valid key: ")
    startTime = time.time()
    try:
        value = database.get(key.encode(encoding='UTF-8'))
    except:
        print('Value not found')
        database.close()
        return
    endTime = time.time()
    elapsedTimeMilli = 1000000*(endTime-startTime)

    try:
        value = value.decode(encoding='UTF-8')
    except AttributeError:
        print('Value not found in the database')
        database.close()
        return

    print("Retrieved value: %s"%value)
    print("Elapsed time: %d"%elapsedTimeMilli)

    # record in file
    file = open("answers", "a")
    file.write(key+'\n')
    file.write(value+'\n')
    file.write('\n')
    file.close()
    
    try:
        database.close()
        if dbType==3:
            database2.close()
    except Exception as e:
        print(e)

def retrieveWithData(dbType):
    if not databaseExist(dbType):
        print('Database not exist, please select 1 to populate a new database')
        return

    database = db.DB()
    if dbType == 1:
        database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
    elif dbType==2:
        database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_RDONLY)
    elif dbType==3:
        database2 = db.DB()
        database.open(DA_FILE+'_index', None, db.DB_BTREE, db.DB_RDONLY)
        database2.open(DA_FILE+'_secindex', None, db.DB_BTREE, db.DB_RDONLY)

    value = input("Please input a value: ").encode(encoding='UTF-8')
    keys = []
    startTime = time.time()

    for key in database.keys():
        if database.get(key)==value:
            keys.append(key)

    endTime = time.time()
    elapsedTimeMilli = 1000000*(endTime-startTime)
    # record in file
    file = open('answers', 'a')
    if not keys==[]:
        for key in keys:
            print("Retrieved key: "+ key.decode('UTF-8'))
            file.write(key.decode('UTF-8')+'\n')
            file.write(value.decode('UTF-8')+'\n')
            file.write('\n')
        file.close()
    else:
        print('Key not found in the database.')

    print("Elapsed time: %d"%elapsedTimeMilli)
    # close the database
    try:
        database.close()
        if dbType==3:
            database2.close()
    except Exception as e:
        print(e)

def retrieveWithRange(dbType):
    if not databaseExist(dbType):
        print('Database not exist, please select 1 to populate a new database')
        return

    lowerBound = input('Please input the lower bound of the range: ')
    upperBound = input('Please input the upper bound of the range: ')
    while lowerBound>upperBound:
        print('Input invalid. Lower bound should not be higher than upper bound.')
        lowerBound = input('Please input the lower bound of the range: ')
        upperBound = input('Please input the upper bound of the range: ')

    lowerBound = lowerBound.encode('UTF-8')
    upperBound = upperBound.encode('UTF-8')

    database = db.DB()
    if dbType == 1:
        database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
    elif dbType==2:
        database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_RDONLY)
    elif dbType==3:
        database.open(DA_FILE+'_index', None, db.DB_BTREE, db.DB_RDONLY)
        database2 = db.DB()
        database2.open(DA_FILE+'_secindex', None, db.DB_BTREE, db.DB_RDONLY)
        
    results = []
    if dbType==1 or dbType==2:
        startTime = time.time()
        for key in database.keys():
            if key>=lowerBound and key>=upperBound:
                results.append((key,database.get(key)))
        endTime = time.time()
        elapsedTimeMilli = 1000000*(endTime-startTime)

        if not results:
            print('No result found in the database.')
            return
    else:
        keys = database.keys()
        # use binary search to speed up search in indexfile
        start = binarySearch(database.keys(), lowerBound) # locate the start point of the keys
        end = binarySearch(database.keys(), upperBound)# locate the end point of the keys

        startTime = time.time()
        for i in range(start, end):
            results.append((keys[i],database.get(keys[i])))
        endTime = time.time()
        elapsedTimeMilli = 1000000*(endTime-startTime)
            
    # record in file
    file = open('answers', 'a')
    print('Retrieved: ')
    for kVPair in results:
        key = kVPair[0].decode('UTF-8')
        value = kVPair[1].decode('UTF-8')
        #print('Key: ', key)
        #print('Value: ', value)
        file.write(key+'\n')
        file.write(value+'\n')
        file.write('\n')
    file.close()
    print('Elapsed time: ', elapsedTimeMilli)

    # close the database
    try:
        database.close()
        if dbType==3:
            database2.close()
    except Exception as e:
        print(e)

def binarySearch(db, key):
    l = 0
    h = len(db)-1
    while l<h:
        m = (l+h)//2
        if key == db[m]:
            return m
        elif key < db[m]:
            h = m-1
            result = h
        else:
            l = m+1
            result = l
    return result

def destroyDatabase(dbType,mode):
    ## call DB--> remove()
    if (not databaseExist(dbType) and mode==0):
        print('Database not exist, please select 1 to populate a new database')
        return

    try:
        if dbType == 1:
            call(["rm","-r","./tmp/zhaorui_db/berkeley_db_btree"])
        if dbType == 2:
            call(["rm","-r","./tmp/zhaorui_db/berkeley_db_hash"])
        if dbType == 3:                        
            call(["rm","-r","./tmp/zhaorui_db/berkeley_db_secindex"])
            call(["rm","-r","./tmp/zhaorui_db/berkeley_db_index"])
    except IOError:
        print('Cannot find the file')

def main():
    # create path
    if not os.path.exists(PATH_DIRECTORY):
        os.makedirs(PATH_DIRECTORY)

    try:
        dbTypeChoice = sys.argv[1].lower()
    except IndexError:
        print('You should specify which type of database to use')
        return

    dbType = 0
    while not dbType:
        dbType = {
            "btree" : 1,
            "hash" : 2,
            "index":3
        }.get(dbTypeChoice, 0)
    if not dbTypeChoice:
        print('database type error')
        return

    while True:
        selected = 0
        print('''
            ------------------------------------------
            1. Create and populate a database
            2. Retrieve records with a given key
            3. Retrieve records with a given data
            4. Retrieve records with a given range of key values
            5. Destroy the database
            6. Quit
            ------------------------------------------
            ''')

        while not (selected=='1' or selected=='2' or selected=='3' or selected=='4' or selected=='5' or selected=='6'):
            selected = input('Please select the program: ')
            
        if selected == '1':
            createPopulateDatabase(dbType)
        elif selected == '2':
            retrieveWithKey(dbType)
        elif selected == '3':
            retrieveWithData(dbType)
        elif selected == '4':
            retrieveWithRange(dbType)
        elif selected == '5':
            destroyDatabase(dbType,0)
        elif selected == '6':
            destroyDatabase(dbType,1)
            call(["rm","-r","answers"])
            break

    print('See you~')

if __name__ == '__main__':
    main()

