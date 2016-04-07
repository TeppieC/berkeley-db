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
# to use: ./mydbtest [option]

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
        # create a secondary index database using btree
        database2 = db.DB()
        try:
            database.open(DA_FILE+'_index', None, db.DB_HASH, db.DB_CREATE)
            database2.set_flags(db.DB_DUP)
            database2.open(DA_FILE+'_secindex', None, db.DB_HASH, db.DB_CREATE)
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
        key = key.encode('UTF-8')
        value = value.encode('UTF-8')
        try:
            database.put(key, value)
        except:
            pass

        # set the value with a secondary index
        # in order to faster up in searching data
        if dbType==3:
            try:
                database2.put(value,key)
            except:
                pass

    # for test
    database.put(b'teppie',b'chen')
    database.put(b'nicholas',b'li')
    database.put(b'jiaxuan',b'yue')
    database.put(b'qwerty',b'abcdefghi')
    database.put(b'wasd',b'udlr')
    if dbType==3:
        database2.put(b'abcdefghi',b'qwerty')
        database2.put(b'chen',b'teppie')
        database2.put(b'li',b'nicholas')
        database2.put(b'yue',b'jiaxuan')
        database2.put(b'udlr',b'wasd')


    print('Successfully populated the database')
    try:
        database.close()
        if dbType==3:
            database2.close()
    except Exception as e:
        print(e)


def retrieveWithKey(dbType):
    print(''*70)
    print('****************Retrieve Data With Key**************')
    if not databaseExist(dbType):
        print('Database not exist, please select 1 to populate a new database')
        return

    database = db.DB()
    if dbType == 1:
        database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
    elif dbType==2:
        database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_RDONLY)
    elif dbType==3:
        database.open(DA_FILE+'_index', None, db.DB_HASH, db.DB_RDONLY)

    key = input("Please enter a valid key: ")
    startTime = time.time()
    try:
        value = database.get(key.encode('UTF-8'))
    except:
        print('Data not found')
        database.close()
        return
    endTime = time.time()
    elapsedTimeMilli = 1000000*(endTime-startTime)

    try:
        value = value.decode('UTF-8')
    except AttributeError:
        print('Data not found for this key in the database')
        database.close()
        return

    print("Retrieved data: %s"%value)
    print("Elapsed time: %d"%elapsedTimeMilli)

    # record in file
    file = open("answers", "a")
    file.write(key+'\n')
    file.write(value+'\n')
    file.write('\n')
    file.close()
    
    try:
        database.close()
    except Exception as e:
        print(e)

def retrieveWithData(dbType):
    print(''*70)
    print('****************Retrieve Key With Data**************')
    if not databaseExist(dbType):
        print('Database not exist, please select 1 to populate a new database')
        return

    database = db.DB()
    if dbType == 1:
        database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
    elif dbType==2:
        database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_RDONLY)
    elif dbType==3:
        database.open(DA_FILE+'_secindex', None, db.DB_HASH, db.DB_RDONLY)

    value = input("Please enter a data: ").encode('UTF-8')
    keys = []
    startTime = time.time()

    if dbType==1 or dbType==2: # if is btree or hash
        for key in database.keys():
            if database.get(key)==value:
                keys.append(key)
    else: # if is indexfile
        valueAsKey = value
        cur = database.cursor()

        iter = cur.first()
        for key in database.keys():
            if iter==valueAsKey:
                pass
            iter = cur.next()
        ''''
        valueAsKey = value
        try:
            for key in database.keys():
                if = database.get(valueAsKey)
            keys.append(keyAsValue)
        except:
            print('Key not found for the given data in the database')
            database.close()
            return
'''
    endTime = time.time()
    elapsedTimeMilli = 1000000*(endTime-startTime)
    # record in file
    file = open('answers', 'a')
    try:
        for key in keys:
            print("Retrieved key: "+ key.decode('UTF-8'))
            file.write(key.decode('UTF-8')+'\n')
            file.write(value.decode('UTF-8')+'\n')
            file.write('\n')
        file.close()
    except AttributeError:
        print('Key not found for the given data in the database.')

    print("Elapsed time: %d"%elapsedTimeMilli)
    # close the database
    try:
        database.close()
    except Exception as e:
        print(e)

def retrieveWithRange(dbType):
    print(''*70)
    print('***********Retrieve Key and Data With A Range of Key**********')
    if not databaseExist(dbType):
        print('Database not exist, please select 1 to populate a new database')
        return

    lowerBound = input('Please enter the lower bound of the range: ')
    upperBound = input('Please enter the upper bound of the range: ')
    while lowerBound>upperBound:
        print('Input invalid. Lower bound should not be higher than upper bound.')
        lowerBound = input('Please enter the lower bound of the range: ')
        upperBound = input('Please enter the upper bound of the range: ')

    lowerBound = lowerBound.encode('UTF-8')
    upperBound = upperBound.encode('UTF-8')

    database = db.DB()
    if dbType == 1:
        database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
    elif dbType==2:
        database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_RDONLY)
    elif dbType==3:
        database.open(DA_FILE+'_index', None, db.DB_HASH, db.DB_RDONLY)

    results = []
    if dbType==1 or dbType==2: # if is btree or hash
        startTime = time.time()
        for key in database.keys():
            if key>=lowerBound and key>=upperBound:
                results.append((key,database.get(key)))
        endTime = time.time()
        elapsedTime = 1000000*(endTime-startTime)

        if not results:
            print('No result found for the range of key in the database.')
            return
    else: # if is index file
        cur = database.cursor()
        iter = (cur.set_range(lowerBound))

        startTime = time.time()
        try:
            while not iter[0]>upperBound:
                results.append(iter)
                iter = (cur.next())
        except:
            print('No results obtained for the query')
            return

        endTime = time.time()
        elapsedTime = 1000000*(endTime-startTime)
        
    print('%d results have been obtained'%len(results))
    # record in file
    file = open('answers', 'a')
    print('Retrieved: ')
    for kVPair in results:
        key = kVPair[0].decode('UTF-8')
        data = kVPair[1].decode('UTF-8')
        #print('Key: ', key)
        #print('Data: ', data)
        file.write(key+'\n')
        file.write(data+'\n')
        file.write('\n')
    file.close()
    print('Elapsed time in microseconds: ', elapsedTime)

    # close the database
    try:
        database.close()
    except Exception as e:
        print(e)

def destroyDatabase(dbType,mode):
    ## call DB--> remove() to remove the database
    if (not databaseExist(dbType) and mode==0):
        print('Database not exist, please select 1 to populate a new database')
        return

    try:
        if dbType == 1:
            db.DB().remove("./tmp/zhaorui_db/berkeley_db_btree")
        if dbType == 2:
            db.DB().remove("./tmp/zhaorui_db/berkeley_db_hash")
        if dbType == 3:                        
            db.DB().remove("./tmp/zhaorui_db/berkeley_db_index")
            db.DB().remove("./tmp/zhaorui_db/berkeley_db_secindex")
    except:
        # if database has not been created
        pass

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
        print('-'*70)
        print(''*70)
        print('''
               Berkeley DB application for project2
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

# todo:
# have to make sure that no duplicate keys are inserted.
