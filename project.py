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

# For sublime: check indent with spaces, and set indent as 4 spaces

# imports
from bsddb3 import db
import random
import os
import sys
import time
import shutil, subprocess

# set the path
PATH_DIRECTORY = "tmp/zhaorui_db/"
DA_FILE = PATH_DIRECTORY+"berkeley_db"

DB_SIZE = 100000
SEED = 10000000

# helper functions
def saveInFile(key,value):
	key = key.decode('UTF-8')
	value = key.decode('UTF-8')
	#########

def get_random():
    return random.randint(0, 63)

def get_random_char():
    return chr(97 + random.randint(0, 25))

def createPopulateDatabase(mode):
	if mode==1:
		# create a b-tree database
		database = db.DB()
		try:
			database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_CREATE)
		except:
			print('Error creating file')
		random.seed(SEED)
	elif mode==2:
		# create a hash database
		database = db.DB()
		try:
			database.open(DA_FILE+'_hash', None, db.DB_HASH, db.DB_CREATE)
		except:
			print('Error creating file')
		random.seed(SEED)
	elif mode==3:
		pass

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
        #print (key)
        #print (value)
        #print ("")
        # encoding
        key = key.encode(encoding='UTF-8')
        value = value.encode(encoding='UTF-8')
        database.put(key, value);

    print('Successfully populated the database')
    try:
    	database.close()
    except Exception as e:
    	print(e)


def retrieveWithKey(mode):
	database = db.DB()
	if mode == 1:
		DA_FILE = "/tmp/zhaorui_db/berkeley_db_btree"
		database.open(DA_FILE+'_btree', None, db.DB_BTREE, db.DB_RDONLY)
	elif mode==2:
		pass
	elif mode==3:
		pass

	key = input("Please input a valid key: ")
	startTime = time.time()
	try:
		value = db.get(key.encode(encoding='UTF-8'))
	except:
		print('Value not found')
	endTime = time.time()
	elapsedTimeMilli = 1000000*(endTime-startTime)
	value = value.decode(encoding='UTF-8')
	print("Retrieved value: %s"%value)
	print("Elapsed time: %d"%elapsedTimeMilli)

	# record in file
	file = open("answers", "a")
	file.write(key+'\n')
	file.write(value+'\n')
	file.write('\n')
	file.close()
	pass

def retrieveWithData(mode):
	pass

def retrieveWithRange(mode):
	pass

def destroyDatabase(mode):
	pass

def main():
	if not os.path.exists(PATH_DIRECTORY):
		os.makedirs(PATH_DIRECTORY)
	global OUTPUT_FILE = open("answers","w") # output file

	modeChoice = sys.argv[1].lower()
	mode = 0
	while not mode:
		mode = {
			"btree" : 1,
			"hash" : 2,
			"indexfile":3
		}.get(modeChoice, 0)
	if not modeChoice:
		print('database type error')
		return

    selected = 0
    while True:

	    print('''
    		------------------------------------------
    		1. Create and populate a database
        	2. Retrieve records with a given key
        	3. Retrieve records with a given data
        	4. Retrieve records with a given range of key values
        	5. Destroy the database
        	6. Quit
        	''')
    	selected = input('Please select the program: ')
    	
    	if selected == '1':
    		createPopulateDatabase(mode)
    	elif selected == '2':
    		retrieveWithKey(mode)
    	elif selected == '3':
    		retrieveWithData(mode)
    	elif selected == '4':
    		retrieveWithRange(mode)
    	elif selected == '5':
    		destroyDatabase(mode)
    		#TODO
    	elif selected == '6':
    		#TODO
    		break

    print('See you~')

if __main__ == '__main__':
	main()

## 1. what is a index file? how it works?
## 2. use of the new methods. How to open in "read and write"
## 3. how to reopen a database created before?
### Use a big trunk of main()
# invertedDA_FILE?