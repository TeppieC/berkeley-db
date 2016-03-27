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
import bsddb3 as bsddb
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

def createPopulateDatabase(mode):
	if mode==1:
		# create a b-tree database
		db = bsddb.btopen(DA_FILE+'btree')

	pass

def retrieveWithKey(mode):
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