# Strategy 1:
# Step 1: Read all the tables from SQL server and store them appropriately in JSON format in disk by transformaing as required
# Step 2: Create the root key-pair documents(base table) using the mapping JSON config file and insert them into MongoDB using insertMany
# Step 3: Read the derived tables data on the disk and then transform them appropriately into the mapping_config JSON file
# Step 4: Create the additional data (list/doc objects) and update them accordingly into the MongoDB using the updateOne cluase

# Pros and cons:


import json
import os

from pymongo import MongoClient
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

cores = multiprocessing.cpu_count()

user_input = \
    {
    "mongodb_user":"admin",
    "mongodb_password":"passwordone",
    "mapping_config_file_name":"mapping_config.json",
    "mapping_config_file_path":""
    }


# Function to create a MongoDB connection
def get_mongo_database(mongodb_host="localhost",
                       mongodb_port="27018",
                       mongodb_user="admin",
                       mongodb_password=""):

  # Provide the localhost using pymongo
  #CONNECTION_STRING = "mongodb://"+mongodb_user+":"+mongodb_password+"@"+mongodb_host+":"+mongodb_port

  # Connection to the replica set at ports 27018, 27019, 27020
  CONNECTION_STRING = "mongodb://"+mongodb_user+":"+mongodb_password+"@"+"localhost:27018,localhost:27019,localhost:27020/?replicaSet=rs0"

  # Using the srv connection string
  #CONNECTION_STRING = "mongodb+srv://"+mongodb_user+":"+mongodb_password+"@"+"myatlasclusteredu.r3vvbic.mongodb.net/"

  # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
  client = MongoClient(CONNECTION_STRING,w="majority",retryWrites=True)
  return client


def read_json_file(json_file_name):
    # Read the config.json file to get the mapping
    with open(json_file_name, 'r') as f:
        return json.load(f)


# Read the config.json file to get the mapping
mapping_config = read_json_file(user_input.get("mapping_config_file_name"))

# Print the contents of the mapping_config dictionary

db_name = mapping_config.get('db_name')
collection_name = mapping_config.get('collection_name')
document_structure = mapping_config.get('document_structure')

client = get_mongo_database(mongodb_user=user_input.get('mongodb_user'),
                            mongodb_password=user_input.get('mongodb_password'))
mongodb_database = client[db_name]

base_tables = ['departments','employees']
derived_tables = ['dept_manager','titles','dept_emp','salaries']

# Reading the base tables into a dictionary
base_table_dict = {}
for base_table in base_tables:
    base_table_dict[base_table] = read_json_file(base_table+".json")

# Create a new dictionary to store the filtered data
new_data = {}
base_table_to_be_used = ''
# Insert the base data first into the root elements of the JSON
for key, value in document_structure.get('employees').items():
    if not isinstance(value, dict) and not isinstance(value, list) and value != 'ObjectId':
        new_data[key] = value
        base_table_to_be_used = value.split('.')[1]


mongodb_docs = []

# TODO: Convert this document creation operation to be multi-threaded
# TODO: Split the dictionary to chunks and then use multi-thread to create the list of docs or
#  use the individual threaded output to be inserted parallely

print("Inserting the base table data into the MongoDb collection - "+collection_name)
print("Prepping the data into document format...")
for prim_key,value_dict in tqdm(base_table_dict[base_table_to_be_used].items()):
    insert_doc = {}
    for key, value in new_data.items():
        insert_doc[key] = value_dict.get(value.split('.')[-1])

    # Add a new row using loc[]
    mongodb_docs.append(insert_doc)

# Insert the base data into the MongoDB collection, Get a reference to the collection
collection = mongodb_database[collection_name]

# Dropping the collection if it already exists
collection.drop()
# insert the documents into the collection
print("Inserting "+str(len(mongodb_docs))+" into the collection: "+collection_name)
collection.insert_many(mongodb_docs)
print("Success...!!")

# Create Index on the Primary key
# TODO: get the primary key to be used info generalistically
primary_key = 'emp_no'
print("Creating the index on the field: "+primary_key)
collection.create_index(primary_key,unique=True)
print("Success...!!")

dir_path = 'chunks'
try:
    os.mkdir(dir_path)
    print(f"Directory '{dir_path}' created successfully.")
except FileExistsError:
    print(f"Directory '{dir_path}' already exists.")
except OSError as e:
    print(f"Error creating directory '{dir_path}': {e}")

# Working on the derived tables
for derived_table in derived_tables:
    continue
    print("Working on the derived table: "+derived_table)

    dir_path = 'chunks/'+derived_table+'/'

    try:
        os.mkdir(dir_path)
        print(f"Directory '{dir_path}' created successfully.")
    except FileExistsError:
        print(f"Directory '{dir_path}' already exists.")
    except OSError as e:
        print(f"Error creating directory '{dir_path}': {e}")

    derived_table_list = read_json_file(derived_table + ".json")
    # chinking the data into chunks of 100,000
    chunk_size = 100000
    total_chunks = len(derived_table_list)//chunk_size

    # Figure out the structure to be added into the MongoDb doc using the document structure
    insert_derived_doc_structure = document_structure.get(collection_name).get(derived_table)

    if isinstance(insert_derived_doc_structure,list):
        insert_derived_doc_structure = insert_derived_doc_structure[0]

    columns_needed = []
    for key, value in insert_derived_doc_structure.items():
        # TODO: Build a reference sheet for looking up the column names from referenced tables
        columns_needed.append(value.split('.')[-1])

    for chunk_num in range(0,total_chunks+1):
        if chunk_num!=total_chunks:
            df_data = derived_table_list[chunk_num*chunk_size:((chunk_num+1)*chunk_size)-1]
        else:
            df_data = derived_table_list[(total_chunks) * chunk_size:len(derived_table_list)]

        # Saves the RAM from getting overloaded with data
        df = pd.DataFrame(df_data)
        df = df[columns_needed+[primary_key]]
        # Get this primary key and then generalise this
        grp_df = df.groupby([primary_key])

        # define the function to apply to each group
        def process_group(gr, df):
            new_doc = {}
            df = df[columns_needed]  # -> dict
            # convert dataframe to list of dicts
            new_doc[primary_key] = gr[0]
            new_doc[derived_table] = df.to_dict(orient='records')

            return new_doc

        # create a ThreadPoolExecutor with max_workers threads
        executor = ThreadPoolExecutor(max_workers=cores)

        # submit a task to the executor for each group
        tasks = [executor.submit(process_group, gr, df) for gr, df in tqdm(grp_df)]

        # get the results from the tasks
        derived_table_chunk_list = [task.result() for task in tasks]
        print("Processed  chunk number: "+str(chunk_num))
        print(len(derived_table_chunk_list))

        with open(dir_path+str(derived_table)+"_chunk_"+str(chunk_num)+'.txt', 'w') as f:
            for my_dict in derived_table_chunk_list:
                my_dict_str = json.dumps(my_dict)
                f.write(my_dict_str + '\n')


for derived_table in derived_tables:
    print("Inserting the data from the "+derived_table+" ...")
    dir_path = 'chunks/' + derived_table + '/'
    for file_name in os.listdir(dir_path):

        #if 'chunk_0.' in file_name or 'chunk_1.' in file_name:
        #    continue
        print("Reading from " + file_name)
        my_list_of_dicts = []
        with open(dir_path+file_name, 'r') as f:
            for line in f:
                my_dict_str = line.strip()
                my_dict = json.loads(my_dict_str)
                my_list_of_dicts.append(my_dict)

        # define the function to apply to each group
        def process_group(dict, derived_table):
            query={primary_key:dict.get(primary_key)}
            new_obj = dict.get(derived_table)
            response = collection.update_one(
                filter = query,
                update = {"$push": {derived_table: {"$each": new_obj}}}
            )
            return response

        # create a ThreadPoolExecutor with max_workers threads
        executor = ThreadPoolExecutor(max_workers=cores)

        # submit a task to the executor for each group
        tasks = [executor.submit(process_group, dict, derived_table) for dict in tqdm(my_list_of_dicts)]

        # get the results from the tasks
        responses_list = [task.result() for task in tasks]



# Read data from the SQL server and make the transformations to base and derived data
# Create localhost mongoDB server and insert the base data
# Update the data in the local host mongoDB server with the derived data to manufacture the documents
# Take mongodump of the data base and use mongorestore on the data base to the actual productional servers - such as Atlas replica sets


# mongodump --host localhost --port 27019 --db migration_db --collection employees --out /Users/gouravsb/Desktop/tradecraft_missions
# mongorestore --uri mongodb+srv://student:passwordone@myatlasclusteredu.r3vvbic.mongodb.net --authenticationDatabase admin --nsInclude migration_db.employees /Users/gouravsb/Desktop/tradecraft_missions/migration_db/employees.bson
