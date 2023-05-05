# Import the required libraries
import os
import math
import openai
import mysql.connector
from pymongo import MongoClient
from transformers import GPT2TokenizerFast
from xml.etree.ElementTree import Element, SubElement, tostring

import xml.dom.minidom

# Set an environment variable for tokenizer
os.environ['TOKENIZERS_PARALLELISM'] = "false"

# Set the OpenAI API key
openai.api_key = 'ENTER YOUR OPENAI API KEY'
# Choose the demo to run
demo = input('Demo 1: Mapping the columns from SQL DB(multiple tables) to MongoDB without any collection already in MongoDB\n'
      'Demo 2: Mapping the columns from SQL DB(multiple tables) to MongoDB with collections already in Atlas with different key names\n'
      'Demo 3: Mapping the columns from one SQL table to MongoDB collection\n'
      'Demo 4: Convert the exisiting entity relational XML file to MongoDB schema\n'
      'Which demo do u want to run?  ')
print('\n')
demo = int(demo)
GPT_model = "text-davinci-003"
demo_dict = {
  1:{"sql_get_all_tables": True,"sql_user": "root","sql_password":"passwordone",
     "sql_host":"127.0.0.1","sql_db_name":"employees"},
  2:{"sql_get_all_tables": True,"sql_user": "root","sql_password":"passwordone",
     "sql_host":"127.0.0.1","sql_db_name":"employees",
     "mongodb_db":"test","mongodb_user" : "student", "mongodb_password" : "passwordone"},
  3:{"sql_get_all_tables": False,"sql_user": "root","sql_password":"passwordone",
     "sql_host":"127.0.0.1","sql_db_name":"world","sql_table_name":"country","sql_top_row_count":5,
     "mongodb_keys":'[regionID, surfaceArea, indepYear, pop, expectedLifeYears, countryCode, '
                    'countryCode2, countryName, continentName, GNP, GNPOld, localName, formGov, '
                    'HeadOfState, capitalCity]'},
  4:{"sql_get_all_tables": True,"sql_user": "root","sql_password":"passwordone",
     "sql_host":"127.0.0.1","sql_port":3306,"sql_db_name":"employees","sql_top_row_count":1}}
#mongodb_keys=''#'[_id, country_name, country_code,dist, pop]'


def generate_erd_xml_format(hostname, port, user, password, database_name):
  try:
    # establish connection to MySQL database
    connection = mysql.connector.connect(
      host=hostname,
      port=port,
      database=database_name,
      user=user,
      password=password
    )

    # get database schema information
    # table_type != 'VIEW'
    cursor = connection.cursor()
    cursor.execute("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s",
                   (database_name,))
    rows_basic = cursor.fetchall()

    cursor.execute("SHOW FULL TABLES WHERE table_type != 'VIEW'")

    # Fetch only table names
    result = cursor.fetchall()
    tables = [res[0] for res in result]

    cursor.execute("""SELECT
          table_name, 
          column_name, 
          referenced_table_name, 
          referenced_column_name,
          constraint_name
      FROM 
          INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE 
          CONSTRAINT_SCHEMA = %s
          AND REFERENCED_TABLE_NAME IS NOT NULL;
      """, (database_name,))

    rows = cursor.fetchall()

    # create XML document
    # <?xml version="1.0" encoding="UTF-8"?>
    diagram = Element('diagram', {'version': '1', 'name': database_name + '_ERD'})
    entities = SubElement(diagram, 'entities')
    relations = SubElement(diagram, 'relations')
    data_source = SubElement(entities, 'data-source', {'id': 'mysql'})

    # group columns by table name
    basic_table_cols = {}
    table_id_dict = {}
    iter = 0
    for row in rows_basic:
      if row[0] not in tables:
        continue
      if row[0] not in basic_table_cols:
        iter += 1
        basic_table_cols[row[0]] = []
      basic_table_cols[row[0]].append(row[1:])
      table_id_dict[row[0]] = str(iter)

    table_keys_info = {}
    for iter, row in enumerate(rows):
      if row[0] not in table_keys_info:
        table_keys_info[row[0]] = []
      table_keys_info[row[0]].append(row[1:])

    # create entity elements for each table
    for table, cols in basic_table_cols.items():
      entity = SubElement(data_source, 'entity',
                          {'id': str(table_id_dict[table]), 'table-name': table,
                           #'fq-name': database_name + '.' + table
                           })
      #path = SubElement(entity, 'path', {'name': database_name})
      for col in cols:
        type = 'string' if 'char' in str(col[1])[2:-1] else str(col[1])[2:-1]
        #column = SubElement(entity, 'column', {'name':str(col[0]),
        #                                       'type': type,
        #                                       })

      for col in table_keys_info.get(table, []):
        type = 'fk' if 'fk_' in str(col[3]) or '_fk' in str(col[3]) else 'pk'
        relation = SubElement(relations, 'relation',
                              {'name': str(col[3]),
                               #'fq-name': database_name + '.' + table + '.' + str(col[3]),
                               'type': type,
                               'pk-ref-table-id': table_id_dict[str(col[1])],
                               'pk-ref-column-name': str(col[2]),
                               'fk-ref-table-id': table_id_dict[table],
                               'fk-ref-column-name': str(col[0])})

    # convert XML document to string and return
    return tostring(diagram)

  except mysql.connector.Error as e:
    print('Error:', e)

  finally:
    if connection.is_connected():
      cursor.close()
      connection.close()


def calculate_gpt_cost(prompt):
  tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
  # Get the length of the prompt in tokens
  prompt_length = len(tokenizer.encode(prompt))

  # Calculate the number of tokens in 1K tokens
  tokens_per_1k = 1000

  # Calculate the number of 1K tokens needed for the prompt
  num_1k_tokens = int(math.ceil(prompt_length / tokens_per_1k))

  # Calculate the cost of using chat GPT for the prompt
  cost_per_1k = 0.002
  total_cost = cost_per_1k * num_1k_tokens

  return total_cost


def get_mongo_database(mongodb_host="localhost",mongodb_port="27017",mongodb_user="admin",mongodb_password="passwordone"):

  # Provide the localhost using pymongo
  #CONNECTION_STRING = "mongodb://"+mongodb_user+":"+mongodb_password+"@"+mongodb_host+":"+mongodb_port
  # Using the srv connection string
  CONNECTION_STRING = "mongodb+srv://"+mongodb_user+":"+mongodb_password+"@"+"myatlasclusteredu.r3vvbic.mongodb.net/?retryWrites=true&w=majority"

  # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
  client = MongoClient(CONNECTION_STRING)

  return client


def get_mongo_collection_details(client, collection_name):
  collection_name = client[collection_name]
  item_details = collection_name.find()
  for item in item_details:
    return(list(filter(lambda x:x!='_id',list(item.keys()))))
  return []


def get_SQL_connection(sql_host,sql_user,sql_password,sql_db_name):
  # Connect to the MySQL server - using the values entered from the UI
  cnx = mysql.connector.connect(
    host=sql_host,
    user=sql_user,
    password=sql_password,
    database=sql_db_name
  )
  return cnx


def generate_chatGPT_text(prompt, model, max_length=256, temperature=0.7):
  """
  Generates text using the OpenAI GPT API.
  Args:
      prompt (str): The prompt text to generate the text from.
      model (str): The ID of the GPT model to use.
      max_length (int): The maximum length of the generated text (default: 100).
      temperature (float): The sampling temperature to use when generating text (default: 0.5).
  Returns:
      A string containing the generated text.
  """
  response = openai.Completion.create(
    engine=model,
    prompt=prompt,
    max_tokens=max_length,
    temperature=temperature,
    top_p=1,
    frequency_penalty=0,
    presence_penalty=0
  )
  text = response.choices[0].text.strip()
  return text


# Running the requested demo
if demo_dict[demo].get("sql_get_all_tables") and demo in [1,2]:
  if demo==1:
    print("Demo 1: Mapping the columns from SQL DB(multiple tables) to MongoDB without any collection already in MongoDB\n")
  else:
    print("Demo 2: Mapping the columns from SQL DB(multiple tables) to MongoDB with collections already in Atlas with different key names\n")

  cnx = get_SQL_connection(demo_dict[demo].get("sql_host"),
                           demo_dict[demo].get("sql_user"),
                           demo_dict[demo].get("sql_password"),
                           demo_dict[demo].get("sql_db_name"))
  mycursor = cnx.cursor()
  mycursor.execute("SHOW FULL TABLES WHERE table_type != 'VIEW'")

  # Fetch all table names
  result = mycursor.fetchall()
  tables = [res[0] for res in result]

  sql_top_row_count = 1
  sql_db_tables_details = ''
  for sql_table_name in tables:
    # Demo 1: Without any collections stored already in the MongoDB use this:
    if demo==1:
      collection_keys = []

    if demo==2:
      # Demo 2: With collections already stored in the MongoDB use this:
      client = get_mongo_database(mongodb_user=demo_dict[demo].get('mongodb_user'),
                            mongodb_password=demo_dict[demo].get('mongodb_password'))
      mongodb_database = client[demo_dict[demo].get('mongodb_db')]

      collection_keys = get_mongo_collection_details(mongodb_database, sql_table_name)

    #TODO: Bring the Primary and Foreign key info from here
    #mycursor.execute("DESCRIBE "+str(sql_table_name))
    #result = mycursor.fetchall()

    # Execute a SELECT query to retrieve data from the test_table
    mycursor.execute("SELECT * FROM " + str(sql_table_name) + " LIMIT " + str(sql_top_row_count))

    # Fetch all rows of data from the query result
    result = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]

    if len(collection_keys)==0:
      collection_keys = columns

    columns_str = ''
    for col in columns:
      columns_str += col + ','
    columns_str = columns_str[:-1]

    # Get sql_top_row_count rows of data
    row_str = '['
    for row in result[0:sql_top_row_count]:
      row_str = row_str + '['
      for element in row:
        row_str += str(element) + ','
      row_str = row_str[:-1]
      row_str += ']'
    row_str += ']'

    sql_db_tables_details += "Table-"+sql_table_name+"\nColumns-["+columns_str+"]\nRow 1-"+row_str+"\nMongoDB keys-"+str(collection_keys).replace("'",'')+"\n"

  prompt = "Given this SQL database with "+str(len(tables))+ " tables- "+str(tables)+\
           "\n transform it to MongoDB database with "+str(len(tables))+\
           " collections & only return mappings of each collection in JSON format. \n"+\
           "For example-{'col1':'key1'} where col1 is from the columns of SQL table,"+\
           "key1 is from the collection in MongoDB. \n"+\
           "Details about SQL tables:\n"+sql_db_tables_details
  print(prompt)
  print("\nNumber of words in the prompt ~ " + str(len(prompt.split(' '))))
  print("Cost of making this query to chatGPT servers = $" + str(calculate_gpt_cost(prompt)))
elif demo==3:
  # Demo 3:
  print("Demo 3: Mapping the columns from one SQL table to MongoDB collection\n")
  cnx = get_SQL_connection(demo_dict[demo].get("sql_host"),
                           demo_dict[demo].get("sql_user"),
                           demo_dict[demo].get("sql_password"),
                           demo_dict[demo].get("sql_db_name"))
  mycursor = cnx.cursor()

  # Execute a SELECT query to retrieve data from the test_table
  mycursor.execute("SELECT * FROM "+str(demo_dict[demo].get("sql_table_name"))+" LIMIT "+str(demo_dict[demo].get("sql_top_row_count")))

  # Fetch all rows of data from the query result
  result = mycursor.fetchall()
  columns = [col[0] for col in mycursor.description]

  columns_str = ''
  for col in columns:
    columns_str += col + ', '
  columns_str = columns_str[:-1]

  # Get sql_top_row_count rows of data
  row_str = '['
  for row in result[0:demo_dict[demo].get("sql_top_row_count")]:
    row_str = row_str + '['
    for element in row:
      row_str += str(element) + ', '
    row_str = row_str[:-1]
    row_str += ']\n'
  row_str = row_str[:-1]+']'

  prompt = "Given this SQL table- '" + str(demo_dict[demo].get("sql_table_name")) + "' from database- '" + str(demo_dict[demo].get("sql_db_name")) + \
           "' transform to MongoDB document & only return mapping of fields in JSON format. "
  prompt = prompt + "\nColumns in the table: [" + columns_str + "]. "
  prompt = prompt + "\nFirst " + str(demo_dict[demo].get("sql_top_row_count")) + " rows of table:\n" + row_str + ". "

  if demo_dict[demo].get("mongodb_keys") != '':
    prompt = prompt + "\nMongoDB keys are " + demo_dict[demo].get("mongodb_keys") + ". " \
                                                           "\nUsing this return a mapping between the SQL table fields and mongo keys in a JSON format."
  else:
    prompt = prompt + "MongoDB keys are same as the column names in the SQL table. " \
                      "Using this return a mapping between the SQL table fields and mongo keys in a JSON format."

  print(prompt)
  print("\nNumber of words in the prompt ~ "+str(len(prompt.split(' '))))
  print("Cost of making this query to chatGPT servers = $"+str(calculate_gpt_cost(prompt)))
elif demo==4:
  cnx = get_SQL_connection(demo_dict[demo].get("sql_host"),
                           demo_dict[demo].get("sql_user"),
                           demo_dict[demo].get("sql_password"),
                           demo_dict[demo].get("sql_db_name"))
  mycursor = cnx.cursor()
  mycursor.execute("SHOW FULL TABLES WHERE table_type != 'VIEW'")

  # Fetch all table names
  result = mycursor.fetchall()
  tables = [res[0] for res in result]

  sql_top_row_count = demo_dict[demo].get("sql_top_row_count")
  sql_db_tables_details = ''
  for sql_table_name in tables:
    # Execute a SELECT query to retrieve data from the test_table
    mycursor.execute("SELECT * FROM " + str(sql_table_name) + " LIMIT " + str(sql_top_row_count))

    # Fetch all rows of data from the query result
    result = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]

    columns_str = ''
    for col in columns:
      columns_str += col + ','
    columns_str = columns_str[:-1]

    # Get sql_top_row_count rows of data
    row_str = '['
    for row in result[0:sql_top_row_count]:
      row_str = row_str + '['
      for element in row:
        if len(str(element))>20:
          element = element[0:10]+'...'
        row_str += str(element) + ','
      row_str = row_str[:-1]
      row_str += ']'
    row_str += ']'

    sql_db_tables_details += "Table-" + sql_table_name + "\nColumns-[" + columns_str + "]\n"+\
                             " Row(s)" + row_str +"\n"

  xml_str = generate_erd_xml_format(demo_dict[demo].get("sql_host"),
                                    demo_dict[demo].get("sql_port"),
                                    demo_dict[demo].get("sql_user"),
                                    demo_dict[demo].get("sql_password"),
                                    demo_dict[demo].get("sql_db_name"))
  XML_doc = xml_str.decode('utf-8')

  dom = xml.dom.minidom.parseString(XML_doc)
  XML_str_pretty = dom.toprettyxml()

  prompt = """Following these general rules for MongoDB Schema Design:
                Rule 1: Favor embedding unless there is a compelling reason not to.
                Rule 2: Needing to access an object on its own is a compelling reason not to embed it.
                Rule 3: Avoid joins and lookups if possible, but don't be afraid if they can provide a better schema design.
                Rule 4: Arrays should not grow without bound. If there are more than a couple of hundred 
                documents on the many side, don't embed them; if there are more than a few thousand documents 
                on the many side, don't use an array of ObjectID references. High-cardinality arrays are a 
                compelling reason not to embed."""
  prompt = "\n Given ERD XML file: \n"+XML_str_pretty+ \
           "\n and details about the " + str(len(tables)) + " tables: \n" + sql_db_tables_details + \
           "\n generate a denormalized and efficient MongoDB schema (JSON format) only, do not give any example document?\n"
  #

  print(prompt)
  print("\nNumber of words in the prompt ~ " + str(len(prompt.split(' '))))
  print("Cost of making this query to chatGPT servers = $" + str(calculate_gpt_cost(prompt)))

text = generate_chatGPT_text(prompt, GPT_model, max_length=1000)
print(text)

if demo ==4:
  prompt = "Given this MongoDB Schema: "+text+" suggest indexes to optimise the queries."
  #print(prompt)
  #text = generate_chatGPT_text(prompt, GPT_model, max_length=1000)
  #print(text)

try:
  # Close the SQL connection if opened
  cnx.close()
  # Close the MongoDB connection if opened
  client.close()
except:
  pass
