# Overview

This repository is derived from Uber Differential Privacy Tool (https://github.com/uber/sql-differential-privacy) and also contains a Client-Server system written in Python for sending queries.
The files can be found under: code/anon-methods/sql-differential-privacy/src/main/scala/examples/

# Working with the system

In simpleClient.py -

1. simpleClient.py has two static JSON paylaods (`first_request` and `subsequent_request`).
2. a) For first request modify the fields `query`, `epsilon` and `budget` in `first_request` (Line 25-30). Leave `sid` as NULL. 
`sid = ' '` indicates that it is the start of a new session. 
2. b) Set this JSON payload by setting `response = session.get(url, params=json.dumps(first_request))` in Line 35.
3. a) For all subsequent requests modify just the `query` in the JSON payload `subsequent_request` (Line 41-46). 
3. b) Set this JSON payload by setting `response = session.get(url, params=json.dumps(subsequent_request))` in Line 35.

In simpleServer.py -
1. In `read_file()` method, Line 34, provide the path to where Uber Tool is creating the result (.txt) files.
2. In `write_file()` method, Line 54, provide the path to to sql-differential-privacy folder. This is where all JSON requests sent by simpleClient.py will be written to .json files. 
3. In `get()` method, Line 103, provide the path to to sql-differential-privacy folder. Same as above.


To work with ElasticSensitivityExample.scala - 
1. In Line 57, `val database = Schema.getDatabase("raw_banking")`, enter the database name from the schema depending on which DB to run queries on.
2. In Line 66, provide the path where simpleServer.py is generating the JSON files (should be same path as Line 54 in simpleServer.py). 
3. In Line 130,  provide the appropriate credentials in DB connection section (`val con_str`).

To work with QueryRewritingExample.scala - 
1. In Line 22, `val database = Schema.getDatabase("raw_banking")`, enter the database name from the schema depending on which DB to run queries on.
2. In Line 31, provide the path where simpleServer.py is generating the JSON files (should be same path as Line 54 in simpleServer.py). 
3. In Line 95,  provide the appropriate credentials in DB connection section (`val con_str`).


After these changes, run ElasticSensitivityExample.scala and/or QueryRewritingExample.scala scripts along with simpleServer.py.
Run the Client script to send query, budget, epsilon and sid values. 







