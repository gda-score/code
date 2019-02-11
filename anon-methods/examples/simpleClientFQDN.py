#!/usr/bin/env python3

""" client.py pastes some data to a url and sends it to the server.
It also receives the parameters in the url extracted by the server
and prints them in JSON along with a HTTP response code
"""

import json

import requests

# Localhost url
# TODO: Change URL to server
url = 'https://db001.gda-score.org/ubertool'

# Client sends Get request
session = requests.Session()

# Exception handling in case exception occurs while connecting to server
try:

    # For first request use 'query': '' and 'sid': ''
    # When sid is Null it indicates start of a session
    # Client sends this data in url
    # In subsequent requests set the sid returned by the server
	# Use the sid returned by the server in subsequent requests if the request is from the same client
	# Set sid back to NULL when you want to start a new session
    first_request = {
        'query': '',
        'epsilon': '1.0',
        'budget': '3.0',
        'dbname': 'raw_banking',
        'sid': ''
    }

    # Client stores the response sent by the simpleServer.py
    # For first request send 'first_request' in params
    # For all subsequent requests, change the query in 'subsequent_request' and put 'subsequent_request' in params
    response = session.get(url, params=json.dumps(first_request), verify=False)

    # Client prints the data returned by the server
    resp = response.json()

    
    print("Query Result: " + str(resp[0]))    
    print("Session ID: " + str(resp[1]))
	print("Please put the Session ID in the JSON payload in subsequent requests.")

    # Client prints the response code
    print("Response Code: " + response)

except requests.ConnectionError as e:
    print("Connection Error. Make sure you are connected to Internet.")
    print(str(e))

except requests.Timeout as e:
    print("Timeout Error")
    print(str(e))

except requests.RequestException as e:
    print("General Error")
    print(str(e))

except KeyboardInterrupt:
    print("Program closed")
