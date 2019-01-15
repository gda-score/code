
""" client.py pastes some data to a url and sends it to the server.
It also receives the parameters in the url extracted by the server
and prints them in JSON along with a HTTP response code
"""

import json

import requests

# Localhost url
# TODO: Change URL to server
url = 'http://127.0.0.1:5890/data'

# Client sends Get request
session = requests.Session()

# Exception handling in case exception occurs while connecting to server
try:

    # JSON payload to use for first request where 'sid' is set to Null
    # When sid is Null it indicates start of a session
    # Client sends this data in url
    first_request = {
        'query': 'SELECT count(account_id) FROM accounts',
        'epsilon': '1.0',
        'budget': '2.0',
        'dbname': 'raw_banking',
        'sid': ''
    }

    # Client stores the response sent by the simpleServer.py
    # For first request send 'first_request' in params
    # For all subsequent requests, change the query in 'subsequent_request' and put 'subsequent_request' in params
    response = session.get(url, params=json.dumps(first_request))

    # Client prints the data returned by the server
    resp = response.json()

    # Use this JSON Payload in all requests after first request
    subsequent_request = {
        'query': 'SELECT count(account_id) FROM accounts',
        'epsilon': '1.0',
        'budget': '2.0',
        'dbname': 'raw_banking',
        'sid': str(resp[1])
    }

    print("Noisy Result: " + str(resp[0]))
    print("Please put the Session ID in the JSON payload in subsequent requests.")
    print("Session ID: " + str(resp[1]))

    # Client prints the response code
    print(response)

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
