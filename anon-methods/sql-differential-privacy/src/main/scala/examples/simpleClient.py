# Author: Rohan
# Date: 12-11-2018

""" client.py pastes some data to a url and sends it to the server.
It also receives the parameters in the url extracted by the server
and prints them in JSON along with a HTTP response code
"""

import json

import requests

# Static data in JSON for testing
# Client sends this data in url
data = {
    'query': 'SELECT count(account_id) FROM accounts',
    'epsilon': '1.0',
    'budget': '2.0',
    'sid': '951923924'
}

# Localhost url
url = 'http://127.0.0.1:5890/data'

# Client sends Get request
session = requests.Session()

# Exception handling in case exception occurs while connecting to server
try:

    response = session.get(url, params=json.dumps(data))

    # Client prints the data returned by the server
    resp = response.json()
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
