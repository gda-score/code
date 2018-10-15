# Author: Rohan
# Date: 11-10-2018

""" client.py pastes some data to a url and sends it to the server.
It also receives the parameters in the url extracted by the server
and prints them in JSON along with a HTTP response code
"""

import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Static data in JSON for testing
# Client sends this data in url
data = {
    'name': 'John Doe',
    'age': 40,
    'children': ['Susie', 'Mike', 'Philip'],
    'address': {'Street': 'Park Avenue',
                'Zip': 123456
                }
}

# Localhost url
url = 'http://127.0.0.1:5000/data'

# Client sends Get request
# Retry connection attempts in case client is unable to connect with the server
# Each retry attempt will create a new Retry object with updated values, so they can be safely reused.
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)  # A backoff factor to apply between attempts after the second try
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)
# Exception handling in case exception occurs while connecting to server
try:
    resp = session.get(url, params=json.dumps(data))
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

# Client prints the data returned by the server in JSON
print(resp.json())

# Client prints the response code
print(resp)
