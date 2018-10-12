# Author: Rohan
# Date: 11-10-2018

""" client.py pastes some data to a url and sends it to the server.
It also receives the parameters in the url extracted by the server
and prints them in JSON along with a HTTP response code
"""

import json

import requests

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
url = 'http://127.0.0.1:5002/data'

# Client sends Get request
resp = requests.get(url, params=json.dumps(data))

# Client prints the data returned by the server in JSON
print(resp.json())

# Client prints the response code
print(resp)
