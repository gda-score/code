#!/usr/bin/env python3

""" client.py pastes some data to a url and sends it to the server.
It also receives the parameters in the url extracted by the server
and prints them in JSON along with a HTTP response code
"""

import json
import requests
import pprint


url = 'https://db001.gda-score.org/ubertool'  # Server URL

sid = ''  # Initialize Session ID variable

# Make a Query list with count of number of times each query should be executed
querylist = [{'query': '', 'count': 1}, {'query': 'SELECT FREQUENCY, COUNT(*) FROM accounts GROUP BY 1', 'count': 1}]

session = requests.Session() # Client establishes a session


# For loops to send queries from querylist
for k in range(0, len(querylist)):
    for j in range(0, querylist[k]['count']):
        request = {}  # Set the request to empty initially

        # Exception handling in case exception occurs while connecting to server
        try:
            # When sid in querylist is Null it indicates start of a session
            # Client sends this data in url
            if not sid:

                request = {
                    'query': querylist[k]['query'],
                    'epsilon': '1.0',
                    'budget': '2.0',
                    'dbname': 'raw_banking',
                    'sid': ''
                }

            # If sid is not Null in querylist, extract the query from the querylist and put it in the 'query' field
            else:
                request = {
                    'query': querylist[k]['query'],
                    'epsilon': '1.0',
                    'budget': '2.0',
                    'dbname': 'raw_banking',
                    'sid': sid
                }

            # Client stores the response sent by the simpleServer.py
            response = session.get(url, params=json.dumps(request), verify=False)

            resp = response.json()  # Convert response sent by server to JSON

            pprint.pprint(resp)  # Client prints the data returned by the server
            sid = resp['Server Response']['Session ID']  # Set Session ID to value returned by server

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
