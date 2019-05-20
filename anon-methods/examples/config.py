#!/usr/bin/python3

budget = '5000.0'  # Budget value
dbname = 'raw_banking'  # Database name
url = 'https://db001.gda-score.org/ubertool'  # Flask server url

"""
Send the first query as `NULL` with `'count': 1`.
Make separate `query`, `count` and `epsilon` key-value pairs for each subsequent query.
Send the first query as NULL with count 1 to get back just the Session ID (See `README.md` file for detailed guidelines).
VALID FORMAT: [{First Query: '', 'count': 1}, {Following Queries: 'Some Valid Query', 'count': x, 'epsilon': x}]
where `x` is the number of times you want to execute the query.
NOTE: Make a separate 'query' and 'count' key-value pair for EACH query.

Returns just the 'Session ID' for first query (NULL).
Following valid queries return the noisy result in 'Result' field.
Any invalid query will return an `Error` message.
"""

querylist = [{'query': '', 'count': 1},
             # ok {'query': 'Select count(distinct uid) from accounts', 'count': 1, 'epsilon': '2.0'},
            # ok {'query': 'Select count(*) from accounts', 'count': 1, 'epsilon': '2.0'},
            # bad {'query': 'Select sum(amount) from transactions', 'count': 1, 'epsilon': '2.0'},
            # bad {'query': 'Select sum(amount) from transactions', 'count': 1, 'epsilon': '2.0'},
            # ok {'query': 'Select count(*) from transactions', 'count': 1, 'epsilon': '2.0'},
            {"query": "Select count(*) from transactions WHERE operation = 'VKLAD'", "count": 1, "epsilon": "2.0"},
            ]
