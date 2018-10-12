# Author: Rohan
# Date: 11-10-2018

""" server.py listens for incoming requests from the client and extracts the parameters from the url sent by the client.
It then writes the extracted parameters to a file in JSON format and sends the extracted file back to the client.
"""

import json
import time

from flask import Flask, request
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)


# Method to write url parameters in JSON to a file
def write_file(response):
    time_stamp = str(time.strftime("%Y-%m-%d_%H-%M-%S"))  # timeStamp stores current data and time to append to filename
    with open('data' + time_stamp + '.json', 'w') as outfile:
        json.dump(response, outfile)


""" Server method to handle incoming data.
Calls writeFile method to write the url parameters in JSON to a file.
Returns the same parameters as response to client.
"""


class GetParams(Resource):
    def get(self):
        response = json.loads(list(dict(request.args).keys())[0])  # Stores the response in JSON format
        write_file(response)
        return response


api.add_resource(GetParams, '/data')  # Route for GetJSON()

if __name__ == '__main__':
    app.run(port='5002')
