import pprint
from gdaAttack import gdaAttack
from gdaTools import setupGdaAttackParameters

# This script makes attack queries, and then requests the
# resulting GDA score. 

pp = pprint.PrettyPrinter(indent=4)

config = {
    "configVersion": "compact1",
    "basic": {
        "attackType": "Test Attack",
        "criteria": "singlingOut"
    },
    'anonTypes': [
        ["diffix","latest"]
    ],
    'tables': [ ['banking','accounts'] ]
}

paramsList = setupGdaAttackParameters(config)
params = paramsList[0]
x = gdaAttack(params)

# Launch queries
query = dict(myTag='query1')
# Note error in this sql
sql = """select frequency, count(*)
         from accounts
         where cli_district_id in (5,6,7)
         group by 1"""
query['sql'] = sql
x.askAttack(query)

query = dict(myTag='query2')
sql = """select avg(account_id)
         from accounts
         where account_id between 0 and 10000"""
query['sql'] = sql
x.askAttack(query)

# And gather up the answers:
while True:
    reply = x.getAttack()
    # print the first row only
    print(f"myTag is {reply['query']['myTag']}")
    if 'error' in reply:
        print(reply['error'])
    else:
        print(reply['answer'][0])
    if reply['stillToCome'] == 0:
        break

score = x.getResults()
pp.pprint(score)
x.cleanUp()
