import pprint
from common.gdaScore import gdaAttack

# This script makes prior knowledge queries, and then requests the
# resulting GDA score. 

pp = pprint.PrettyPrinter(indent=4)

params = dict(name='exampleKnowledge1',
              rawDb='localBankingRaw',
              anonDb='cloakBanking',
              criteria='singlingOut',
              table='accounts',
              flushCache=False,
              verbose=False)
x = gdaAttack(params)

# Launch queries
query = dict(myTag='query1')
sql = """select client_id, frequency, disp_type
         from accounts
         where right(md5(cast(client_id as text)),2) IN ('00','01')"""
query['sql'] = sql
x.askKnowledge(query)

query = dict(myTag='query2')
sql = """select client_id, birth_number, cli_district_id, lastname
         from accounts
         where right(md5(cast(client_id as text)),1) IN ('0')"""
query['sql'] = sql
x.askKnowledge(query)

# And gather up the answers:
while True:
    reply = x.getKnowledge()
    pp.pprint(reply)
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
