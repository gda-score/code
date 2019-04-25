import sys
import pprint
from common.gdaScore import gdaAttack, gdaScores
from .myUtilities import checkMatch

# Anon: None
# Attack: List DB contents
# Criteria: Linkability
# Database: Banking.Accounts

# -------------------------- subroutines ---------------------------

# -------------------------- body ---------------------------

pp = pprint.PrettyPrinter(indent=4)

verbose = 0
v = verbose
doCache = True

# Note in following that since there is no anonymization, the anonymized
# DB is the same as the raw DB
paramsGda = dict(name=__file__ + 'gdaScore',
              rawDb='gdaScoreBankingRawLink',
              anonDb='gdaScoreBankingRawLink',
              pubDb='gdaScoreBankingRawPub',
              criteria='linkability',
              table='accounts',
              flushCache=True,
              verbose=False)

params = paramsGda

# TEST ALL CORRECT (UID ONLY)

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# First lets just get a list of UIDs from the raw table

query = {}
sql = """select distinct client_id
         from accounts
         limit 10"""
query['sql'] = sql
x.askAttack(query)
replyCorrect = x.getAttack()
if v: pp.pprint(replyCorrect)

# -------------------  Claims Phase  ----------------------------

# the first askClaim will not commit to the claim
claim = False
for row in replyCorrect['answer']:
    spec = {'uid':'client_id',
            'guess':[{'col':'client_id','val':row[0]}]
           }
    x.askClaim(spec,claim=claim,cache=doCache)
    claim = True

while True:
    reply = x.getClaim()
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

print("\nTest all correct (just guess UID):")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
if v: pp.pprint(score)
expect = {'attackCells': 10,
          'attackGets': 1,
          'claimCorrect': 9,
          'claimError': 0,
          'claimMade': 9,
          'claimPassCorrect': 1,
          'claimTrials': 10,
          'knowledgeCells': 0,
          'knowledgeGets': 0
         }
checkMatch(score,expect,'client_id')
x.cleanUp()

# TEST SOME CORRECT (QUERY PUB TABLE ONLY)

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# Here I try a dumb attack where I just assume that the UIDs in the
# public table are also in the protected table

query = {}
sql = """select distinct client_id
         from accounts
         limit 50"""
query['sql'] = sql
query['db'] = 'pubDb'
x.askExplore(query)
reply = x.getExplore()
if v: pp.pprint(reply)

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------
# No attack phase, since I'm using only public knowledge

# -------------------  Claims Phase  ----------------------------

for row in reply['answer']:
    spec = {'uid':'client_id',
            'guess':[{'col':'client_id','val':row[0]}]
           }
    x.askClaim(spec,claim=True,cache=doCache)

# not sure how many of these will be correct, so lets count them and use
# that for the test result
claimCorrect = 0
while True:
    reply = x.getClaim()
    if reply['claimResult'] == 'Correct':
        claimCorrect += 1
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

print("\nTest some correct (query pub table only):")
if claimCorrect < 15 or claimCorrect > 35:
    # statistically very unlikely
    print(f"FAIL: Expected roughly 25 correct claims, got {claimCorrect}!")
    x.cleanUp()
    sys.exit()
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
if v: pp.pprint(score)
expect = {'attackCells': 0,
          'attackGets': 0,
          'claimCorrect': claimCorrect,
          'claimError': 0,
          'claimMade': 50,
          'claimPassCorrect': 0,
          'claimTrials': 50,
          'knowledgeCells': 0,
          'knowledgeGets': 0
         }
checkMatch(score,expect,'client_id')
x.cleanUp()

