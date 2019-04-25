import pprint
from common.gdaScore import gdaAttack, gdaScores
from common.gdaUtilities import setupGdaAttackParameters
from attacks.examples.myUtilities import checkMatch

# Anon: None
# Attack: List DB contents
# Criteria: Inference
# Database: Banking.Accounts

pp = pprint.PrettyPrinter(indent=4)
verbose = 0
v = verbose

# -------------------------- subroutines ---------------------------

# -------------------------- body ---------------------------

# Note in following that since there is no anonymization, the anonymized
# DB is the same as the raw DB

config = {
    "configVersion": "compact1",
    "basic": {
        "attackType": "Test",
        "criteria": "inference"
    },
    'anonTypes': [ ['no_anon'] ],
    'tables': [ ['banking','accounts'] ]
}

paramsList = setupGdaAttackParameters(config)
params = paramsList[0]
pp.pprint(params)
# TEST ALL CORRECT

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# In the List attack for Inference, we look for columns where there
# is only a single distinct instance of another column. In this case,
# acct_date has relatively many distinct values, and frequency has
# relatively few, so it isn't hard to find instances where users with
# different acct_date all have the same frequency. Note that rather
# than acct_date, we could have used several columns each with fewer
# distinct values.

query = {}
sql = """select acct_date, max(frequency)
         from accounts
         group by 1
         having count(distinct frequency) = 1
         limit 10"""
query['sql'] = sql
x.askAttack(query)
replyCorrect = x.getAttack()

# -------------------  Claims Phase  ----------------------------

# the first askClaim will pass on committing to the claim
claim = 0
for row in replyCorrect['answer']:
    spec = {'known':[{'col':'acct_date','val':row[0]}],
            'guess':[{'col':'frequency','val':row[1]}]
           }
    x.askClaim(spec,claim=claim)
    claim = 1

while True:
    reply = x.getClaim()
    if v: print(f"Query: {reply['query']['sql']}")
    if v: print(f"Result: {reply['claimResult']}")
    if reply['stillToCome'] == 0:
        break

# -------------------  Scores Phase  ----------------------------

print("Test all correct:")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
pp.pprint(score['col']['frequency'])
expect = {'attackCells': 20,
          'attackGets': 1,
          'claimCorrect': 9,
          'claimError': 0,
          'claimMade': 9,
          'claimPassCorrect': 1,
          'claimTrials': 10,
          'knowledgeCells': 0,
          'knowledgeGets': 0
         }
checkMatch(score,expect,'frequency')
x.cleanUp(doExit=False)

# TEST ALL WRONG

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

query = {}
sql = """select acct_date, max(frequency)
         from accounts
         group by 1
         having count(distinct frequency) > 1
         limit 10"""
query['sql'] = sql
x.askAttack(query)
replyWrong = x.getAttack()

# -------------------  Claims Phase  ----------------------------

claim = 0
for row in replyWrong['answer']:
    spec = {'known':[{'col':'acct_date','val':row[0]}],
            'guess':[{'col':'frequency','val':row[1]}]
           }
    x.askClaim(spec,claim=claim)
    claim = 1

while True:
    reply = x.getClaim()
    if v: print(f"Query: {reply['query']['sql']}")
    if v: print(f"Result: {reply['claimResult']}")
    if reply['stillToCome'] == 0:
        break

# -------------------  Scores Phase  ----------------------------

print("Test all wrong:")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
pp.pprint(score['col']['frequency'])
expect = {'attackCells': 20,
          'attackGets': 1,
          'claimCorrect': 0,
          'claimError': 0,
          'claimMade': 9,
          'claimPassCorrect': 0,
          'claimTrials': 10,
          'knowledgeCells': 0,
          'knowledgeGets': 0
         }
checkMatch(score,expect,'frequency')
x.cleanUp(doExit=False)

# TEST MIX CORRECT and WRONG

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# Use prior attack phases

# -------------------  Claims Phase  ----------------------------

claim = 0
for row in replyWrong['answer']:
    spec = {'known':[{'col':'acct_date','val':row[0]}],
            'guess':[{'col':'frequency','val':row[1]}]
           }
    x.askClaim(spec,claim=claim)
    claim = 1

claim = 0
for row in replyCorrect['answer']:
    spec = {'known':[{'col':'acct_date','val':row[0]}],
            'guess':[{'col':'frequency','val':row[1]}]
           }
    x.askClaim(spec,claim=claim)
    claim = 1

while True:
    reply = x.getClaim()
    if v: print(f"Query: {reply['query']['sql']}")
    if v: print(f"Result: {reply['claimResult']}")
    if reply['stillToCome'] == 0:
        break

# -------------------  Scores Phase  ----------------------------

print("Test mix of correct and wrong:")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
pp.pprint(score['col']['frequency'])
expect = {'attackCells': 0,
          'attackGets': 0,
          'claimCorrect': 9,
          'claimError': 0,
          'claimMade': 18,
          'claimPassCorrect': 1,
          'claimTrials': 20,
          'knowledgeCells': 0,
          'knowledgeGets': 0
         }
checkMatch(score,expect,'frequency')
print("\nOperational Parameters:")
op = x.getOpParameters()
pp.pprint(op)
x.cleanUp()
