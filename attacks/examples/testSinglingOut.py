import sys
import pprint
sys.path.append('../../common')
from gdaScore import gdaAttack, gdaScores
from myUtilities import checkMatch

# Anon: None
# Attack: List DB contents
# Criteria: Singling-out
# Database: Banking.Accounts

# -------------------------- subroutines ---------------------------

# -------------------------- body ---------------------------

pp = pprint.PrettyPrinter(indent=4)

verbose = 0
v = verbose
doCache = True

# Note in following that since there is no anonymization, the anonymized
# DB is the same as the raw DB
paramsLocal = dict(name=__file__,
              rawDb='localBankingRaw',
              anonDb='localBankingRaw',
              criteria='singlingOut',
              table='accounts',
              flushCache=False,
              verbose=False)

paramsGda = dict(name=__file__ + 'gdaScore',
              rawDb='gdaScoreBankingRaw',
              anonDb='gdaScoreBankingRaw',
              criteria='singlingOut',
              table='accounts',
              flushCache=True,
              verbose=False)

params = paramsLocal

# TEST ALL CORRECT (ONE GUESSED COLUMN)

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# The List attack, the simplest of attacks, simply requests a list
# of counts for a set of column values, where each count is equal
# to exactly one, and therefore has only a single uid.

query = {}
sql = """select acct_date, acct_district_id, frequency, count(*)
         from accounts
         group by 1,2,3
         having count(*) = 1
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
            'known':[{'col':'acct_date','val':row[0]},
                     {'col':'acct_district_id','val':row[1]}],
            'guess':[{'col':'frequency','val':row[2]}]
           }
    x.askClaim(spec,claim=claim,cache=doCache)
    claim = True

while True:
    reply = x.getClaim()
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

print("\nTest all correct (one guessed column):")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
#pp.pprint(score['col']['frequency'])
if v: pp.pprint(score)
expect = {'attackCells': 40,
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
x.cleanUp()

# TEST ALL CORRECT (TWO GUESSED COLUMNS)

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# The List attack, the simplest of attacks, simply requests a list
# of counts for a set of column values, where each count is equal
# to exactly one, and therefore has only a single uid.

query = {}
sql = """select acct_date, acct_district_id, frequency, count(*)
         from accounts
         group by 1,2,3
         having count(*) = 1
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
            'known':[{'col':'acct_date','val':row[0]}],
            'guess':[{'col':'frequency','val':row[2]},
                     {'col':'acct_district_id','val':row[1]}]
           }
    x.askClaim(spec,claim=claim,cache=doCache)
    claim = True

while True:
    reply = x.getClaim()
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

print("\nTest all correct (two guessed columns):")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
#pp.pprint(score['col']['frequency'])
if v: pp.pprint(score)
expect = {'attackCells': 40,
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
x.cleanUp()

# TEST ALL INCORRECT

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# The List attack, the simplest of attacks, simply requests a list
# of counts for a set of column values, where each count is equal
# to exactly one, and therefore has only a single uid.

query = {}
sql = """select acct_date, acct_district_id, frequency, count(*)
         from accounts
         group by 1,2,3
         having count(*) > 1
         limit 10"""
query['sql'] = sql
x.askAttack(query)
replyWrong = x.getAttack()
if v: pp.pprint(replyWrong)

# -------------------  Claims Phase  ----------------------------

claim = False
for row in replyWrong['answer']:
    spec = {'uid':'client_id',
            'known':[{'col':'acct_date','val':row[0]},
                     {'col':'acct_district_id','val':row[1]}],
            'guess':[{'col':'frequency','val':row[2]}]
           }
    x.askClaim(spec,claim=claim,cache=doCache)
    claim = True

while True:
    reply = x.getClaim()
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

# -------------------  Scores Phase  ----------------------------

print("\nTest all wrong:")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
#pp.pprint(score['col']['frequency'])
expect = {'attackCells': 40,
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
x.cleanUp()

# TEST MIX OF CORRECT and INCORRECT

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# This attack doesn't require any exploratory queries

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# Use prior attack phases

# -------------------  Claims Phase  ----------------------------

claim = False
for row in replyWrong['answer']:
    spec = {'uid':'client_id',
            'known':[{'col':'acct_date','val':row[0]},
                     {'col':'acct_district_id','val':row[1]}],
            'guess':[{'col':'frequency','val':row[2]}]
           }
    x.askClaim(spec,claim=claim,cache=doCache)
    claim = True

claim = False
for row in replyCorrect['answer']:
    spec = {'uid':'client_id',
            'known':[{'col':'acct_date','val':row[0]},
                     {'col':'acct_district_id','val':row[1]}],
            'guess':[{'col':'frequency','val':row[2]}]
           }
    x.askClaim(spec,claim=claim,cache=doCache)
    claim = True

while True:
    reply = x.getClaim()
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

# -------------------  Scores Phase  ----------------------------

print("\nTest mix correct and wrong:")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
#pp.pprint(score['col']['frequency'])
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
x.cleanUp()

# TEST ALL COLUMNS CORRECT

# Here I make an attack that shows that all columns are susceptible

x = gdaAttack(params)

# -------------------  Exploration Phase  ------------------------
# Start by getting the names of all columns
cols = x.getColNames()

# -------------------  Prior Knowledge Phase  --------------------
# This attack doesn't require any prior knowledge

# -------------------  Attack Phase  -----------------------------

# The List attack, the simplest of attacks, simply requests a list
# of counts for a set of column values, where each count is equal
# to exactly one, and therefore has only a single uid.

query = {}
sql = "select "
for col in cols:
    sql += str(f"{col}, ")
sql += "count(*) from accounts group by "
for i in range(1,len(cols)+1):
    sql += str(f"{i}")
    if i == len(cols):
        sql += " "
    else:
        sql += ", "
sql += "having count(*) = 1 limit 100"
query['sql'] = sql
print(sql)
x.askAttack(query)
replyCorrect = x.getAttack()
if v: pp.pprint(replyCorrect)

# -------------------  Claims Phase  ----------------------------

for row in replyCorrect['answer']:
    spec = {'uid':'client_id'}
    guess = []
    for i in range(len(cols)):
        guess.append({'col':cols[i],'val':row[i]})
    spec['guess'] = guess
    x.askClaim(spec,cache=doCache)

while True:
    reply = x.getClaim()
    if v: print("Claim Result:")
    if v: pp.pprint(reply)
    if reply['stillToCome'] == 0:
        break

# -------------------  Scores Phase  ----------------------------

print("\nTest all columns correct:")
attackResult = x.getResults()
sc = gdaScores(attackResult)
score = sc.getScores()
if v: pp.pprint(score)
expect = {'attackCells': 1000,
          'attackGets': 1,
          'claimCorrect': 100,
          'claimError': 0,
          'claimMade': 100,
          'claimPassCorrect': 0,
          'claimTrials': 100,
          'knowledgeCells': 0,
          'knowledgeGets': 0,
          'confidenceImprovement': 1.0,
          'columnSusceptibility': 1.0
         }
# for every column, we expect the same score:
for col in cols:
    checkMatch(score,expect,col)
print("\nOperational Parameters:")
op = x.getOpParameters()
pp.pprint(op)
x.cleanUp()

