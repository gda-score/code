import pprint
import sys
from gdaAttack import gdaAttack
from gdaScore import gdaScores
from gdaTools import setupGdaAttackParameters

sys.stdout.flush()

# Set printAttack to True to get print of all attack queries and results
# (roughly 10-15 of these per attacked user)
printAttack = False
# Set this to true to reduce the number of actual attack runs so
# that the program doesn't take too long to run
numUsersAttacked = 3     # set to 200 for full attack --- takes a long time

# -------------------------- subroutines ---------------------------

def finishUp(x):
    attackResult = x.getResults()
    sc = gdaScores(attackResult)
    score = sc.getScores()
    (bounty,score) = sc.getBountyScoreFromScore(score)
    print('''
The score returned by `getScores()` contains substantial information about the
attack, and can for instance be used to generate a GDA Score diagram.

For the purpose of the bounty, the call `getBountyScoreFromScore()` computes and
returns only the parameters used for the 'Effectiveness' portion of the bounty prize:
    ''')
    pp.pprint(bounty)
    x.cleanUp()
    quit()

def doAttackQuery(x,strOff,strLen,strMatch,columns,noisyRatios):
    query = {}
    sql = f"""
        SELECT frequency, count(*)
        FROM accounts
        WHERE substring({columns[0]} FROM {strOff[0]} FOR {strLen[0]}) = '{strMatch[0]}'
    """
    if len(columns) == 2:
        sql += f"""
        AND substring({columns[1]} FROM {strOff[1]} FOR {strLen[1]}) = '{strMatch[1]}'
    """
    sql += " GROUP BY 1"
    query['sql'] = sql
    x.askAttack(query)
    reply = x.getAttack()
    if len(reply['answer']) < len(noisyRatios):
        # fail because of suppression
        return None
    total = 0
    for bucket in reply['answer']:
        total += bucket[1]
    for bucket in reply['answer']:
        noisyRatios[bucket[0]] += bucket[1]/total
    return reply     # pass

def runOneColumn(value,colName,noisyRatios,p):
    strLen = 1
    numQueries = 0
    while True:
        numPass = 0
        for i in range(len(value) + 1 - strLen):
            if p: print(f"Try {colName} {value}, len {strLen}, offset {i+1}")
            strMatch = value[i:i+strLen]
            reply = doAttackQuery(x,[i+1],[strLen],[strMatch],[colName],noisyRatios)
            if reply:
                numPass += 1
                numQueries += 1
                if p: pp.pprint(reply)
                if p: pp.pprint(noisyRatios)
        if numPass == 0:
            # We didn't get any passed queries for this string length
            return numQueries
        strLen += 1
        if strLen == len(value):
            return numQueries

def runAttackQueries(x,first,last,noisyRatios,p):
    # initialize the noisy ratios
    for key in noisyRatios.keys():
        noisyRatios[key] = 0
    if p: print(f"Attack victim {first} {last}:")
    numQueries = runOneColumn(first,'firstname',noisyRatios,p)
    numQueries += runOneColumn(last,'lastname',noisyRatios,p)
    return numQueries

# -------------------------- body ---------------------------

pp = pprint.PrettyPrinter(indent=4)

verbose = 0
v = verbose
doCache = True

# This config indicates that we are going to do a singling out attack
# on Diffix using the accounts table of the banking dataset.

config = {
    "configVersion": "compact1",
    "basic": {
        "attackType": "Test",
        "criteria": "linkability"
    },
    'anonTypes': [ ["diffix","v19_1_0"] ],
    'tables': [ ['banking','accounts'] ]
}

# setupGdaAttackParameters() converts the config into a list of parameter
# structures for the GDA Score library. If the config has multiple anonTypes
# or multiple tables, then setupGdaAttackParameters() generates one parameter
# structure per anonTypes/tables combination. Here there is only one each,
# so there is only one parameters structure created.
paramsList = setupGdaAttackParameters(config)
params = paramsList[0]
pp.pprint(params)

# Make the object for running the attack
x = gdaAttack(params)

print(" -------------------  Exploration Phase  ------------------------")

print('''
For a linkability attack, there are two databases, a protected one and
a public one. About half of the users in the public one are also in the
protected one (common users), and about half the users in each are
unique to that database. The data for common users is identical for
both databases.

In the linkability attack, the attacker tries to determine if users in
the public database are also in the protected database.

Atticus first gets a list of column names (assumed to be public
knowledge):
''')
colNames = x.getColNames(dbType='anonDb')
pp.pprint(colNames)

print('''
Atticus then selects 200 users from the public database. For this
attack, Atticus assumes that all users with first and last names
both greater than 6 characters are in the protected datababse.
(Yes this is a stupid attack ... it is just for illustration.)
Therefore Atticus gets first and last name and UID.
''')

sql = f'''
    SELECT uid, firstname, lastname
    FROM accounts
    WHERE length(firstname) > 6 and length(lastname) > 6
    LIMIT {numUsersAttacked}
'''

print(sql)

print('''
The `askExplore() / getExplore()` calls can be used to send any query
to any database without effecting the bounty score. In this query,
the public database is selected with `pubDb`:
''')

query = {}
query['sql'] = sql
query['db'] = 'pubDb'
x.askExplore(query)
reply = x.getExplore()
if 'answer' in reply:
    pubUsers = reply['answer']

print("The first few users are these:")
numRows = min(5,numUsersAttacked)
for i in range(numRows):
    print(f"   {pubUsers[i]}")

# -------------------  Attack and Claim Phase  -----------------------------

print('''
Since this is such a dumb and simple attack, there are no attack queries.
Please look at bountySinglingOut.py for examples of attack queries.

Instead we just make a claim for every user that we selected from the
public table.
''')

for user in pubUsers:
    first = user[1]
    last = user[2]
    uid = user[0]
    spec = {
            'guess':[{'col':'uid','val':uid}]
           }
    # Note here that if for a different attack, the attacker determined that
    # there was not sufficient confidence to make a claim, then the attacker
    # should still do and askClaim()/getClaim(), but set `claim=False`.
    x.askClaim(spec,claim=True)
    reply = x.getClaim()
    if printAttack: pp.pprint(reply)
    print(f"Claim for user {first} {last} is {reply['claimResult']}")

finishUp(x)

