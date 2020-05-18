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

def exploreDiffix(x,sql):
    query = {}
    query['sql'] = sql
    # By setting 'db' to 'anonDb', we explore the anonymous service, which in
    # this case is Diffix Cedar
    query['db'] = 'anonDb'
    x.askExplore(query)
    reply = x.getExplore()
    if 'answer' in reply:
        return reply['answer']
    else:
        print("exploreDiffix error")
        pp.pprint(reply)
        x.cleanUp()
        quit()

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

verbose = False

# This config indicates that we are going to do a singling out attack
# on Diffix using the accounts table of the banking dataset.

config = {
    "configVersion": "compact1",
    "basic": {
        "attackType": "Test",
        "criteria": "singlingOut"
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
params['verbose'] = verbose
pp.pprint(params)

# Make the object for running the attack
x = gdaAttack(params)

print(" -------------------  Exploration Phase  ------------------------")
print('''
There are certain things we can assume are public knowledge about a dataset.
This includes the table and column names and types, as well as values in
columns that are common to many users.

We can for instance get a list of column names and types:
''')
colNamesTypes = x.getColNamesAndTypes(dbType='anonDb')
pp.pprint(colNamesTypes)
print('''
Given these columns names, we can look at some common values. Generally
one could use getPublicColValues() for that, but for the Diffix Cedar
bounty program, we regard anything learned from Diffix itself to be 
public knowledge.

For instance, we can use Diffix to look at the values and row counts 
of the 'lastname' column in the banking accounts table:
''')
sql = '''
    SELECT lastname, count(*)
    FROM accounts
    GROUP BY 1
    ORDER BY 2 desc
'''
print(sql)

ans = exploreDiffix(x,sql)

print('''
Let's look at the first five buckets of the answer.
''')
for i,bucket in enumerate(ans):
    print(bucket)
    if i > 5: break

print('''
Note that the first bucket has the value '*' instead of a last name. In
the Aircloak system, this represents all values that were suppressed because
there are not enough distinct users to show the last name. From this answer,
then, we learn that most last names are unique or nearly unique. (Note that
for numeric columns, NULL is returned instead of '*'.)

Now let's look at the frequency column:
''')

sql = '''
    SELECT frequency, count(*)
    FROM accounts
    GROUP BY 1
    ORDER BY 2 desc
'''
freqHist = exploreDiffix(x,sql)
pp.pprint(freqHist)

print('''
In contrast to 'lastname', 'frequency' does not indicate any suppression
(no '*' value), and has only three distinct values. There could still be a
very small amount of suppression, but so little as to not matter for the
purpose of this attack.
''')

print("\n-------------------  Prior Knowledge Phase  --------------------")
print('''
The attacker Atticus needs to know all first and last names to run the attack.
Atticus will run the attack on 200 users, which is the minimum number
needed to establish confidence. This counts as prior knowledge.
Atticus can obtain this prior knowledge with the following query, which
will produce all rows for 200 users. (Since there is one user per row
in the accounts table, then this will produce 200 random rows.)
''')
prior = x.getPriorKnowledge(['firstname','lastname'],'users',count=numUsersAttacked)
print(f"getPriorKnowledge result has {len(prior)} users")
print("The first few users are these:")
numRows = min(5,numUsersAttacked)
for i in range(numRows):
    print(f"   {prior[i]}")

# -------------------  Attack and Claim Phase  -----------------------------

print('''
Let's suppose that in this attack, Atticus wants to exploit the
substring() function to create different groups of users, all of which
contain the victim, but otherwise contain somewhat different users.
With each such group, the attack will count the number of users with each
frequency value. The frequency value that, on average, has more users than
the expected number of users by the largest margin is assumed to be the
value held by the victim.
''')

print('''
First we need to compute the relative ratio of frequency values. We
already queried for the frequency count histogram in the exploration
phase. Compute the total number of users (approximate, since these are
anonymized counts):
''')

total = 0
for bucket in freqHist:
    total += bucket[1]

print(f"The total number of users is {total}")

ratios = {}
noisyRatios = {}
numFreqs = 0
for bucket in freqHist:
    ratios[bucket[0]] = bucket[1]/total
    noisyRatios[bucket[0]] = 0      # initialize
    numFreqs += 1

print(f"Each of {numFreqs} frequency values accounts for this fraction of users:")
pp.pprint(ratios)

print('''
For each of the 200 users, we are going to take a combination of
substrings from the first and last names, which will result in some
set of users. We will query for the frequency counts for these users,
and tally up the corresponding ratios.

The specific attack queries run in runAttackQueries() will differ
depending on the attack. The main point here is that the askAttack() /
getAttack() API calls are made when running the attack.

In this attack, we are assuming that each firstname,lastname combination
is unique among all users. The singling out claims are of the form:

    "There is a single user with the given values of firstname, lastname
     and frequency"

Since firstname and lastname are probably unique, then if we guess the
correct frequency value, the user is successfully singled out, and the
confidence score improves.

Now make the attack queries. This involves multiple queries per user, so
takes a little time.....
''')

totNumQueriesCorrect = 0
totNumQueriesWrong = 0
totRatioExceedCorrect = 0
totRatioExceedWrong = 0
numCorrect = 0
numWrong = 0
for first,last in prior:
    # -------------------------- Attack Part ---------------------------
    numQueries = runAttackQueries(x,first,last,noisyRatios,printAttack)
    # At this point, noisyRatios contains the sum of the ratios that each
    # frequency value contributes, and numQueries is the number of queries
    # that contributed to those sums.
    # -------------------------- Claim Part ---------------------------
    for freq in noisyRatios.keys():
        noisyRatios[freq] = noisyRatios[freq] / numQueries
    # For this attack, we assume that the frequency for which the noisy
    # ratio exceeds the true ratio by the most is where the victim is.
    maxFreq = ''
    maxRatioExceed = -100
    for freq in noisyRatios.keys():
        ratioExceed = noisyRatios[freq] - ratios[freq]
        if ratioExceed > maxRatioExceed:
            maxFreq = freq
            maxRatioExceed = ratioExceed
    # Now we can make a claim with maxFreq and the claimed frequency.
    # The known columns are firstname and lastname, since we know these
    # from prior knowledge. The frequency column is the one we don't know,
    # so that one is set as the guess here.
    spec = {
            'known':[{'col':'firstname','val':first},
                     {'col':'lastname','val':last}],
            'guess':[{'col':'frequency','val':maxFreq}]
           }
    # Note here that if for a different attack, the attacker determined that
    # there was not sufficient confidence to make a claim, then the attacker
    # should still do and askClaim()/getClaim(), but set `claim=False`.
    x.askClaim(spec,claim=True)
    reply = x.getClaim()
    if reply['claimResult'] == 'Correct':
        totNumQueriesCorrect += numQueries
        totRatioExceedCorrect += maxRatioExceed
        numCorrect += 1
    else:
        totNumQueriesWrong += numQueries
        totRatioExceedWrong += maxRatioExceed
        numWrong += 1
    if printAttack: pp.pprint(reply)
    print(f"Claim for {first} {last} with frequency {maxFreq} is {reply['claimResult']}")

# These parameters were used to determine if confidence could be raised for
# certain attacks (and therefore not claiming lower-confidence attacks). This 
# turned out not to be the case.
avgNumQueriesCorrect = totNumQueriesCorrect / numCorrect
avgNumQueriesWrong = totNumQueriesWrong / numWrong
avgRatioExceedCorrect = totRatioExceedCorrect / numCorrect
avgRatioExceedWrong = totRatioExceedWrong / numWrong

#print(f"numQueries: Correct: {avgNumQueriesCorrect}, Wrong: {avgNumQueriesWrong}")
#print(f"ratioExceed: Correct: {avgRatioExceedCorrect}, Wrong: {avgRatioExceedWrong}")

print('''
Note that the attack up to this point is on a single column, `frequency`. This is
sufficient for producing an "Effectiveness" score for the bounty, but not for
producing a "Coverage" score. For this, the attack would need to be repeated for
every other column (or at least for those where the attacker believes he or she
can obtain a decent Coverage score).
''')

finishUp(x)

