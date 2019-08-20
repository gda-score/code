import sys
import pprint
import six
from gdascore.gdaScore import gdaAttack, gdaScores
from gdascore.gdaUtilities import setupGdaAttackParameters,comma_ize,makeGroupBy,finishGdaAttack,makeInNotNullConditions
# from makeGraphs import runOneDirectory

v = True

# This script makes attack queries, and then requests the
# resulting GDA score.

pp = pprint.PrettyPrinter(indent=4)

params = dict(name='UberSinglingOutAttack',
              rawDb='localBankingRaw',
              anonDb='cloakBankingAnon', #For raw make both raw and annondb loaclBankingRaw #For anon make raw loaclBankingRaw and annon cloakBankingAnon
              criteria='singlingOut',
              table='accounts', # change the table name to run individual table.
              resultsPath='../attacks/attackResults',
              flushCache=False,
              verbose=False)
# x = gdaAttack(params)  #TODO: Code clean up

def getTotalUser(x):
    """Returns the number of users of the table."""
    # Launch queries
    query = dict(uid='uid')
    # Note error in this sql
    sql = str(f"""select count(distinct uid)
                 from {params['table']}""")
    query['sql'] = sql
    x.askAttack(query)

def getResultFromQuery(queryParser,x):
    """Returns the values of the table being used in the attack."""
    #TODO: Make the script dynamic for both raw and anondb
    # Test for annonDb
    colnames = x.getColNames(dbType='anonDb')
    # colnames = x.getColNames()
    for i in colnames:
        values = x.getPublicColValues(i)
        if values != []:
            queryParser[i] = values
    return queryParser

def makeNoiseQuery(getKeycolumn, getCombinations,x):
    """Returns the noise of the table being used in the attack."""
    # Launch queries
    # TODO: Make the script dynamic for both raw and anondb
    # Test for annonDb
    numAskAttack = 0
    colnames = x.getColNames(dbType = 'anonDb')
    # colnames = x.getColNames()
    # changed to uid from account_id
    primaryKeyColumn = dict(uid='uid')
    # Note this sql query is generated dynamically
    outputCol = getKeyColumn
    outputComb = getCombinations
    comLength = len(outputComb)
    colLength = len(outputCol)
    #  20 is acclaimed as a branch of queries
    branch = 20
    # Launch queries
    query = dict(myTag='query1')
    # Raw query
    raw_sql = str(f"""select count(distinct {primaryKeyColumn['uid']})
              from {params['table']}
              where """)

    while comLength > 0:
        val = getCombinations[len(outputComb) - comLength]
        sql = raw_sql
        while colLength > 0:
            if isinstance(val[len(outputCol) - colLength], six.string_types):
                dynamic_add = str(f"""{outputCol[len(outputCol) - colLength]} = '{val[len(outputCol) - colLength]}' """) + ' and '
            else:
                dynamic_add = str(f"""{outputCol[len(outputCol) - colLength]} = {val[len(outputCol) - colLength]} """) + ' and '
            if colLength == 1:
                if isinstance(val[len(outputCol) - colLength], six.string_types):
                    dynamic_add = str(f"""{outputCol[len(outputCol) - colLength]} = '{val[len(outputCol) - colLength]}'""")
                else:
                    dynamic_add = str(f"""{outputCol[len(outputCol) - colLength]} = {val[len(outputCol) - colLength]}""")
            colLength = colLength - 1
            sql = sql + dynamic_add
        query['sql'] = sql
        # query = dict(db="raw", sql=sql)
        # make 20 clone of each queries, write now 20 is acclaimed as a branch of queries
        for q in range(branch):
            if v: print(f"{numAskAttack}: {query}")
            numAskAttack += 1
            x.askAttack(query)
        colLength = len(outputCol)
        comLength = comLength - 1

def getDiffrentColumnValues(col, values , queryParser):
    colvalDict = {}
    for key, value in queryParser.items():
        if key == col:
            for allval in value:
                values.append(allval[0])
            colvalDict = {col: values}
        values = []
    return colvalDict

def getNumberofKeyColumn(queryParser,getKeyColumn):
    for key in queryParser:
        getKeyColumn.append(key)
    return getKeyColumn

def getResultForComb(getKeyColumn,getResult,values,queryParser):
    for col in getKeyColumn:
        retDic = getDiffrentColumnValues(col, values, queryParser)
        getResult.append(retDic[col])
    return getResult

def getCombinatorics(getResult):
    r = [[]]
    for x in getResult:
        t = []
        for y in x:
            for i in r:
                t.append(i + [y])
        r = t

    return r
# get Average of the query branch
def Average(lst):
    return sum(lst) / len(lst)

def uber_singling_out_attack(params,getKeyColumn,getResult,values):
#    x = gdaAttack(params)
#    getTotalUser(x)
#    #No longer needed so doing the clean up after getting the number of user in the table
#    x.cleanUp()

    #Regenerate the x for further processing
    x = gdaAttack(params)
    queryParser = {}
    getResultFromQuery(queryParser, x)


    # Get number of return column
    getKeyColumn = getNumberofKeyColumn(queryParser, getKeyColumn)

    # Get total result
    getResult = getResultForComb(getKeyColumn, getResult, values, queryParser)

    # Use of recursion for combinatorics, with dynamically accessible values
    getCombinations = getCombinatorics(getResult)

    # Create all possible queries.
    makeNoiseQuery(getKeyColumn, getCombinations, x)



    # gather all the result of branch queries in a list, do the mean after that
    returnResults = []

    verbose = 0
    v = verbose
    doCache = True

    branchReturn = 20
    # check number of combinations
    outputComb = len(getCombinations)
    # And gather up the answers:
    for i in range(outputComb):
        # make 20 clone of each queries, get result of 20 similar queries
        for item in range(branchReturn):
            reply = x.getAttack()
            if v: print(reply)
            if 'error' in reply:
                print(reply['error'])
            else:
                returnResults.append(reply['answer'][0][0])
            if reply['stillToCome'] == 0:
                break
        average = Average(returnResults)
        if 0.5 <= average <= 1.5:
            average = 1.0
        if average == 1.0:
            claim = True
            # Test for annonDb
            colnames = x.getColNames(dbType='anonDb')
            # colnames = x.getColNames()
            # changed to uid from account_id
            primaryKeyColumn = dict(uid='uid')
            spec = {}
            spec = {'uid': primaryKeyColumn, 'known': []}  # known is optional, and always null here
            outputCol = getKeyColumn
            val = getCombinations[i]
            key = 'guess'
            spec.setdefault(key, [])
            for item in range(len(outputCol)):
                spec[key].append({'col': outputCol[item], 'val': val[item]})
            x.askClaim(spec, claim=claim, cache=doCache)

            if v: print("------------------- Attack claims:")
            while True:
                reply = x.getClaim()
                if v: pp.pprint(reply)
                if reply['stillToCome'] == 0:
                    break
            # Clear cache all the time
            returnResults = []
        else:
            claim = False
            #Clear cache all the time
            returnResults = []
    attackResult = x.getResults()
    sc = gdaScores(attackResult)
    score = sc.getScores()
    if v: pp.pprint(score)
    x.cleanUp()
    final = finishGdaAttack(params, score)
    pp.pprint(final)

# This reads in the attack parameters and checks to see if the
# attack has already been run and completed
verbose = True
v = verbose

paramsList = setupGdaAttackParameters(sys.argv, criteria="singlingOut",
                                                  attackType="Singling-Out Attack")
for params in paramsList:
    if params['finished'] == True:
        print("The following attack has been previously completed:")
        pp.pprint(params)
        print(f"Results may be found at {params['resultsPath']}")
        continue

    print("Running attack with following parameters:")
    pp.pprint(params)
    getKeyColumn = []
    getResult = []
    values = []
    uber_singling_out_attack(params, getKeyColumn, getResult, values)
print('Complete Attack')

