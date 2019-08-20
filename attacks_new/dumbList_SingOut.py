import sys
import pprint
from gdascore.gdaScore import gdaAttack, gdaScores
from gdascore.gdaUtilities import setupGdaAttackParameters,comma_ize,makeGroupBy,finishGdaAttack,makeInNotNullConditions

pp = pprint.PrettyPrinter(indent=4)
doVerbose = False


# -------------------------- subroutines --------------------

# -------------------------- body ---------------------------

def dumb_list_singling_out_attack(params):
    """ Dumb List attack for the Singling Out criteria.

        All it does is request rows with all columns from the anonymized
        database. The attack succeeds if the anonymized database returns
        rows that single out users, and fails otherwise. It is designed to
        work against raw and pseudonymized data."""
    attack = gdaAttack(params)
    
    # -------------------  Exploration Phase  ------------------------
    # We need to know the columns that are in the anonymized database
    # and in the raw database. It is these columns that we can attack.
    # (Note that pseudonymization schemes can delete some columns.)
    
    table = attack.getAttackTableName()
    rawColNames = attack.getColNames(dbType='rawDb')
    anonColNames = attack.getColNames(dbType='anonDb')
    if rawColNames is None or anonColNames is None:
        print(f"No table to attack (raw {rawColNames}, anon {anonColNames}")
        attack.cleanUp()
        return
    uid = attack.getUidColName()
    colNamesAll = list(set(rawColNames) & set(anonColNames))
    if v: print(f"Use columns: {colNamesAll}")

    # The cloak can't handle queries with a large number of columns,
    # so we split up the attack into groups of 5 columns each. Each group
    # contains the uid column, so that we are sure that the resulting
    # answer pertains to a single user.
    groupSize = 5
    minAttacksPerGroup = 5
    groups = []
    colsWithoutUid = colNamesAll.copy()
    colsWithoutUid.remove(uid)
    if v: print(colNamesAll)
    if v: print(colsWithoutUid)
    index = 0
    while(1):
        if index >= len(colsWithoutUid):
            break
        endIndex = index + groupSize - 1
        nextGroup = colsWithoutUid[index:endIndex]
        nextGroup.append(uid)
        groups.append(nextGroup)
        index += groupSize - 1

    # This will give us around 100 attack queries total:
    numAttacksPerGroup = min(int(100/len(groups)) + 1,minAttacksPerGroup)
    if v: pp.pprint(groups)

    # -------------------  Prior Knowledge Phase  --------------------
    # This attack doesn't require any prior knowledge
    
    # -------------------  Attack Phase  -----------------------------
    
    for colNames in groups:
        query = {}
        sql = "SELECT "
        sql += comma_ize(colNames)
        sql += str(f"count(*) FROM {table} WHERE ")
        sql += makeInNotNullConditions(colNames)
        sql += makeGroupBy(colNames)
        sql += " HAVING count(*) = 1 ORDER BY uid "
        sql += str(f" LIMIT {numAttacksPerGroup} ")
        query['sql'] = sql
        print("-------------------- Attack query:")
        print(sql)
        attack.askAttack(query)
        reply = attack.getAttack()
        if v: print("-------------------- Attack reply:")
        if v: pp.pprint(reply)
        
        # -------------------  Claims Phase  ----------------------------
        
        if 'answer' not in reply:
            print("ERROR: reply to claim query contains no answer")
            pp.pprint(reply)
            attack.cleanUp()
            sys.exit()
        for row in reply['answer']:
            spec = {}
            guess = []
            for i in range(len(colNames)):
                guess.append({'col':colNames[i],'val':row[i]})
            spec['guess'] = guess
            attack.askClaim(spec)
        
        if v: print("------------------- Attack claims:")
        while True:
            reply = attack.getClaim()
            if v: pp.pprint(reply)
            if reply['stillToCome'] == 0:
                break
    
    # -------------------  Scores Phase  ----------------------------
    
    attackResult = attack.getResults()
    sc = gdaScores(attackResult)
    score = sc.getScores()
    if v: pp.pprint(score)
    attack.cleanUp()
    final = finishGdaAttack(params,score)
    pp.pprint(final)

# This reads in the attack parameters and checks to see if the
# attack has already been run and completed
verbose = False
v = verbose

paramsList = setupGdaAttackParameters(sys.argv)
for params in paramsList:
    params['verbose'] = doVerbose
    if params['finished'] == True:
        print("The following attack has been previously completed:")
        pp.pprint(params)
        print(f"Results may be found at {params['resultsPath']}")
        continue

    print("Running attack with following parameters:")
    pp.pprint(params)
    dumb_list_singling_out_attack(params)
