import sys
import pprint
import itertools
from gdascore.gdaScore import gdaAttack, gdaScores
from gdascore.gdaUtilities import setupGdaAttackParameters, comma_ize, makeGroupBy, finishGdaAttack

pp = pprint.PrettyPrinter(indent=4)
verbose = True
v = verbose


# -------------------------- subroutines --------------------

def doQueryErrorAndExit(reply, attack):
    pp.pprint(reply)
    attack.cleanUp(cleanUpCache=False)
    sys.exit()


def runOneAttack(guessedCol, knownCols, attack, table, numClaims):
    # -------------- Attack phase ------------------
    # And now run the attack for some fraction of the attackable cells
    if v: print(f"RunOneAttack with guessed '{guessedCol}', known {knownCols}")
    allCols = [guessedCol] + list(knownCols)
    sql = "SELECT "
    sql += comma_ize(allCols)
    sql += str(f"count(*) FROM {table} ")
    sql += makeGroupBy(allCols)
    query = dict(sql=sql)
    attack.askAttack(query)
    reply = attack.getAttack()
    if 'error' in reply:
        doQueryErrorAndExit(reply, attack)
    # Build a dict out of the knownCols values, and remember the index
    # for cases where the knownCols has a single guessedCol value
    s = {}
    ans = reply['answer']
    for r in range(len(ans)):
        # I want a 'foo'.join(thing) here, but need to deal with fact that
        # the value might not be a string
        key = ''
        for i in range(1, len(allCols)):
            key += '::' + str(f"{ans[r][i]}")
        if key in s:
            s[key] = -1
        else:
            s[key] = r
    for key, r in s.items():
        if r == -1:
            continue
        # This is a potential inference
        spec = {}
        known = []
        row = ans[r]
        for i in range(1, len(allCols)):
            known.append({'col': allCols[i], 'val': row[i]})
        spec['known'] = known
        if row[0] is None:
            pp.pprint(ans)
        spec['guess'] = [{'col': guessedCol, 'val': row[0]}]
        attack.askClaim(spec)

    while True:
        reply = attack.getClaim()
        numClaims += 1
        if v: pp.pprint(reply)
        if reply['stillToCome'] == 0:
            break
    return numClaims


# -------------------------- body ---------------------------

def diffix_infer_1_attack(params):
    ''' This is an inference attack against Diffix

        In this attack, we find attribute groups where the inference
        conditions exist (one one guessed column value exists for some
        set of one or more known column values). This is designed to work
        against Diffix and Full K-anonymity at least.
    '''
    attack = gdaAttack(params)

    # -------------------  Exploration Phase  ------------------------
    # We need to know the columns that are in the anonymized database
    # and in the raw database. It is these columns that we can attack.

    table = attack.getAttackTableName()
    rawColNames = attack.getColNames(dbType='rawDb')
    anonColNames = attack.getColNames(dbType='anonDb')
    colNames = list(set(rawColNames) & set(anonColNames))
    if v: print(f"Common columns are: {colNames}")

    # Get the total number of rows so that we can later determine fraction
    # of cells per column that are susceptible
    sql = str(f"SELECT count(*) FROM {table}")
    query = dict(db="rawDb", sql=sql)
    attack.askExplore(query)
    reply = attack.getExplore()
    if 'error' in reply:
        doQueryErrorAndExit(reply, attack)
    totalRows = reply['answer'][0][0]
    if v: print(f"Total Rows: {totalRows}")

    # There is really no point in trying to find instances of
    # inference where the guessed column has a large number of values.
    # In these cases, the chances of finding an inference instance is
    # very low. We (arbitrarily for now) set the threshold for this at 10

    # By the same token, an attack where the known column has a majority
    # values that are distinct to a single user won't work for an attack,
    # because in the case of Diffix, they will be low-count filtered, and
    # in the case of Full K-anonymity, they may be aggregated

    # So we record the number of distinct values per column. (In practice,
    # this would not be known exactly, but the attacker can be assumed to
    # have a reasonable guess just based on knowledge of the column.)
    distincts = {}
    guessableCols = []
    for col in colNames:
        sql = str(f"SELECT count(DISTINCT {col}) FROM {table}")
        query = dict(db="rawDb", sql=sql)
        attack.askAttack(query)
        reply = attack.getAttack()
        if 'error' in reply:
            doQueryErrorAndExit(reply, attack)
        totalDistinct = reply['answer'][0][0]
        distincts[col] = totalDistinct
        if totalDistinct <= 10:
            guessableCols.append(col)
    if v: print(f"Distincts: {distincts}")
    if v: print(f"guessableCols: {guessableCols}")

    # -------------------  Prior Knowledge Phase  --------------------
    # This attack doesn't require any prior knowledge

    for guessedCol in guessableCols:
        numClaims = 0
        remainingCols = [x for x in colNames if x != guessedCol]
        # We want to try various combinations of the remaining columns,
        # and try the attack if the ratio of distinct values (or expected
        # distinct value combinations) is not too high
        unusedCombinations = 0
        for num in range(len(remainingCols)):
            if unusedCombinations > 1000:
                # If we don't find a useable combination 1000
                # consecutive times, then give up
                break
            if numClaims > 25:
                break
            combs = itertools.combinations(remainingCols, num + 1)
            while True:
                if unusedCombinations > 1000:
                    break
                if numClaims > 25:
                    break
                try:
                    knownCols = next(combs)
                except:
                    break
                totalDistinct = 1
                for c in knownCols:
                    totalDistinct *= distincts[c]
                if v: print(f"totalDistinct: {totalDistinct} "
                            "from known columns {knownCols}")
                if (totalDistinct / totalRows) > 0.8:
                    unusedCombinations += 1
                    continue
                unusedCombinations = 0
                numClaims = runOneAttack(guessedCol, knownCols,
                                         attack, table, numClaims)

    # -------------------  Scores Phase  ----------------------------

    attackResult = attack.getResults()
    sc = gdaScores(attackResult)
    # New we need to assign susceptibility scores, which means making
    # some explore queries
    for guessedCol in colNames:
        remainingCols = [x for x in colNames if x != guessedCol]
        # -------------- More exploration phase ------------------
        # First find out how many of the cells are attackable
        sql = "SELECT sum(rows) FROM (SELECT "
        sql += comma_ize(remainingCols)
        sql += str(f"count(*) AS rows FROM {table} ")
        sql += makeGroupBy(remainingCols)
        sql += str(f" HAVING count(DISTINCT {guessedCol}) = 1) t")
        if v: print("-------------------- Explore query:")
        if v: print(sql)
        query = dict(db="raw", sql=sql)
        attack.askExplore(query)
        reply = attack.getExplore()
        if 'error' in reply:
            doQueryErrorAndExit(reply, attack)
        numRows = reply['answer'][0][0]
        if v: print("-------------------- Explore reply:")
        if v: pp.pprint(reply)
        susValue = numRows / totalRows
        sc.assignColumnSusceptibility(guessedCol, susValue)
    # Get average score (default behavior)
    score = sc.getScores()
    if v: pp.pprint(score)
    score = sc.getScores(numColumns=1)
    if v: pp.pprint(score)
    attack.cleanUp(cleanUpCache=False)
    final = finishGdaAttack(params, score)
    pp.pprint(final)


# This reads in the attack parameters and checks to see if the
# attack has already been run and completed

paramsList = setupGdaAttackParameters(sys.argv, criteria="inference",
                                      attackType="Inference Attack on Diffix 1")
for params in paramsList:
    if params['finished'] == True:
        print("The following attack has been previously completed:")
        pp.pprint(params)
        print(f"Results may be found at {params['resultsPath']}")
        continue

    print("Running attack with following parameters:")
    pp.pprint(params)
    diffix_infer_1_attack(params)
