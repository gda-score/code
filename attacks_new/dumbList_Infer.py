import sys
import pprint
from gdascore.gdaScore import gdaAttack, gdaScores
from gdascore.gdaUtilities import setupGdaAttackParameters,comma_ize,makeGroupBy,finishGdaAttack,makeInNotNullConditions

pp = pprint.PrettyPrinter(indent=4)
doVerbose = False


# -------------------------- subroutines --------------------

def doQueryErrorAndExit(reply,attack):
    pp.pprint(reply)
    attack.cleanUp()
    sys.exit()


# -------------------------- body ---------------------------

def dumb_list_inference_attack(params):
    """ Dumb List attack for the Inference criteria.

        In an inference attack, there are 'known' column values, and
        'guessed' column values. An inference claim succeeds when all
        users with the known column values have the same guessed column
        values. There only needs to be one such user, so we can try
        making inferences on all columns by using all the other columns
        as known values.
        """
    attack = gdaAttack(params)
    
    # -------------------  Exploration Phase  ------------------------
    # We need to know the columns that are in the anonymized database
    # and in the raw database. It is these columns that we can attack.
    # (Note that pseudonymization schemes typically delete some columns.)
    
    table = attack.getAttackTableName()
    rawColNames = attack.getColNames(dbType='rawDb')
    anonColNames = attack.getColNames(dbType='anonDb')
    if rawColNames is None or anonColNames is None:
        print(f"No table to attack (raw {rawColNames}, anon {anonColNames}")
        attack.cleanUp()
        return
    colNames = list(set(rawColNames) & set(anonColNames))

    # Get the total number of rows so that we can later determine fraction
    # of cells per column that are susceptible
    sql = str(f"SELECT count(*) FROM {table}")
    if v: print(sql)
    query = dict(db="raw",sql=sql)
    attack.askExplore(query)
    reply = attack.getExplore()
    if 'error' in reply:
        doQueryErrorAndExit(reply,attack)
    totalRows = reply['answer'][0][0]

    # -------------------  Prior Knowledge Phase  --------------------
    # This attack doesn't require any prior knowledge
    
    # -------------------  Attack Phase  -----------------------------
    # I'm going to attack each (guessed) column by using the remaining
    # columns as the known colums. In the following, I loop through
    # attack and claims for each guessed column.

    for guessedCol in colNames:
        remainingCols = [x for x in colNames if x != guessedCol]
        # -------------- Attack phase ------------------
        # And now run the attack for some fraction of the attackable cells
        sql = "SELECT "
        sql += comma_ize(remainingCols)
        sql += str(f"max({guessedCol}) FROM {table} WHERE ")
        sql += makeInNotNullConditions(remainingCols)
        sql += makeGroupBy(remainingCols)
        sql += str(f" HAVING count(DISTINCT {guessedCol}) = 1 ")
        sql += str(f"ORDER BY 1 LIMIT 20")
        if v: print(sql)
        query = dict(sql=sql)
        attack.askAttack(query)
        reply = attack.getAttack()
        if 'error' in reply:
            # For this attack, cloak can't deal with max(text_col),
            # so just continue without claims
            continue
        # -------------- Claims phase ------------------
        for row in reply['answer']:
            spec = {}
            known = []
            for i in range(len(remainingCols)):
                known.append({'col':remainingCols[i],'val':row[i]})
            spec['known'] = known
            i = len(remainingCols)
            spec['guess'] = [{'col':guessedCol,'val':row[i]}]
            attack.askClaim(spec)
            while True:
                reply = attack.getClaim()
                if v: pp.pprint(reply)
                if reply['stillToCome'] == 0:
                    break
    
    # -------------------  Scores Phase  ----------------------------
    
    attackResult = attack.getResults()
    sc = gdaScores(attackResult)
    # New we need to assign susceptibility scores, which means making
    # some explore queries
    for guessedCol in colNames:
        remainingCols = [x for x in colNames if x != guessedCol]
        if len(remainingCols) > 20:
            remainingCols = remainingCols[:20]
        # -------------- More exploration phase ------------------
        # First find out how many of the cells are attackable
        sql = "SELECT sum(rows) FROM (SELECT "
        sql += comma_ize(remainingCols)
        sql += str(f"count(*) AS rows FROM {table} ")
        sql += makeGroupBy(remainingCols)
        sql += str(f" HAVING count(DISTINCT {guessedCol}) = 1) t")
        if v: print("-------------------- Explore query:")
        if v: print(sql)
        query = dict(db="raw",sql=sql)
        attack.askExplore(query)
        reply = attack.getExplore()
        if 'error' in reply:
            doQueryErrorAndExit(reply,attack)
        numRows = reply['answer'][0][0]
        if v: print("-------------------- Explore reply:")
        if v: pp.pprint(reply)
        susValue = numRows / totalRows
        sc.assignColumnSusceptibility(guessedCol,susValue)
    score = sc.getScores()
    if v: pp.pprint(score)
    final = finishGdaAttack(params,score)
    attack.cleanUp()
    pp.pprint(final)

# This reads in the attack parameters and checks to see if the
# attack has already been run and completed
verbose = False
v = verbose

paramsList = setupGdaAttackParameters(sys.argv, criteria="inference",
        attackType = "Simple List Inference Attack")
for params in paramsList:
    params['verbose'] = doVerbose
    if params['finished'] == True:
        print("The following attack has been previously completed:")
        pp.pprint(params)
        print(f"Results may be found at {params['resultsPath']}")
        continue

    print("Running attack with following parameters:")
    pp.pprint(params)
    dumb_list_inference_attack(params)
