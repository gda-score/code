import sys
import json
import pprint
import math

def setupGdaAttackParameters(cmdArgs, criteria=''):
    """ Basic prep for input and output of running an attack

        `cmdArgs` is the command line args list (`sys.argv`) <br/>
        `cmdArgs[1]` is either the name or the config file, or
        empty if the default config file name should be used. <br/>
        Returns a list of data structure that can be used for
        class `gdaAttack()` <br/>
        Adds the following to the returned data structure (`par`) <br/>
        `par['finished']`: `True` if attack previously completed. 
        Else `False` <br/>
        `par['resultsPath']`: Path to filename where results should
        be stored. <br/>
        `par['criteria']: if one of the calling parameters.
    """

    pp = pprint.PrettyPrinter(indent=4)
    usageStr = str(f"""Usage: 
        Either specify configuration file:
            > {cmdArgs[0]} config.json
        Or assume default configuration file '{cmdArgs[0]}.json':
            > {cmdArgs[0]}
        """)
    if len(cmdArgs) == 1:
        fileName = cmdArgs[0] + '.json'
    elif len(cmdArgs) != 2:
        sys.exit(usageStr)
    else:
        fileName = sys.argv[1]

    try:
        f = open(fileName, 'r')
    except:
        e = str(f"ERROR: file '{fileName}' not found.\n{usageStr}")
        sys.exit(e)

    pmList = json.load(f)
    f.close()

    for pm in pmList:
        if 'criteria' not in pm or len(pm['criteria']) == 0:
            if not criteria:
                sys.exit("ERROR: criteria must be specified")
            pm['criteria'] = criteria
    
        if 'name' not in pm or len(pm['name']) == 0:
            baseName = str(f"{sys.argv[0]}")
            if 'anonType' in pm and len(pm['anonType']) > 0:
                baseName += str(f".{pm['anonType']}")
            if 'anonSubType' in pm and len(pm['anonSubType']) > 0:
                baseName += str(f".{pm['anonSubType']}")
            if 'dbType' not in pm or len(pm['dbType']) == 0:
                baseName +=  str(f".{pm['rawDb']}.{pm['anonDb']}")
            else:
                baseName += str(f".{pm['dbType']}")
            if 'table' in pm and len(pm['table']) > 0:
                baseName += str(f".{pm['table']}")
        else:
            baseName = pm['name']
        pm['name'] = baseName
    
        resultsDir = "attackResults";
        if 'resultsDir' in pm and len(pm['resultsDir']) > 0:
            resultsDir = pm['resultsDir']
    
        resultsPath = resultsDir + "/" + baseName + ".json"
        pm['resultsPath'] = resultsPath
        pm['finished'] = False
        try:
            f = open(resultsPath, 'r')
        except:
            # No prior results for this attack have been posted
            pass
        else:
            # Prior results have been posted. Make sure they are complete.
            res = json.load(f)
            if 'finished' in res:
                pm['finished'] = True

    return pmList

def finishGdaAttack(params,score):
    """ Stores the attack results in a json file
    
        Returns the attack results data structure"""

    if 'finished' in params:
        del params['finished']
    final = {}
    final['score'] = score
    final['params'] = params
    final['finished'] = True
    j = json.dumps(final, sort_keys=True, indent=4)
    resultsPath = params['resultsPath']
    try:
        f = open(resultsPath, 'w')
    except:
        e = str(f"Failed to open {resultsPath} for write")
        sys.exit(e)
    f.write(j)
    f.close()
    return final

def comma_ize(strings,lastComma=True):
    """ Make a single string with input strings separated by commas.
        
        Set `lastComma=False` if there should be no ending comma."""

    string = ''
    for s in strings:
        string += str(f"{s}, ")
    if not lastComma:
        # snip off the last ', '
        ret = string[0:-2]
    else:
        ret = string
    return ret

def makeGroupBy(columns):
    """ Make a GROUP BY clause appropriate for list of columns"""

    clause = 'GROUP BY '
    for val in range(1,len(columns)+1):
        clause += str(f"{val}, ")
    # Strip off the last comma and add a space
    ret = clause[0:-2] + ' '
    return ret

def getInterpolatedValue(val0,val1,scoreGrid):
    """Compute interpolated value from grid of mapping tuples
    
       This routine takes as input a list of tuples ("grid") of the form
       `(val0,val1,score)`. It maps (val0,val1) values to a corresponding
       score. It returns a score that is interpolated between the
       scores in the grid. An example of such a grid can be found in
       gdaScore.py, called `_defenseGrid1`. Note that val0 and val1 must
       go in descending order as shown. Input values that are above the
       highest val0 and val1 values will take the score of the first
       entry. Input values that are below the lowest val0 and val1 will
       take the score of the last entry.
    """
    scoreAbove = -1
    scoreBelow = -1
    for tup in scoreGrid:
        tup0 = tup[0]
        tup1 = tup[1]
        score = tup[2]
        if val0 <= tup0 and val1 <= tup1:
            tup0Above = tup0
            tup1Above = tup1
            scoreAbove = score
    for tup in reversed(scoreGrid):
        tup0 = tup[0]
        tup1 = tup[1]
        score = tup[2]
        if val0 >= tup0 and val1 >= tup1:
            tup0Below = tup0
            tup1Below = tup1
            scoreBelow = score
    if scoreAbove == -1 and scoreBelow == -1:
        return None
    if scoreAbove == -1:
        return scoreBelow
    if scoreBelow == -1:
        return scoreAbove
    if scoreAbove == scoreBelow:
        return scoreAbove
    # Interpolate by treating as right triangle with tup0 as y and
    # tup1 as x
    yLegFull = tup0Above - tup0Below
    xLegFull = tup1Above - tup1Below
    hypoFull = math.sqrt((xLegFull ** 2) + (yLegFull ** 2))
    yLegPart =  val0 - tup0Below
    xLegPart =  val1 - tup1Below
    hypoPart = math.sqrt((xLegPart ** 2) + (yLegPart ** 2))
    frac = hypoPart / hypoFull
    interpScore = scoreBelow - (frac * (scoreBelow - scoreAbove))
    return interpScore
