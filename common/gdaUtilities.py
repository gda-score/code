import sys
import json
import pprint
import math
import os
from pkg_resources import Requirement, resource_filename


def try_for_config_file(config_rel_path):
    # First case: find config in repository; use paths in PATH and PYTHONPATH as potential repository root folders
    for p in sys.path:
        if os.path.isfile(os.path.abspath(os.path.join(p, config_rel_path))):
            return os.path.abspath(os.path.join(p, config_rel_path))
    if "PYTHONPATH" in os.environ:
        for p in os.environ["PYTHONPATH"].split(os.pathsep):
            if os.path.isfile(os.path.abspath(os.path.join(p, config_rel_path))):
                return os.path.abspath(os.path.join(p, config_rel_path))

    # Second case: find config inside pip package location
    if os.path.isfile(os.path.abspath(resource_filename(Requirement.parse("gda-score-code"), config_rel_path))):
        return os.path.abspath(resource_filename(Requirement.parse("gda-score-code"), config_rel_path))

    # Third case: look in typical config file locations
    if os.path.isfile(os.path.abspath(os.path.join(os.path.expanduser("~"), ".gdaScore", config_rel_path))):
        return os.path.abspath(os.path.join(os.path.expanduser("~"), ".gdaScore", config_rel_path))
    if os.path.isfile(os.path.abspath(os.path.join(os.sep, "etc", "gdaScore", config_rel_path))):
        return os.path.abspath(os.path.join(os.sep, "etc", "gdaScore", config_rel_path))

    # Forth case: we find nothing
    return None


def getDatabaseInfo(dbName):
    ''' Retrieves the database info from the database config file.

        The path to the database config file must be hard-coded here.
    '''
    path = try_for_config_file(os.path.join("common", "config", "myDatabases.json"))
    if path is None:
        print(f"Error: No config file found")
        return None
    print(path)
    fh = open(path, "r")
    j = json.load(fh)
    if dbName in j:
        return j[dbName]
    else:
        print(f"Error: Database '{dbName}' not found in file {path}")
        return None

def setupGdaAttackParameters(cmdArgs, criteria='', attackType=''):
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
        `par['attackType']: if one of the calling parameters.
    """

    pp = pprint.PrettyPrinter(indent=4)
    usageStr = str(f"""Usage: 
        Either specify configuration file:
            > {cmdArgs[0]} config.json
        Or assume default configuration file '{cmdArgs[0]}.json':
            > {cmdArgs[0]}
        """)
    if len(cmdArgs) == 1:
        cmdName = os.path.basename(cmdArgs[0])
        if len(cmdName) == 0:
            sys.exit(usageStr)
        fileName = cmdName + '.json'
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
        if 'attackType' not in pm or len(pm['attackType']) == 0:
            if not attackType:
                sys.exit("ERROR: attackType must be specified")
            pm['attackType'] = attackType
    
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
        # Remove any spaces in the base name
        baseName.replace(" ","")
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

def makeInNotNullConditions(columns):
    """ Make the WHERE clause conditions of IS NOT NULL for columns"""

    clause = ''
    for col in columns:
        clause += str(f" {col} IS NOT NULL AND ")
    # Strip off the last AND
    ret = clause[0:-4]
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


