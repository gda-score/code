import copy
import importlib.util
import json
import coloredlogs, logging
import math
import ntpath
import os
import pprint
import sys

coloredlogs.DEFAULT_FIELD_STYLES['asctime'] = {}
coloredlogs.DEFAULT_FIELD_STYLES['levelname'] = {'bold': True, 'color': 'white', 'bright': True}
coloredlogs.DEFAULT_LEVEL_STYLES['info'] = {'color': 'cyan', 'bright': True}
coloredlogs.install(
        fmt="[%(levelname)s] %(message)s (%(filename)s, %(funcName)s(), line %(lineno)d, %(asctime)s)",
        datefmt='%Y-%m-%d %H:%M',
        level=logging.INFO,
)
# for pdoc documentation
__all__ = ["setupGdaAttackParameters"]

from pkg_resources import Requirement, resource_filename


def try_for_config_file(config_rel_path):
    ####### added by frzmohammadali #######
    interested_file = ntpath.basename(config_rel_path)

    # case 0: local config path defined by user: when installing by pip
    global_config_variable = dict()
    config_var = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'global_config', 'config_var.json')
    if os.path.isfile(config_var):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'global_config', 'config_var.json'), 'r') as f:
            file_content = f.read()
            if len(file_content):
                global_config_variable = json.loads(file_content)
        try:
            potential_path = os.path.join(global_config_variable['config_path'], 'config', interested_file)
        except KeyError:
            # local config path has not been provided. should look into global_config in next step.
            pass
        else:
            if os.path.isfile(potential_path):
                return potential_path

    # Case 0.5: global_config: when installing by pip
    potential_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'global_config', interested_file)
    if os.path.isfile(potential_path):
        return potential_path
    ####### added by frzmohammadali #######

    # First case: find config in repository; use paths in PATH and PYTHONPATH as potential repository root folders
    for p in sys.path:
        if os.path.isfile(os.path.abspath(os.path.join(p, config_rel_path))):
            return os.path.abspath(os.path.join(p, config_rel_path))
    if "PYTHONPATH" in os.environ:
        for p in os.environ["PYTHONPATH"].split(os.pathsep):
            if os.path.isfile(os.path.abspath(os.path.join(p, config_rel_path))):
                return os.path.abspath(os.path.join(p, config_rel_path))

    # Second case: find config inside pip package location
    spec = importlib.util.find_spec("gda-score-code")
    if spec is not None:
        if os.path.isfile(os.path.abspath(resource_filename(Requirement.parse("gda-score-code"), config_rel_path))):
            return os.path.abspath(resource_filename(Requirement.parse("gda-score-code"), config_rel_path))

    # Third case: look in typical config file locations
    if os.path.isfile(os.path.abspath(os.path.join(os.path.expanduser("~"), ".gdaScore", config_rel_path))):
        return os.path.abspath(os.path.join(os.path.expanduser("~"), ".gdaScore", config_rel_path))
    if os.path.isfile(os.path.abspath(os.path.join(os.sep, "etc", "gdaScore", config_rel_path))):
        return os.path.abspath(os.path.join(os.sep, "etc", "gdaScore", config_rel_path))

    # Fourth case: we find nothing
    return None


def getDatabaseInfo(theDb):
    # For backwards compatibility, if this is a string use the old way
    if isinstance(theDb, str):
        return oldGetDatabaseInfo(theDb)
    # Get user name and password
    ### <OLD WAY> ###
    # cred = getCredentials()
    # theDb['password'] = cred[theDb['type']]['password']
    # theDb['user'] = cred[theDb['type']]['user']
    ### </OLD WAY> ###

    ### <NEW WAY> ###
    if theDb['type'] == "postgres":
        if os.environ.get("GDA_SCORE_RAW_PASS") and os.environ.get("GDA_SCORE_RAW_USER"):
            theDb['password'] = os.environ.get("GDA_SCORE_RAW_PASS")
            theDb['user'] = os.environ.get("GDA_SCORE_RAW_USER")
        else:
            logging.critical("GDA_SCORE_RAW_USER and GDA_SCORE_RAW_PASS must be set as environment variables "
                     "for working with rawDB. see README.md")
            sys.exit(0)

    elif theDb['type'] == "aircloak":
        if os.environ.get("GDA_SCORE_DIFFIX_PASS") and os.environ.get("GDA_SCORE_DIFFIX_USER"):
            theDb['password'] = os.environ.get("GDA_SCORE_DIFFIX_PASS")
            theDb['user'] = os.environ.get("GDA_SCORE_DIFFIX_USER")
        else:
            logging.critical("GDA_SCORE_DIFFIX_USER and GDA_SCORE_DIFFIX_PASS must be set as "
                     "environment variables for working with Aircloak database. see README.md")
            sys.exit(0)
    ### </NEW WAY> ###

    return theDb


def oldGetDatabaseInfo(dbName):
    ''' Retrieves the database info from the database config file.

        The path to the database config file must be hard-coded here.
    '''
    path = try_for_config_file(os.path.join("common", "config", "myDatabases.json"))
    if path is None:
        print(f"ERROR: No config file found")
        return None
    fh = open(path, "r")
    j = json.load(fh)
    if dbName in j:
        return j[dbName]
    else:
        print(f"Error: Database '{dbName}' not found in file {path}")
        return None


def getMasterConfig():
    ''' Retrieves master.json from the config file

        The path to the database config file must be hard-coded here.
    '''
    path = try_for_config_file(os.path.join("common", "config", "master.json"))
    if path is None:
        print(f"ERROR: No config file found (master.json)")
        return None
    fh = open(path, "r")
    j = json.load(fh)
    return j


def getCredentials():
    ''' Retrieves master.json from the config file

        The path to the database config file must be hard-coded here.
    '''
    path = try_for_config_file(os.path.join("common", "config", "myCredentials.json"))
    if path is None:
        print(f"ERROR: No config file found (myCredentials.json)")
        return None
    fh = open(path, "r")
    j = json.load(fh)
    return j


def getAnonDbs(master, anon, dbType):
    if dbType == 'link':
        databases = 'linkDatabases'
    elif dbType == 'pub':
        databases = 'pubDatabases'
    else:
        databases = 'databases'
    nextLevel = 0
    friendlyNames = [master['anonClasses']['friendlyName']]
    pp = pprint.PrettyPrinter(indent=4)
    if anon[nextLevel] not in master['anonClasses']:
        return (None, friendlyNames, None)
    nextDict = master['anonClasses'][anon[nextLevel]]
    friendlyNames.append(nextDict['friendlyName'])
    while True:
        if databases in nextDict:
            return (nextDict[databases], friendlyNames, nextDict['service'])
        # databases isn't here, so need to look for next level
        nextLevel += 1
        if nextLevel > (len(anon) - 1):
            return (None, friendlyNames, None)
        if anon[nextLevel] not in nextDict:
            return (None, friendlyNames, None)
        nextDict = nextDict[anon[nextLevel]]
        friendlyNames.append(nextDict['friendlyName'])
    return (None, friendlyNames, None)


def getTab(sourceDbs, useableDbs):
    for useDb in useableDbs:
        if useDb in sourceDbs:
            return useDb
    return None


def setupGdaAttackParameters(configInfo=None, utilityMeasure='',
                             criteria='', attackType=''):
    """ Basic prep for input and output of gdaAttack (attack or utility)

        If called with no parameters, then this routine
        reads a configuration file of the same name as the python file
        but with `.json` appended. (I.e. if the python file is `test.py`,
        then `test.py.json` is read.) <br/>
        Otherwise, `configInfo` is a python dict. <br/>
        Either way (json file or python dict), the basic structure of the
        configuration is: <br/>

            {
              "configVersion": "compact1",
              "basic": {
                "attackType": "Simple List",
                "criteria": "linkability"
              },
              "anonTypes": [
                ["no_anon"],
                ["diffix","latest"],
              ],
              "tables": [
                ["banking","accounts"],
                ["taxi","rides"],
              ]
            }

        The above is for attacks. If utility measure, then the `"basic"`
        contents may be for instance: <br/>

          "basic": {
            "utilityMeasure": "Single Column Count",
            "measureParam":"uid",
            "samples": 25,
            "ranges": [[10,50],[50,100],[100,500],[500,1000],[1000,5000]]
          },

        Returns a list of data structures that can be used for
        class `gdaAttack()`. One such data structure will be returned for
        every `anonTypes`/`tables` combination. (The above example would
        therefore return four data structures in the list.) <br/>
        If `anonTypes` is missing, then `[["no_anon"]]` is used by
        default. `basic` is not needed if the configuration is not used
        for either an attack or a utility measure. As such, the simplest
        valid configuration is: <br/>

            { "tables": [["table","column"]] }

        which gives access to the non-anonymized (raw) table. <br/>
        If the attack or utility measure has already been run (i.e. there is
        a complete scores json file), then `"finished": True` is added to the
        returned data structure. <br/>
        The other calling parameters are for backwards compatibility. <br/>
    """
    pp = pprint.PrettyPrinter(indent=4)
    # We can either pull in the config from a file, or from a dict.
    # If the former, configFile will be a list.
    if configInfo is None:
        configInfo = sys.argv
    if isinstance(configInfo, list):
        usageStr = str(f"""Usage: 
            Either specify configuration file:
                > {configInfo[0]} config.json
            Or assume default configuration file '{configInfo[0]}.json':
                > {configInfo[0]}
            """)
        # Pull in config from a json file
        if len(configInfo) == 1:
            cmdName = os.path.abspath(configInfo[0])
            if len(cmdName) == 0:
                sys.exit(usageStr)
            fileName = cmdName + '.json'
        elif len(configInfo) != 2:
            sys.exit(usageStr)
        else:
            fileName = sys.argv[1]

        try:
            f = open(fileName, 'r')
        except:
            e = str(f"ERROR: file '{fileName}' not found.\n{usageStr}")
            sys.exit(e)

        config = json.load(f)
        f.close()
    else:
        # use the dict
        config = configInfo

    if isinstance(config, list):
        # This is the old config style
        return oldSetupGdaAttackParameters(config, criteria, attackType)
    pmList = []
    master = getMasterConfig()
    if 'basic' in config and 'criteria' in config['basic']:
        criteria = config['basic']['criteria']
    else:
        # just a dummy value
        criteria = 'singlingOut'
    if criteria == 'linkability':
        (rawDbs, rawFriendlyNames, rawService) = (
            getAnonDbs(master, ["no_anon"], 'link'))
    else:
        (rawDbs, rawFriendlyNames, rawService) = (
            getAnonDbs(master, ["no_anon"], ''))
    if 'anonTypes' not in config:
        config['anonTypes'] = [["no_anon"]]
    for anon in config['anonTypes']:
        if not isinstance(anon, list):
            print("ERROR: The 'anonTypes' config must be a list of lists")
            quit()
        params = {"anonType": anon}
        if criteria == 'linkability':
            (anonDbs, anonFriendlyNames, anonService) = (
                getAnonDbs(master, anon, 'link'))
        else:
            pp.pprint(anon)
            (anonDbs, anonFriendlyNames, anonService) = (
                getAnonDbs(master, anon, ''))
        if anonDbs is None:
            params["error"] = "Could not find anonymization in master"
            pmList.append(params)
            continue
        for tab in config['tables']:
            # For each table, we need to find the database name
            params["table"] = tab
            db = tab[0]
            table = tab[1]
            dataFriendlyNames = [master['datasources']['friendlyName']]
            if db not in master['datasources']:
                params["error"] = "Could not find database in master"
                pmList.append(params)
                continue
            datasource = master['datasources'][db]
            dataFriendlyNames.append(datasource['friendlyName'])
            if table not in datasource['tables']:
                params["error"] = "Could not find table in master"
                pmList.append(params)
                continue
            dataFriendlyNames.append(table)
            uid = datasource['tables'][table]['uid']
            # We have the right records from master, so build the params
            # pp.pprint(anonDbs)
            # pp.pprint(rawDbs)
            # pp.pprint(datasource)
            servs = master['services']
            rawTab = getTab(datasource['databases'], rawDbs)
            params['rawDb'] = {"dbname": rawTab}
            params['rawDb']['port'] = servs[rawService]['port']
            params['rawDb']['host'] = servs[rawService]['host']
            params['rawDb']['type'] = servs[rawService]['type']
            anonTab = getTab(datasource['databases'], anonDbs)
            params['anonDb'] = {"dbname": anonTab}
            params['anonDb']['port'] = servs[anonService]['port']
            params['anonDb']['host'] = servs[anonService]['host']
            params['anonDb']['type'] = servs[anonService]['type']
            resultsPath = 'results/'
            name = os.path.basename(sys.argv[0])
            for item in anon:
                name += '.' + item
            name += '.' + db + '.' + table
            params['name'] = name
            params['flushCache'] = False
            params['verbose'] = False
            params['resultsPath'] = 'results/' + name + '.json'
            params['table'] = table
            params['uid'] = uid
            params['friendly'] = {"anonymization": anonFriendlyNames}
            params['friendly']['dataSource'] = dataFriendlyNames
            params['criteria'] = criteria
            if 'basic' in config:
                # basic is needed if attack or utility measure
                params['basicConfig'] = config['basic']
                if 'attackType' in config['basic']:
                    # This is an attack configuration
                    params['friendly']['attack'] = [
                        config['basic']['attackType'], criteria]
                    # If criteria is linkability, we need to add the
                    # public linkability db
                    if criteria == 'linkability':
                        (pubDbs, pubFriendlyNames, pubService) = (
                            getAnonDbs(master, anon, 'pub'))
                        pubTab = getTab(datasource['databases'], pubDbs)
                        params['pubDb'] = {"dbname": pubTab}
                        params['pubDb']['port'] = servs[rawService]['port']
                        params['pubDb']['host'] = servs[rawService]['host']
                        params['pubDb']['type'] = servs[rawService]['type']
                elif 'utilityMeasure' in config['basic']:
                    # This is a utility configuration
                    params['friendly']['utility'] = [
                        config['basic']['utilityMeasure'],
                        config['basic']['measureParam']]
            # pp.pprint(params)
            # Need to determine if the results have already been finished
            params['finished'] = False
            try:
                f = open(params['resultsPath'], 'r')
            except:
                # No prior results for this attack have been posted
                pass
            else:
                # Prior results have been posted. Make sure they are complete.
                res = json.load(f)
                if 'finished' in res:
                    params['finished'] = True
            new = copy.deepcopy(params)
            pmList.append(new)
    return pmList


def oldSetupGdaAttackParameters(pmList, criteria, attackType):
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
            baseName = str(f"{sys.argv[0].replace(os.path.sep, '-').strip('-')}")
            if 'anonType' in pm and len(pm['anonType']) > 0:
                baseName += str(f".{pm['anonType']}")
            if 'anonSubType' in pm and len(pm['anonSubType']) > 0:
                baseName += str(f".{pm['anonSubType']}")
            if 'dbType' not in pm or len(pm['dbType']) == 0:
                baseName += str(f".{pm['rawDb']}.{pm['anonDb']}")
            else:
                baseName += str(f".{pm['dbType']}")
            if 'table' in pm and len(pm['table']) > 0:
                baseName += str(f".{pm['table']}")
        else:
            baseName = pm['name']
        # Remove any spaces in the base name
        baseName.replace(" ", "")
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


def finishGdaAttack(params, score):
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
    for _ in range(2):
        try:
            f = open(resultsPath, 'w')
        except:
            if not os.path.exists(os.path.dirname(resultsPath)):
                os.mkdir(os.path.dirname(resultsPath))
                continue
            e = str(f"Failed to open {resultsPath} for write")
            sys.exit(e)
        else:
            break
    f.write(j)
    f.close()
    return final


def comma_ize(strings, lastComma=True):
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
    for val in range(1, len(columns) + 1):
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


def getInterpolatedValue(val0, val1, scoreGrid):
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
    yLegPart = val0 - tup0Below
    xLegPart = val1 - tup1Below
    hypoPart = math.sqrt((xLegPart ** 2) + (yLegPart ** 2))
    frac = hypoPart / hypoFull
    interpScore = scoreBelow - (frac * (scoreBelow - scoreAbove))
    return interpScore


def findSnappedRange(val):
    # simple brute force approach
    start = 1
    # find a starting value less than the target value val
    while (1):
        if start > val:
            start /= 10
        else:
            break
    # then find the pair of snapped values above and below val
    while (1):
        # the loop always starts at a power of 10
        below = start
        above = below * 2
        if ((below <= val) and (above >= val)):
            break
        below = start * 2
        above = start * 5
        if ((below <= val) and (above >= val)):
            break
        below = start * 5
        above = start * 10
        if ((below <= val) and (above >= val)):
            break
        start *= 10
    final = below
    if ((above - val) < (val - below)):
        final = above
    return final
