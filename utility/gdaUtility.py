import sys
import os
import pprint
import json
import copy
import random
sys.path.append('../common')
from gdaQuery import findQueryConditions
from gdaUtilities import getDatabaseInfo

#sys.path.append(os.path.abspath(r'../../code-master'))
#from library.gdaScore import gdaAttack
#from common.gdaScore import gdaAttack
#from common.gdaUtilities import comma_ize,makeGroupBy

sys.path.append('../common')
from gdaScore import gdaAttack
from gdaUtilities import comma_ize,makeGroupBy

from logging.handlers import TimedRotatingFileHandler
import  logging
from statistics import mean,median,stdev

pp = pprint.PrettyPrinter(indent=4)
# '''
# _Log_File="../log/utility.log"
# 
# def createTimedRotatingLog():
#     logger =logging.getLogger('RotatingLog')
#     logger.setLevel(logging.INFO)
#     formatter = logging.Formatter('%(asctime)s| %(levelname)s| %(message)s','%m/%d/%Y %I:%M:%S %p')
#     handler = TimedRotatingFileHandler(_Log_File,when='midnight',interval=1,backupCount=0)
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
#     return logger
# 
# logging = createTimedRotatingLog()
# '''
class gdaUtility:
    def __init__(self):
        '''Measures the utility of anonymization methods.

           See `distinctUidUtilityMeasureSingleAndDoubleColumn()` for
           details on how to run. <br/>
           Currently limited to simple count of distinct users.
        '''
        self._ar={}
        self._p = False
        self._nonCoveredDict = dict(accuracy=None,col1="TBD",
                coverage=dict(colCountManyRawDb=None,
                    colCountOneRawDb=None,
                    coveragePerCol=0.0,
                    totalValCntAnonDb=None,
                    valuesInBothRawAndAnonDb=None))
        self._rangeDict = dict(col1="TBD",
                coverage=dict(colCountManyRawDb=None,
                    colCountOneRawDb=None,
                    coveragePerCol="TBD",
                    totalValCntAnonDb=None,
                    valuesInBothRawAndAnonDb=None))

    def _getWorkingColumns(self,tabChar,allowedColumns):
        # I'd like to work with a good mix of data types (numeric, datetime,
        # and text), i.e. targetCols. Also try to get a few with the most
        # distinct values because this gives us more flexibility
        print("getWorkingCOlumns")
        targetCols = 8
        columns = []
        tuples = []
        # Start by putting the desired number of numeric columns in the list
        pp.pprint(tabChar)
        for col in tabChar:
            if (((tabChar[col]['column_type'] == "real") or
                    ((tabChar[col]['column_type'][:3] == "int"))) and
                    (col in allowedColumns)):
                tuples.append([col,tabChar[col]['num_distinct_vals']])
        ordered = sorted(tuples, key=lambda t: t[1], reverse=True)
        for i in range(len(ordered)):
            if i >= targetCols:
                break
            columns.append(ordered[i][0])
            print(f"i is {i}")
            print(columns)
        # Then datetime
        tuples = []
        for col in tabChar:
            if (tabChar[col]['column_type'][:4] == "date" and
                    col in allowedColumns):
                tuples.append([col,tabChar[col]['num_distinct_vals']])
        ordered = sorted(tuples, key=lambda t: t[1], reverse=True)
        for i in range(len(ordered)):
            if i >= targetCols:
                break
            columns.append(ordered[i][0])
        # Then text
        tuples = []
        for col in tabChar:
            if (tabChar[col]['column_type'] == "text" and
                    col in allowedColumns):
                tuples.append([col,tabChar[col]['num_distinct_vals']])
        ordered = sorted(tuples, key=lambda t: t[1], reverse=True)
        for i in range(len(ordered)):
            if i >= targetCols:
                break
            columns.append(ordered[i][0])
        return columns

    def _getQueryStats(self,queries,ranges):
        qs = {}
        qs['totalQueries'] = len(queries)
        single = {}
        double = {}
        sizes = {}
        totalSingleColumn = 0
        totalDoubleColumn = 0
        for q in queries:
            if len(q['info']) == 1:
                totalSingleColumn += 1
                col = q['info'][0]['col']
                if col in single:
                    single[col] += 1
                else:
                    single[col] = 1
            else:
                totalDoubleColumn += 1
                key = q['info'][0]['col'] + ':' + q['info'][1]['col']
                if key in double:
                    double[key] += 1
                else:
                    double[key] = 1
            size = q['bucket'][-1]
            if self._p: print(f"bucket {q['bucket']}, size {size}")
            for ran in ranges:
                if size >= ran[0] and size < ran[1]:
                    key = str(f"{ran[0]}-{ran[1]}")
                    if key in sizes:
                        sizes[key] += 1
                    else:
                        sizes[key] = 1
        qs['singleColumn'] = {}
        qs['singleColumn']['totalQueries'] = totalSingleColumn
        qs['singleColumn']['stats'] = single
        qs['doubleColumn'] = {}
        qs['doubleColumn']['totalQueries'] = totalDoubleColumn
        qs['doubleColumn']['stats'] = double
        qs['ranges'] = sizes
        return qs

    def _measureAccuracy(self,param,attack,tabChar,table,uid,allowedColumns):
        ranges = param['ranges']
        numSamples = param['samples']
        numColumns = [1,2]
        columns = self._getWorkingColumns(tabChar,allowedColumns)
        for col in columns:
            if col in allowedColumns:
                print(f"Column {col} should not be chosen ({allowedColumns})")
        queries = []
        for rang in ranges:
            for nc in numColumns:
                cond = []
                q = findQueryConditions(param, attack, columns, allowedColumns,
                        rang[0], rang[1], numColumns=nc)
                pp.pprint(q)
                while(1):
                    res = q.getNextWhereClause()
                    if res is None:
                        break
                    cond.append(res)
                # shuffle the query conditions we found and take the first
                # <numSamples> ones
                random.shuffle(cond)
                queries += cond[:numSamples]
                if self._p: pp.pprint(queries)
                if self._p: print(f"Num queries = {len(queries)}")
        # Now go through and make queries for both raw and anon DBs, and
        # record the difference
        anonDb = getDatabaseInfo(param['anonDb'])
        for query in queries:
            sql = str(f"SELECT count(DISTINCT {uid}) FROM {table} ")
            sql += query['whereClausePostgres']
            rawAns = self._doExplore(attack,"raw",sql)
            sql = str(f"SELECT count(DISTINCT {uid}) FROM {table} ")
            if anonDb['type'] == 'aircloak':
                sql += query['whereClauseAircloak']
            else:
                sql += query['whereClausePostgres']
            anonAns = self._doExplore(attack,"anon",sql)
            query['raw'] = rawAns[0][0]
            query['anon'] = anonAns[0][0]

        queryStats = self._getQueryStats(queries,ranges)
        accScore = {}
        accScore['queries'] = queryStats
        accScore['accuracy'] = self._calAccuracy(queries,param)
        if self._p: pp.pprint(accScore)
        return accScore

    def _measureCoverage(self,param,attack,tabChar,table,
            rawColNames,anonColNames):
        # Here I only look at individual columns,
        # making the assumption that if I can query an individual column,
        # then I can also query combinations of columns.

        # Each entry in this list is for one column
        coverageScores=[]
        for colName in rawColNames:
            # These hold the query results or indication of lack thereof
            rawDbrowsDict = {}
            anonDbrowsDict = {}
            # There are couple conditions under which the column can be
            # considered not covered at all.
            if colName not in anonColNames:
                # Column doesn't even exist
                entry = copy.deepcopy(self._nonCoveredDict)
                entry['col1'] = colName
                coverageScores.append(entry)
                continue
            else:
                # See how much of the column is NULL
                sql = str(f"SELECT count({colName}) FROM {table}")
                rawAns = self._doExplore(attack,"raw",sql)
                anonAns = self._doExplore(attack,"anon",sql)
                numRawRows = rawAns[0][0]
                numAnonRows = anonAns[0][0]
                if numAnonRows == 0:
                    # Column is completely NULL
                    entry = copy.deepcopy(self._nonCoveredDict)
                    entry['col1'] = colName
                    coverageScores.append(entry)
                    continue

            # Ok, there is an anonymized column. 
            if tabChar[colName]['column_label'] == 'continuous':
                # If a column is continuous, then in any event it can be
                # completely covered with range queries, though only if
                # range queries are possible
                rangePossible = 1
                # TODO: Here we put checks for any anonymization types that
                # don't have range queries. For now there are no such.
                # if (param['anonType'] == 'foobar':
                if rangePossible:
                    entry = copy.deepcopy(self._rangeDict)
                    entry['col1'] = colName
                    entry['coverage']['coveragePerCol'] = numAnonRows/numRawRows
                    coverageScores.append(entry)
                    continue
                else:
                    pass

            # Ok, the anonymized column is not covered by a range (either
            # enumerative or no range function exists), so query the DB to
            # evaluate coverage
            sql = "SELECT "
            sql += (colName)
            if(param['measureParam']=="rows"):
                sql += str(f", count(*) FROM {table} ")
            else:
                sql += str(f", count( distinct {param['uid']}) FROM {table} ")
            sql += makeGroupBy([colName])

            rawDbrows = self._doExplore(attack,"raw",sql)
            anonDbrows = self._doExplore(attack,"anon",sql)

            for row in anonDbrows:
                anonDbrowsDict[row[0]] = row[1]
            for row in rawDbrows:
                rawDbrowsDict[row[0]] = row[1]
            coverageEntry = self._calCoverage(rawDbrowsDict,
                    anonDbrowsDict,[colName],param)
            coverageScores.append(coverageEntry )
        return coverageScores

    def _getAllowedColumns(self,coverageScores):
        # This removes any columns with a coverage score of 0. Such columns
        # either don't exist, or are all NULL. Either way, we can't measure
        # their accuracy
        allowedColumns = []
        for cov in coverageScores:
            if (cov['coverage']['coveragePerCol'] is None or
                    cov['coverage']['coveragePerCol'] > 0.001):
                # (I'm just a bit wary of true zero comparisons)
                allowedColumns.append(cov['col1'])
        return allowedColumns

    #Method to calculate Utility Measure
    def distinctUidUtilityMeasureSingleAndDoubleColumn(self,param):
        """ Measures coverage and accuracy.

            `param` is a single data structure from the list of structures
            returned by setupGdaUtilityParameters(). The contents of
            `param` are read from the configuration file. The elements
            of the configuration file are as follows: <br/>
            `name`: The basis for the name of the output json file. Should
            be unique among all measures. <br/>
            `rawDb`: The raw (non-anonymized) database used. <br/>
            `anonDb`: The anonymized database to use. <br/>
            `table`: The name of the table in the database. <br/>
            `anonType`: The type of anonymization (this appears in the
            GDA Score diagram but does not otherwise affect operation). <br/>
            `anonSubType`: Also appears in the GDA Score diagram. <br/>
            `uid`: The name of the uid column. <br/>
            `measureParam`: The thing that gets measured. Only current value
            is "uid", which indicates that counts of distinct uids should
            be measured. <br/>
            `samples`: States the number of samples over which each utility
            group should be measured. <br/>
            `ranges`: A list of ranges. Each range specifies the lower and
            upper bound on the number of "things" that an answer should
            contain as specified by `measureParam`. <br/>
        """
        attack = gdaAttack(param)
        table = attack.getAttackTableName()
        uid = attack.getUidColName()
        rawColNames = attack.getColNames(dbType='rawDb')
        anonColNames = attack.getColNames(dbType='anonDb')
        # Get table characteristics. This tells us if a given column is
        # enumerative or continuous.
        tabChar = attack.getTableCharacteristics()
        if self._p: pp.pprint(tabChar)
        coverageScores = self._measureCoverage(param,attack,tabChar,table,
                rawColNames,anonColNames)
        allowedColumns = self._getAllowedColumns(coverageScores)
        pp.pprint(coverageScores)
        pp.pprint(allowedColumns)

        accuracyScores = self._measureAccuracy(param,attack,tabChar,
                table,uid,allowedColumns)
        self._ar['coverage']=coverageScores
        self._ar['accuracy']=accuracyScores
        self._ar['tableStats'] = tabChar
        attackResult = attack.getResults()
        self._ar['operational']=attackResult['operational']
        attack.cleanUp()

    #Finish utility Measure: Write output to a file.
    def finishGdaUtility(self,params):
        """ Writes the utility scores to the output json file.
        """
        if 'finished' in params:
            del params['finished']
        final = {}
        final.update(self._ar)
        final['params'] = params
        final['finished'] = True
        j = json.dumps(final, sort_keys=True, indent=4)
        resultsPath = params['resultsPath']

        directory=os.path.dirname(resultsPath)
        if not os.path.exists(directory):
            e = str(f"Directory doesn't exists in the {resultsPath} to create a file. Create a directory")
            sys.exit(e)

        try:
            f = open(resultsPath, 'w')
        except:
            e = str(f"Failed to open {resultsPath} for write")
            sys.exit(e)

        f.write(j)
        f.close()
        return final

    def _calCoverage(self,rawDbrowsDict,anonDbrowsDict,colNames,param):
        #logging.info('RawDb Dictionary and AnnonDb Dictionary: %s and %s', rawDbrowsDict, anonDbrowsDict)
        noColumnCountOnerawDb=0
        noColumnCountMorerawDb=0
        valuesInBoth=0
        coverage=dict()
        for rawkey in rawDbrowsDict:
            if rawDbrowsDict[rawkey]==1:
                noColumnCountOnerawDb += 1
            else:
                noColumnCountMorerawDb += 1
        for anonkey in anonDbrowsDict:
            if anonkey in rawDbrowsDict:
                if rawDbrowsDict[anonkey] >1:
                    valuesInBoth += 1
        valuesanonDb=len(anonDbrowsDict)

        #Coverage Metrics
        coverage['coverage'] = {}
        coverage['coverage']['colCountOneRawDb']=noColumnCountOnerawDb
        coverage['coverage']['colCountManyRawDb']=noColumnCountMorerawDb
        coverage['coverage']['valuesInBothRawAndAnonDb']=valuesInBoth
        coverage['coverage']['totalValCntAnonDb']=valuesanonDb
        if(noColumnCountMorerawDb==0):
            coverage['coverage']['coveragePerCol'] =None
        else:
            coverage['coverage']['coveragePerCol']=valuesInBoth/noColumnCountMorerawDb
        columnParam={}
        colPos=1
        for col in colNames:
            columnParam["col"+str(colPos)]=col
            colPos = colPos + 1
        columnParam.update(coverage)
        return columnParam


    #Method to calculate Coverage and Accuracy
    def _calAccuracy(self,queries,param):
        #logging.info('RawDb Dictionary and AnnonDb Dictionary: %s and %s', rawDbrowsDict, anonDbrowsDict)
        accuracy=dict()
        absErrorList=[]
        simpleRelErrorList=[]
        relErrorList=[]
        for q in queries:
            if q['anon'] == 0:
                continue
            absErrorList.append((abs(q['anon'] - q['raw'])))
            simpleRelErrorList.append((q['raw']/q['anon']))
            relErrorList.append((
                    (abs(q['anon'] - q['raw'])) / (max(q['anon'], q['raw']))))
        absError=0.0
        simpleRelError=0.0
        relError=0.0
        for item in absErrorList:
            absError += item * item
        absError=absError/len(absErrorList);
        for item in simpleRelErrorList:
            simpleRelError+=item*item
        simpleRelError=simpleRelError/len(simpleRelErrorList);
        for item in relErrorList:
            relError+=item*item
        relError=relError/len(relErrorList);
        accuracy={}
        accuracy['simpleRelErrorMetrics'] = {}
        accuracy['relErrorMetrics'] = {}

        absDict={}
        absDict['min']=min(absErrorList)
        absDict['max'] = max(absErrorList)
        absDict['avg'] = mean(absErrorList)
        if (len(absErrorList)>1):
            absDict['stddev'] = stdev(absErrorList)
        else:
            absDict['stddev'] = None
        absDict['meanSquareError'] = absError

        if (param['measureParam']) == "rows":
            absDict['compute'] = "(count((*)rawDb)-count((*)anonDb))"
        else:
            absDict['compute']="(count(distinct_rawDb)-count(distinct_anonDb))"
        accuracy['absolErrorMetrics']=absDict

        #SimpleErrorRelDictionary
        simpleRelDict={}
        simpleRelDict['min'] = min(simpleRelErrorList)
        simpleRelDict['max'] = max(simpleRelErrorList)
        simpleRelDict['avg'] = mean(simpleRelErrorList)
        if(len(simpleRelErrorList)>1):
            simpleRelDict['stddev'] = stdev(simpleRelErrorList)
        else:
            simpleRelDict['stddev'] = None
        simpleRelDict['meanSquareError'] = simpleRelError
        if(param['measureParam'])=="rows":
            simpleRelDict['compute'] = "(count(rawDb(*))/count(anonDb(*)))"
        else:
            simpleRelDict['compute'] = "(count(distinct_rawDb)/count(distinct_anonDb))"
        accuracy['simpleRelErrorMetrics'] = simpleRelDict

        #RelErrorDictionary
        relDict = {}
        relDict['min'] = min(relErrorList)
        relDict['max'] = max(relErrorList)
        relDict['avg'] = mean(relErrorList)
        if(len(relErrorList)>1):
            relDict['stddev'] = stdev(relErrorList)
        else:
            relDict['stddev'] = None

        relDict['meanSquareError'] = relError
        if (param['measureParam']) == "rows":
            relDict[
                'compute'] = "(abs(count((*)rawDb)-count((*)anonDb))/max(count((*)rawDb),count((*)anonDb)))"
        else:
            relDict['compute'] = "(abs(count(distinct_rawDb)-count(distinct_anonDb))/max(count(distinct_rawDb),count(distinct_anonDb)))"
        accuracy['relErrorMetrics'] = relDict

        return accuracy

    def setupGdaUtilityParameters(self,cmdArgs):
        """ Basic prep for input and output of running gdaUtility

            `cmdArgs` is the command line args list (`sys.argv`) <br/>
            `cmdArgs[1]` is either the name of the config file, or
            empty if the default config file name should be used. <br/>
            Returns a list of data structures that can be used for
            class `gdaUtility` <br/>
            Adds the following to the returned data structure (`par`) <br/>
            `par['finished']`: `True` if attack previously completed.
            Else `False` <br/>
            `par['resultsPath']`: Path to filename where results should
            be stored. <br/>
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
            if 'verbose' in pm:
                self._p = pm['verbose']
            # The following prevents me from getting verbose output from
            # all the queries to gdaScore(). Limits verbose output to just
            # gdaUtility()
            #pm['verbose'] = False

            baseName = ""

            if 'rawDb' in pm and len(pm['rawDb']) > 0:
                rawdbName=getDatabaseInfo(pm['rawDb'])['dbname']
                baseName+=str(f"{rawdbName}")
            if 'table' in pm and len(pm['table']) > 0:
                baseName += str(f".{pm['table']}")
            if 'anonDb' in pm or len(pm['anonDb']) > 0:
                anondbName = getDatabaseInfo(pm['anonDb'])['dbname']
                baseName += str(f"_{anondbName}.{pm['table']}")
            if 'measureParam' in pm and len(pm['measureParam']) > 0:
                baseName += str(f".{pm['measureParam']}")
            pm['name'] = baseName
            if 'samples' not in pm:
                pm['samples'] = 25
            if 'ranges' not in pm:
                pm['ranges'] = [[10,50],[50,100],[100,500],
                        [500,1000],[1000,5000]]


            #Default path if resultDir is not sepcified in the parameters.
            resultsDir = "../../scores/utility";

            if 'resultsDir' in pm and len(pm['resultsDir']) > 0:
                resultsDir = pm['resultsDir']

            resultsPath = resultsDir + "/" + baseName + ".json"
            pm['resultsPath'] = resultsPath
            pm['finished'] = False
            try:
                f = open(resultsPath, 'r')
            except:
                # No prior results for this utility measure have been posted
                pass
            else:
                # Prior results have been posted. Make sure they are complete.
                res = json.load(f)
                if 'finished' in res:
                    pm['finished'] = True

        return pmList

    def _doExplore(self,attack,db,sql):
        query = dict(db=db, sql=sql)
        if self._p: print(sql)
        print(sql)   
        attack.askExplore(query)
        reply = attack.getExplore()
        if 'answer' not in reply:
            print("ERROR: reply contains no answer")
            pp.pprint(reply)
            attack.cleanUp()
            sys.exit()
        return reply['answer']
