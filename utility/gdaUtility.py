import sys
import os
import pprint
import json

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
'''
_Log_File="../log/utility.log"

def createTimedRotatingLog():
    logger =logging.getLogger('RotatingLog')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s| %(levelname)s| %(message)s','%m/%d/%Y %I:%M:%S %p')
    handler = TimedRotatingFileHandler(_Log_File,when='midnight',interval=1,backupCount=0)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logging = createTimedRotatingLog()
'''
class gdaUtility:
    def __init__(self):
        self._ar={}


    #Method to calculate Utility Measure
    def _distinctUidUtilityMeasureSingleAndDoubleColumn(self,param):
        attack = gdaAttack(param)
        table = attack.getAttackTableName()
        rawColNames = attack.getColNames(dbType='rawDb')
        anonColNames = attack.getColNames(dbType='anonDb')
        if rawColNames is not None and anonColNames is not None:
            colNames = list(set(rawColNames) & set(anonColNames))
            self._ar['singleColumnScores'] = {}
            tableDictList=[]
            tableDictListMul=[]
            if colNames is not None:
                for colName in colNames:
                    sql = "SELECT "
                    sql += (colName)
                    if(param['measureParam']=="*"):
                        sql += str(f", count(*) FROM {table} ")
                    else:
                        sql += str(f", count( distinct {param['uid']}) FROM {table} ")
                    sql += makeGroupBy([colName])


                    query = dict(db="raw", sql=sql)
                    attack.askKnowledge(query)
                    reply = attack.getKnowledge()

                    #
                    if 'answer' not in reply:
                        print("ERROR: reply to claim query contains no answer")
                        pp.pprint(reply)
                        attack.cleanUp()
                        sys.exit()
                    rawDbrows=reply['answer']


                    query = dict(db="anon", sql=sql)
                    attack.askAttack(query)
                    reply = attack.getAttack()

                    if 'answer' not in reply:
                        print("ERROR: reply to claim query contains no answer")
                        pp.pprint(reply)
                        attack.cleanUp()
                        sys.exit()
                    anonDbrows=reply['answer']
                    anonDbrowsDict = {}
                    rawDbrowsDict = {}
                    for row in anonDbrows:
                        anonDbrowsDict[row[0]] = row[1]
                    for row in rawDbrows:
                        rawDbrowsDict[row[0]] = row[1]
                    covAndAccSingle= self._calAccuracyAndCoverage(rawDbrowsDict,anonDbrowsDict,[colName],param)
                    tableDictList.append(covAndAccSingle)

                    self.coverageAndAccuracyMUltipleCol(anonDbrows, attack, colName, colNames, query, rawDbrows,
                                                        tableDictListMul,table,param)

                self._ar['singleColumnScores']=tableDictList
                self._ar['doubleColumnScores']=tableDictListMul
                self._ar['tableStats']={}
                self._ar['tableStats']['colNames']=colNames
                attackResult = attack.getResults()
                self._ar['operational']=attackResult['operational']
        attack.cleanUp()


    #Method to calculate coverage and accuracy: MultipleColumns
    def coverageAndAccuracyMUltipleCol(self, anonDbrows, attack, colName, colNames, query, rawDbrows, tableDictListMul,table,param):
        for j in range(colNames.index(colName) + 1, len(colNames)):
            colNameMul = []
            colNameMul.append(colName)
            colNameMul.append(colNames[j])
            sql = "SELECT "
            sql += comma_ize(colNameMul, False)
            if (param['measureParam'] == "*"):
                sql += str(f", count(*) FROM {table} ")
            else:
                sql += str(f", count( distinct {param['uid']}) FROM {table} ")
            #sql += str(f", count( distinct {param['uid']}) FROM {table} ")
            sql += makeGroupBy(colNameMul)

            # Query using function provided by Attack.(Not working currently!!!)
            #print(sql)
            query['sql'] = sql
            query['db'] = "raw"
            attack.askKnowledge(query)
            reply = attack.getKnowledge()

            if 'answer' not in reply:
                print("ERROR: reply to claim query contains no answer")
                pp.pprint(reply)
                attack.cleanUp()
                sys.exit()
            for row in reply['answer']:
                rawDbrows = reply['answer']

            query['db'] = "anon"
            attack.askAttack(query)
            reply = attack.getAttack()

            if 'answer' not in reply:
                print("ERROR: reply to claim query contains no answer")
                pp.pprint(reply)
                attack.cleanUp()
                sys.exit()
            for row in reply['answer']:
                anonDbrows = reply['answer']

            anonDbrowsDict = {}
            rawDbrowsDict = {}
            for row in anonDbrows:
                anonDbrowsDict[str(row[0]) + '-' + str(row[1])] = row[2]
            for row in rawDbrows:
                rawDbrowsDict[str(row[0]) + '-' + str(row[1])] = row[2]
            coverAndAccuDouble = self._calAccuracyAndCoverage(rawDbrowsDict, anonDbrowsDict, colNameMul,param)
            tableDictListMul.append(coverAndAccuDouble)

    #Finish utility Measure: Write output to a file.
    def _finishGdaUtility(self,params):
        if 'finished' in params:
            del params['finished']
        final = {}
        final.update(self._ar)
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

    #Method to calculate Coverage and Accuracy
    def _calAccuracyAndCoverage(self,rawDbrowsDict,anonDbrowsDict,colNames,param):
        #logging.info('RawDb Dictionary and AnnonDb Dictionary: %s and %s', rawDbrowsDict, anonDbrowsDict)
        noColumnCountOnerawDb=0
        noColumnCountMorerawDb=0
        valuesInBoth=0
        accuracy=dict()
        coverage=dict()
        accuracyFlag=True
        for rawkey in rawDbrowsDict:
            if rawDbrowsDict[rawkey]==1:
                noColumnCountOnerawDb=noColumnCountOnerawDb+1
            else:
                noColumnCountMorerawDb=noColumnCountMorerawDb+1
        absoluteErrorList=[]
        simpleRelativeErrorList=[]
        relativeErrorList=[]
        for anonkey in anonDbrowsDict:
            if anonkey in rawDbrowsDict:
                if rawDbrowsDict[anonkey] >1:
                    valuesInBoth=valuesInBoth+1
                absoluteErrorList.append((abs(anonDbrowsDict[anonkey] - rawDbrowsDict[anonkey])))
                simpleRelativeErrorList.append((rawDbrowsDict[anonkey]/anonDbrowsDict[anonkey]))
                relativeErrorList.append((
                    (abs(anonDbrowsDict[anonkey] - rawDbrowsDict[anonkey])) / (max(anonDbrowsDict[anonkey], rawDbrowsDict[anonkey]))))
        valuesanonDb=len(anonDbrowsDict)
        absoluteError=0.0
        simpleRelativeError=0.0
        relativeError=0.0
        if(len(absoluteErrorList)>0):
            accuracyFlag=False
            for item in absoluteErrorList:
                absoluteError += item * item
            absoluteError=absoluteError/len(absoluteErrorList);
            for item in simpleRelativeErrorList:
                simpleRelativeError+=item*item
            simpleRelativeError=simpleRelativeError/len(simpleRelativeErrorList);


            for item in relativeErrorList:
                relativeError+=item*item
            relativeError=relativeError/len(relativeErrorList);
            accuracy['accuracy']={}
            accuracy['accuracy']['simpleRelativeErrorMetrics'] = {}
            accuracy['accuracy']['relativeErrorMetrics'] = {}

            absoluteDict={}
            absoluteDict['min']=min(absoluteErrorList)
            absoluteDict['max'] = max(absoluteErrorList)
            absoluteDict['avg'] = mean(absoluteErrorList)
            if (len(absoluteErrorList)>1):
                absoluteDict['stddev'] = stdev(absoluteErrorList)
            else:
                absoluteDict['stddev'] = None
            absoluteDict['meanSquareError'] = absoluteError

            if (param['measureParam']) == "*":
                absoluteDict['compute'] = "(count((*)rawDb)-count((*)anonDb))"
            else:
                absoluteDict['compute']="(count(distinct_rawDb)-count(distinct_anonDb))"
            accuracy['accuracy']['absolErrorMetrics']=absoluteDict

            #SimpleErrorRelativeDictionary
            simpleRelativeDict={}
            simpleRelativeDict['min'] = min(simpleRelativeErrorList)
            simpleRelativeDict['max'] = max(simpleRelativeErrorList)
            simpleRelativeDict['avg'] = mean(simpleRelativeErrorList)
            if(len(simpleRelativeErrorList)>1):
                simpleRelativeDict['stddev'] = stdev(simpleRelativeErrorList)
            else:
                simpleRelativeDict['stddev'] = None
            simpleRelativeDict['meanSquareError'] = simpleRelativeError
            if(param['measureParam'])=="*":
                simpleRelativeDict['compute'] = "(count(rawDb(*))/count(anonDb(*)))"
            else:
                simpleRelativeDict['compute'] = "(count(distinct_rawDb)/count(distinct_anonDb))"
            accuracy['accuracy']['simpleRelativeErrorMetrics'] = simpleRelativeDict

            #RelativeErrorDictionary
            relativeDict = {}
            relativeDict['min'] = min(relativeErrorList)
            relativeDict['max'] = max(relativeErrorList)
            relativeDict['avg'] = mean(relativeErrorList)
            if(len(relativeErrorList)>1):
                relativeDict['stddev'] = stdev(relativeErrorList)
            else:
                relativeDict['stddev'] = None

            relativeDict['meanSquareError'] = relativeError
            if (param['measureParam']) == "*":
                relativeDict[
                    'compute'] = "(abs(count((*)rawDb)-count((*)anonDb))/max(count((*)rawDb),count((*)anonDb)))"
            else:
                relativeDict['compute'] = "(abs(count(distinct_rawDb)-count(distinct_anonDb))/max(count(distinct_rawDb),count(distinct_anonDb)))"
            accuracy['accuracy']['relativeErrorMetrics'] = relativeDict
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
        if(accuracyFlag):
            accuracy['accuracy'] = None
        columnParam={}
        colPos=1
        for col in colNames:
            columnParam["col"+str(colPos)]=col
            colPos = colPos + 1
        columnParam.update(accuracy)
        columnParam.update(coverage)
        return columnParam

    #Method to query the database and returns the fetched tuples.
    def _queryDb(self, _noOftry, sql, x,query):
        for i in range(_noOftry):
            query['myTag'] = i
            x.askExplore(query)
        while True:
            answer = x.getExplore()
            # print(answer)
            tag = answer['query']['myTag']
            # print(f"myTag is {tag}")
            if answer['stillToCome'] == 0:
                break
        return answer



    def _setupGdautilityParameters(self,cmdArgs, criteria=''):
        """ Basic prep for input and output of running gdaUtility

            `cmdArgs` is the command line args list (`sys.argv`) <br/>
            `cmdArgs[1]` is either the name or the config file, or
            empty if the default config file name should be used. <br/>
            Returns a list of data structure that can be used for
            class `gdaUtility` <br/>
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
                    baseName += str(f".{pm['rawDb']}.{pm['anonDb']}")
                else:
                    baseName += str(f".{pm['dbType']}")
                if 'table' in pm and len(pm['table']) > 0:
                    baseName += str(f".{pm['table']}")
            else:
                baseName = pm['name']
            pm['name'] = baseName

            resultsDir = "utilityResults";
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




    #Method to generate SQL script for a datasource
    def generateDBSqlForTable(self,argv,dbType):
        paramsList=self._setupGdaUtilityParametersForSqlScripts(argv,criteria="singlingOut")

        mapToIntList={"bigint", "bytea","boolean","integer","int"}
        maptToTextList={"char","varchar","text","char"}
        mapToRealList={"real","decimal","doubleprecision","numeric"}
        maptoDateTime={"timestamp without time zone","time","timestamp","date"}

        #Debug
        #print (f"{paramsList}")
        for param in paramsList:
            if param['finished'] == True:
                print("The following Utility script for table has been executed:")
                pp.pprint(param)
                print(f"Results may be found at {param['resultsPath']}")
                continue
            attack = gdaAttack(param)
            table = attack.getAttackTableName()
            tableNames=attack.getTableNames(dbType=dbType)
            resultsPath = param['resultsPath']
            try:
                f = open(resultsPath, 'w')
            except:
                e = str(f"Failed to open {resultsPath} for write")
                sys.exit(e)
            for table in tableNames:
                #Table scheme
                createTable=f"create table {table}_char  (column_name text, column_type text, num_rows int," \
                            f"num_uids int, num_distinct_vals int, av_rows_per_vals real, av_uids_per_val real,std_rows_per_val real,std_uids_per_val real,max text, min text, column_label text);"
                colNames = attack.getColNamesAndTypes(dbType=dbType,tableName=table)

                f.write(createTable+'\n')

                num_rows=0

                #Query to get No of Rows
                sql = "SELECT "
                sql += str(f"count(*) FROM {table} ")
                answer = self.queryDB(attack, sql,dbType)
                for an in answer:
                    num_rows=an[0]

                #Query to get Number of distinct UID's
                num_uids = 0
                sql ="SELECT"
                sql+=str(f" count( distinct {param['uid']}) FROM {table}")
                answer = self.queryDB(attack, sql,dbType)
                for an in answer:
                    num_uids = an[0]
                for raCol in colNames:
                    column_name=raCol[0]
                    column_type=''
                    if raCol[1] in mapToIntList:
                        column_type+='int'
                    elif raCol[1] in maptToTextList:
                        column_type += 'text'
                    elif raCol[1] in mapToRealList:
                        column_type += 'real'
                    elif raCol[1] in maptoDateTime:
                        column_type += 'datetime'
                    num_distinct_vals=0
                    #Query to get distinct values of a column
                    sql = "SELECT "
                    sql += str(f"count ( distinct {raCol[0]}) FROM {table} ")
                    answer = self.queryDB(attack, sql,dbType)
                    for an in answer:
                        num_distinct_vals = an[0]

                    av_rows_per_val=num_rows/num_distinct_vals
                    av_uids_per_val=num_uids/num_distinct_vals
                    #std_rows_per_val std_uids_per_val max min
                    '''
                    select sf_flag,count (*)from rides  group by 1
                    '''
                    stdRowsPerVal=[]
                    stdUidsPerVal=[]
                    #Query to get Find standard deviation Per value of a column
                    sql="SELECT "
                    sql += (raCol[0])
                    sql += str(f",count(*) FROM {table} ")
                    sql += makeGroupBy([raCol[0]])
                    answer = self.queryDB(attack, sql,dbType)
                    for an in answer:
                        stdRowsPerVal.append(an[1])
                    if len(stdRowsPerVal) >1 :
                        std_rows_per_val=stdev(stdRowsPerVal)
                    else:
                        std_rows_per_val=-1
                    sql = "SELECT "
                    sql += (raCol[0])
                    sql += str(f",count(distinct {param['uid']}) FROM {table} ")
                    sql += makeGroupBy([raCol[0]])
                    answer = self.queryDB(attack, sql,dbType)
                    for an in answer:
                        stdUidsPerVal.append(an[1])
                    if len(stdUidsPerVal) >1 :
                        std_uids_per_val=stdev(stdUidsPerVal)
                    else:
                        std_uids_per_val = -1

                    #Max: and Min
                    maxi=''
                    mini=''
                    #Query to  find Max and Min(Single query).
                    sql="SELECT "
                    sql +=str(f"{raCol[0]} FROM {table}")
                    listOfValues=[]
                    answer = self.queryDB(attack, sql, dbType)
                    for an in answer:
                        listOfValues.append(an[0])
                    if not None in listOfValues:
                        maxi=max(listOfValues)
                        mini=min(listOfValues)

                    if column_type=='real' or column_type=='datetime':
                        columnlabel='continuous'
                    elif column_type=='text':
                        columnlabel='enumerative'
                    elif column_type=='int':
                        if num_distinct_vals<100:
                            columnlabel='enumerative'
                        else:
                            x=(int(maxi)-int(mini))/10
                            sql = "SELECT "
                            sql += str(f"floor (({raCol[0]})/{x})*{x}")
                            sql += str(f",count(*) FROM {table} ")
                            sql += makeGroupBy([1])
                            answer = self.queryDB(attack, sql,dbType)
                            countList=[]
                            for an in answer:
                                countList.append(an[1])
                            minInList=min(countList)
                            averageOfList=sum(countList)/len(countList)
                            if minInList<0.5*averageOfList:
                                columnlabel='continuous'
                            else:
                                columnlabel = 'enumerative'
                    if(maxi=='' and mini==''):
                        insert = f"insert into {table}_char values (\'{column_name}\',\'{column_type}\',{num_rows},{num_uids},{num_distinct_vals},{av_rows_per_val},{av_uids_per_val}," \
                                 f"{std_rows_per_val},{std_uids_per_val},'','',\'{columnlabel}\');"
                    else:
                        insert=f"insert into {table}_char values (\'{column_name}\',\'{column_type}\',{num_rows},{num_uids},{num_distinct_vals},{av_rows_per_val},{av_uids_per_val}," \
                           f"{std_rows_per_val},{std_uids_per_val},\'{maxi}\',\'{mini}\',\'{columnlabel}\');"


                    f.write(insert+'\n')
            attack.cleanUp()


 #Method to query Database based on dbType
    def queryDB(self, attack, sql,dbType):
        if dbType=='anonDb':
            query = dict(db="anon", sql=sql)
            attack.askAttack(query)
            reply = attack.getAttack()
        elif dbType=='rawDb':
            query = dict(db="raw", sql=sql)
            attack.askKnowledge(query)
            reply = attack.getKnowledge()
        if 'answer' not in reply:
            print("ERROR: reply to claim query contains no answer")
            pp.pprint(reply)
            attack.cleanUp()
            sys.exit()
        answer = reply['answer']
        return answer

    #setup parameters to generate DbScripts
    def _setupGdaUtilityParametersForSqlScripts(self, cmdArgs, criteria=''):
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
                baseName =""
                print(f"{sys.argv[0]}")
                if 'table' in pm and len(pm['table']) > 0:
                    baseName += str(f"{pm['table']}")
            else:
                baseName = pm['name']
            pm['name'] = baseName

            resultsDir = "utilitySqlScripts";
            if 'resultsDir' in pm and len(pm['resultsDir']) > 0:
                resultsDir = pm['resultsDir']

            resultsPath = resultsDir + "/" + baseName + ".sql"
            pm['resultsPath'] = resultsPath
            pm['finished'] = False
            try:
                f = open(resultsPath, 'r')
            except:
                # No prior results for this utility measure have been posted
                pass
            else:
                # Prior results have been posted. Make sure they are complete.

                print(f"Already Executed")
                sys.exit()

        return pmList









