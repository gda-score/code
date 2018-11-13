#sys.path.append('../common')
#from gdaScore import gdaAttack
import sys
sys.path.append('../common')
from gdaUtilities import comma_ize,makeGroupBy
from gdaScore import gdaAttack
from statistics import mean,median,stdev
import pprint
import json
import os
pp = pprint.PrettyPrinter(indent=4)

class gdaTool:
# Method to generate SQL script for a datasource
    _p = dict(dbConfig = "common/config/myDatabases.json")

    def generateDBSqlForTable(self, argv, dbType):
        paramsList = self._setupGdaUtilityParametersForSqlScripts(argv, criteria="singlingOut")
        #Create a dictionary for mapping rather than
        mappingDBTypesDict={"bigint":"int","bytea":"int","boolean":"int","integer":"int","int":"int","smallint":"int",
                            "char":"text","varchar":"text","text":"text","char":"text","character varying":"text",
                            "real":"real","decimal":"real","double precision":"real","numeric":"real",
                            "timestamp without time zone":"datetime","time":"datetime","timestamp":"datetime",
                            "date":"date"

        }

        for param in paramsList:
            if param['finished'] == True:
                print("The following Utility script for table has been executed:")
                pp.pprint(param)
                print(f"Results may be found at {param['resultsPath']}")
                continue
            #Add mandatory fields required for now. Have remove once scope of these parameters are changed.
            path = self._p['dbConfig']
            for x in range(5):
                path = "../" + path
                if os.path.isfile(path):
                    break
                pass
            fh = open(path, "r")
            j = json.load(fh)
            for key in j:
                param['anonDb'] = key
            param['criteria']="singlingOut"
            param['table']="dummy"

            attack = gdaAttack(param)
            table = attack.getAttackTableName()
            tableNames = attack.getTableNames(dbType=dbType)
            resultsPath = param['resultsPath']
            try:
                f = open(resultsPath, 'w')
            except:
                e = str(f"Failed to open {resultsPath} for write")
                sys.exit(e)
            for table in tableNames:
                # Table scheme
                createTable = f"create table {table}_char  (column_name text, column_type text, num_rows int," \
                              f"num_uids int, num_distinct_vals int, av_rows_per_vals real, av_uids_per_val real,std_rows_per_val real,std_uids_per_val real,max text, min text, column_label text);"
                colNames = attack.getColNamesAndTypes(dbType=dbType, tableName=table)
                print(f" column names: {colNames}")
                f.write(createTable + '\n')

                num_rows = 0


                sql = "SELECT "
                sql += str(f"count(*) FROM {table} ")
                answer = self.queryDB(attack, sql, dbType)
                for an in answer:
                    num_rows = an[0]

                # Query to get Number of distinct UID's
                num_uids = 0
                sql = "SELECT"
                sql += str(f" count( distinct uid) FROM {table}")
                answer = self.queryDB(attack, sql, dbType)
                for an in answer:
                    num_uids = an[0]
                for raCol in colNames:
                    column_name = raCol[0]
                    column_type = ''

                    if raCol[1] in mappingDBTypesDict.keys():
                        mappedDBType=mappingDBTypesDict.get(raCol[1])
                        column_type +=mappedDBType

                    num_distinct_vals = 0
                    # Query to get distinct values of a column
                    sql = "SELECT "
                    sql += str(f"count ( distinct {raCol[0]}) FROM {table} ")
                    answer = self.queryDB(attack, sql, dbType)
                    for an in answer:
                        num_distinct_vals = an[0]

                    av_rows_per_val = num_rows / num_distinct_vals
                    av_uids_per_val = num_uids / num_distinct_vals
                    # std_rows_per_val std_uids_per_val max min
                    '''
                    select sf_flag,count (*)from rides  group by 1
                    '''
                    stdRowsPerVal = []
                    stdUidsPerVal = []
                    # Query to get Find standard deviation Per value of a column
                    sql = "SELECT "
                    sql += (raCol[0])
                    sql += str(f",count(*) FROM {table} ")
                    sql += makeGroupBy([raCol[0]])
                    answer = self.queryDB(attack, sql, dbType)
                    for an in answer:
                        stdRowsPerVal.append(an[1])
                    if len(stdRowsPerVal) > 1:
                        std_rows_per_val = stdev(stdRowsPerVal)
                    else:
                        std_rows_per_val = -1
                    sql = "SELECT "
                    sql += (raCol[0])
                    sql += str(f",count(distinct uid) FROM {table} ")
                    sql += makeGroupBy([raCol[0]])
                    answer = self.queryDB(attack, sql, dbType)
                    for an in answer:
                        stdUidsPerVal.append(an[1])
                    if len(stdUidsPerVal) > 1:
                        std_uids_per_val = stdev(stdUidsPerVal)
                    else:
                        std_uids_per_val = -1

                    # Max: and Min
                    maxi = ''
                    mini = ''
                    # Query to  find Max and Min(Single query).
                    sql = "SELECT "
                    sql += str(f"{raCol[0]} FROM {table}")
                    listOfValues = []
                    answer = self.queryDB(attack, sql, dbType)
                    for an in answer:
                        listOfValues.append(an[0])
                    if not None in listOfValues:
                        maxi = max(listOfValues)
                        mini = min(listOfValues)
                    continousDBTypeList=["real","datetime","date"]
                    enumerateDBTypeList=["text"]
                    if column_type in continousDBTypeList:
                        columnlabel = 'continuous'
                    elif column_type in enumerateDBTypeList:
                        columnlabel = 'enumerative'
                    elif column_type == 'int':
                        if num_distinct_vals < 100:
                            columnlabel = 'enumerative'
                        else:
                            x = (int(maxi) - int(mini)) / 10
                            sql = "SELECT "
                            sql += str(f"floor (({raCol[0]})/{x})*{x}")
                            sql += str(f",count(*) FROM {table} ")
                            sql += makeGroupBy([1])
                            answer = self.queryDB(attack, sql, dbType)
                            countList = []
                            for an in answer:
                                countList.append(an[1])
                            minInList = min(countList)
                            averageOfList = sum(countList) / len(countList)
                            if minInList < (0.5 * averageOfList):
                                columnlabel = 'continuous'
                            else:
                                columnlabel = 'enumerative'
                    '''
                    if (maxi == '' and mini == ''):
                        insert = f"insert into {table}_char values (\'{column_name}\',\'{column_type}\',{num_rows},{num_uids},{num_distinct_vals},{av_rows_per_val},{av_uids_per_val}," \
                                 f"{std_rows_per_val},{std_uids_per_val},'','',\'{columnlabel}\');"
                    else:
                    '''
                    insert = f"insert into {table}_char values (\'{column_name}\',\'{column_type}\',{num_rows},{num_uids},{num_distinct_vals},{av_rows_per_val},{av_uids_per_val}," \
                                 f"{std_rows_per_val},{std_uids_per_val},\'{maxi}\',\'{mini}\',\'{columnlabel}\');"

                    f.write(insert + '\n')
            attack.cleanUp()


    # Method to query Database based on dbType
    def queryDB(self, attack, sql, dbType):
        query = dict(db=dbType, sql=sql)
        attack.askKnowledge(query)
        reply = attack.getKnowledge()
        if 'answer' not in reply:
            print("ERROR: reply to claim query contains no answer")
            pp.pprint(reply)
            attack.cleanUp()
            sys.exit()
        answer = reply['answer']
        return answer


    # setup parameters to generate DbScripts
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
                baseName = ""
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

                print(f"Already Executed and file is found at {resultsPath}")
                sys.exit()

        return pmList
