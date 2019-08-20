import sqlite3
import simplejson
import psycopg2
import queue
import threading
import sys
import os
import copy
import base64
import time
import pprint
import datetime
from .gdaUtilities import getInterpolatedValue,getDatabaseInfo

class gdaScores:
    """Computes the final GDA Score from the scores returned by gdaAttack

       See __init__ for input parameters. <br/>
       WARNING: this code is fragile, and can fail ungracefully."""

    # ar (AttackResults) contains the combined results from one or more
    # addResult calls. Values like confidence scores are added in.
    _ar = {}
    def __init__(self, result=None):
        """Initializes state for class `gdaScores()`
           
           `result` is the data structure returned by
           `gdaAttack.getResults()`"""
        self._ar = {}
        if result:
            self.addResult(result)

    def addResult(self, result):
        """ Adds first result or combines result with existing results

            `result` is the data returned by `gdaAttack.getResults()` <br/>
            Returns True if add succeeded, False otherwise"""

        # Check that results are meaningfully combinable
        if 'attack' in self._ar:
            if result['attack'] != self._ar['attack']:
                return False
        else:
            # No result yet assigned, so nothing to update
            self._ar = result
            self._computeConfidence()
            self._assignDefaultSusceptability()
            self._computeDefense()
            return True

        # Result has been assigned, so need to update
        # Add in base results
        for key in self._ar['base']:
            self._ar['base'][key] += result['base'][key]
        # Add in column results
        for col,data in result['col'].items():
            for key,val in data:
                self._ar['col'][col][key] += val
        self._computeConfidence()
        self._assignDefaultSusceptability()
        return True

    def assignColumnSusceptibility(self,column,susValue):
        """ Assigns a susceptibility value to the column

            By default, value will already be 1 (fully susceptible),
            so only need to call this if you wish to assign a different
            value. <br/>
            `column` is the name of the column being assigned to. <br/>
            `susValue` can be any value between 0 and 1 <br/>

            returns False if failed to assign"""
        # following conversion because later the '-' function requires is
        susValue = float(susValue)
        if column not in self._ar['col']:
            return False
        if susValue < 0 or susValue > 1:
            return False
        self._ar['col'][column]['columnSusceptibility'] = susValue
        return True

    def getScores(self,method='mpi_sws_basic_v1',numColumns=-1):
        """ Returns all scores, both derived and attack generated
        
            getScores() may be called multiple times with different
            scoring methods or numColumns. For each such call an additional
            score will be added. <br/> <br/>
            `method` is the scoring algorithm (currently only one,
            'mpi_sws_basic_v'). <br/>
            Derives a score from the `numColumns` columns with the
            weakest defense score. Uses all attacked columns if
            numColumns omitted. `numColumns=1` will give the worst-case
            score (weakest defense), while omitting `numColumns` will
            usually produce a stronger defense score."""
        if numColumns == -1:
            numColumns = len(self._ar['col'])
        if method == 'mpi_sws_basic_v1':
            self._computeMpiSwsBasicV1Scores(numColumns)
        # First compute the individual column defense values
        return self._ar

    # ------------------ Private Methods ------------------------

    # The following list organized as (conf,prob,score), where conf is
    # confidence improvement, prob is probability of making a claim, and
    # score is the composite score. The list is in order of worst score
    # (0) to best score (1).  The idea is to step through the list until
    # the best score is obtained. This is used by the MPI-SWS basic score
    _defenseGrid1 = [
            (1,1,0),(1,.01,.1),(1,.001,.3),(1,.0001,.7),(1,.00001,1),
            (.95,1,.1),(.95,.01,.3),(.95,.001,.7),(.95,.0001,.8),(.95,.00001,1),
            (.90,1,.3),(.90,.01,.6),(.90,.001,.8),(.90,.0001,.9),(.90,.00001,1),
            (.75,1,.7),(.75,.01,.9),(.75,.001,.95),(.75,.0001,1),(.75,.00001,1),
            (.50,1,.95),(.50,.01,.95),(.50,.001,1),(.50,.0001,1),(.5,.00001,1),
            (0,1,1),(0,.01,1),(0,.001,1),(0,.0001,1),(0,.00001,1)
            ]
    # This is organized by susceptibility score and multipliticative factor
    # on the overall score
    _suscList1 = [(1.0,1.0),(0.01,0.95),(0.001,0.9),(0.0001,0.6),
            (0.00001,0.3),(0.000001,0.1),(0.0,0.0)
            ]

    def _appendScoreToScores(self,sc):
        if 'scores' not in self._ar:
            self._ar['scores'] = []
        self._ar['scores'].append(sc)
        return

    def _computeMpiSwsBasicV1Scores(self, numColumns):
        weakCols = self._getWeakestDefenseColumns(numColumns)
        sc = {}
        sc['method'] = 'mpi_sws_basic_v1'
        sc['columnsUsed'] = weakCols
        # compute averages for defense, confidenceImprovement,
        # claimProbability, and susceptibility
        sc['defense'] = 0
        sc['confidenceImprovement'] = 0
        sc['claimProbability'] = 0
        sc['susceptibility'] = 0
        totalClaimsMade = 0
        for col in weakCols:
            totalClaimsMade += self._ar['col'][col]['claimMade']
            sc['defense'] += self._ar['col'][col]['defense']
            sc['confidenceImprovement'] += (
                    self._ar['col'][col]['confidenceImprovement'])
            sc['claimProbability'] += self._ar['col'][col]['claimProbability']
            sc['susceptibility'] += self._ar['col'][col]['columnSusceptibility']
        if len(weakCols) > 0:
            sc['defense'] /= len(weakCols)
            sc['confidenceImprovement'] /= len(weakCols)
            sc['claimProbability'] /= len(weakCols)
            sc['susceptibility'] /= self._ar['tableStats']['numColumns']
        else:
            # No claims could even be made
            sc['susceptibility'] = 0
            sc['confidenceImprovement'] = 0
        # define knowledge needed as the number of knowledge cells requested
        # over the total number of cells for which cliams were made
        # likewise "work" can be defined as the number of attack cells
        # requested over the total number of claimed cells
        if totalClaimsMade:
            sc['knowledgeNeeded'] = ( 
                    self._ar['base']['knowledgeCells'] / totalClaimsMade)
            sc['workNeeded'] = ( 
                    self._ar['base']['attackCells'] / totalClaimsMade)
        else:
            sc['knowledgeNeeded'] = None
            sc['workNeeded'] = None
        # Compute an overall defense score from the other scores
        score = self._getSuscListScore(sc['susceptibility'])
        if score > sc['defense']:
            sc['defense'] = score
        self._appendScoreToScores(sc)
        return

    def _getWeakestDefenseColumns(self, numColumns):
        tuples = []
        cols = self._ar['col']
        # stuff the list with (columnName,defense) tuples
        for colName,data in cols.items():
            if data['claimTrials'] > 0:
                tuples.append([colName,data['defense']])
        weakest = sorted(tuples, key=lambda t: t[1])[:numColumns]
        cols = []
        for tup in weakest:
            cols.append(tup[0])
        return cols

    def _computeConfidence(self):
        cols = self._ar['col']
        for col in cols:
            if cols[col]['claimTrials'] > 0:
                if cols[col]['numConfidenceRatios']:
                    cols[col]['avgConfidenceRatios'] = (
                            cols[col]['sumConfidenceRatios'] / 
                            cols[col]['numConfidenceRatios'])
                if cols[col]['claimMade'] != 0:
                    cols[col]['confidence'] = (
                            cols[col]['claimCorrect'] /
                            cols[col]['claimMade'])
                cols[col]['confidenceImprovement'] = 0
                if cols[col]['avgConfidenceRatios'] < 1.0:
                    cols[col]['confidenceImprovement'] = (
                            (cols[col]['confidence'] - 
                                cols[col]['avgConfidenceRatios']) / 
                            (1 - cols[col]['avgConfidenceRatios']))
        return

    def _assignDefaultSusceptability(self):
        cols = self._ar['col']
        for col in cols:
            if (cols[col]['claimTrials'] > 0 and
                    'columnSusceptibility' not in cols[col]):
                cols[col]['columnSusceptibility'] = 1.0
        return

    def _computeDefense(self):
        cols = self._ar['col']
        for col in cols:
            if cols[col]['claimTrials'] > 0:
                cols[col]['claimProbability'] = (cols[col]['claimMade'] /
                        cols[col]['claimTrials'])
                cols[col]['defense'] = getInterpolatedValue(
                        cols[col]['confidenceImprovement'], 
                        cols[col]['claimProbability'],
                        self._defenseGrid1)
        return

    def _getSuscListScore(self,susc):
        i = 0
        lastSusc = self._suscList1[i][0]
        lastScore = self._suscList1[i][1]
        i += 1
        while i < len(self._suscList1):
            nextSusc = self._suscList1[i][0]
            nextScore = self._suscList1[i][1]
            if susc <= lastSusc and susc >= nextSusc:
                break
            lastSusc = nextSusc
            lastScore = nextScore
            i += 1
        print(f"susc {susc}, nextSusc {nextSusc}, lastSusc {lastSusc}")
        frac = (susc - nextSusc) / (lastSusc - nextSusc)
        score = (frac * (lastScore - nextScore)) + nextScore
        return (1 - score)

class gdaAttack:
    """Manages a GDA Attack

       See __init__ for input parameters. <br/>
       WARNING: this code is fragile, and can fail ungracefully, or
       just hang."""

    # ------------- Class called parameters and configured parameters
    _vb = False
    _cr = ''       # short for criteria
    _pp = None     # pretty printer (for debugging)
    _colNamesTypes = []
    _p = dict(name='',
               rawDb = '',
               anonDb = '',
               pubDb = '',
               criteria = 'singlingOut',
               table = '',
               uid = 'uid',
               flushCache=False,
               verbose=False,
               # following not normally set by caller, but can be
               locCacheDir = "cacheDBs",
               numRawDbThreads = 3,
               numAnonDbThreads = 3,
               numPubDbThreads = 3,
              )
    _requiredParams = ['name','rawDb']

    # ---------- Private internal state
    # Threads
    _rawThreads = []
    _anonThreads = []
    _pubThreads = []
    # Queues read by database threads _rawThreads and _anonThreads
    _rawQ = None
    _anonQ = None
    _pubQ = None
    # Queues read by various caller functions
    _exploreQ = None
    _knowledgeQ = None
    _attackQ = None
    _claimQ = None
    _guessQ = None
    # ask/get counters for setting 'stillToCome'
    _exploreCounter = 0
    _knowledgeCounter = 0
    _attackCounter = 0
    _claimCounter = 0
    _guessCounter = 0
    # State for computing attack results (see _initAtkRes())
    _atrs = {}
    # State for various operational measures (see _initOp())
    _op = {}

    def __init__(self,params):
        """ Sets everything up with 'gdaAttack(params)'

            params is a dictionary containing the following
            required parameters: <br/>
            `param['name']`: The name of the attack. Make it unique, because
            the cache is discovered using this name. <br/>
            `param['rawDb']`: The label for the DB to be used as the
            raw (non-anonymized) DB. <br/>
            Following are the optional parameters: <br/>
            `param['criteria']`: The criteria by which the attack should
            determined to succeed or fail. Must be one of 'singlingOut',
            'inference', or 'linkability'. Default is 'singlingOut'. <br/>
            `param['anonDb']`: The label for the DB to be used as the
            anonymized DB. (Is automatically set to `param['rawDb']` if
            not set.) <br/>
            `param['pubDb']`: The label for the DB to be used as the
            publicly known DB in linkability attacks. <br/>
            `param['table']`: The table to be attacked. Must be present
            if the DB has more than one table. <br/>
            `param['uid']`: The uid column for the table. Must be present
            if the name of the column is other than 'uid'. <br/>
            `param['flushCache']`: Set to true if you want the cache of
            query answers from a previous run flushed. The purpose of the
            cache is to save the work from an aborted attack, which can be
            substantial because attacks can have hundreds of queries. <br/>
            `param['locCacheDir']`: The directory holding the cache DBs.
            Default 'cacheDBs'. <br/>
            `param['numRawDbThreads']`: The number of parallel queries
            that can be made to the raw DB. Default 3. <br/>
            `param['numAnonDbThreads']`: The number of parallel queries
            that can be made to the anon DB. Default 3. <br/>
            `param['numPubDbThreads']`: The number of parallel queries
            that can be made to the public linkability DB. Default 3. <br/>
            `param['verbose']`: Set to True for verbose output.
        """

        if self._vb: print(f"Calling {__name__}.init")
        if self._vb: print(f"   {params}")
        self._initOp()
        self._initCounters()
        self._assignGlobalParams(params)
        self._doParamChecks()
        for param in self._requiredParams:
            if len(self._p[param]) == 0:
                s = str(f"Error: Need param '{param}' in class parameters")
                sys.exit(s)
        # create the database directory if it doesn't exist
        try:
            if not os.path.exists(self._p['locCacheDir']):
                os.makedirs(self._p['locCacheDir'])
        except OSError:
            sys.exit("Error: Creating directory. " +  self._p['locCacheDir'])

        # Get the table name if not provided by the caller
        if len(self._p['table']) == 0:
            tables = self.getTableNames()
            if len(tables) != 1:
                print("Error: gdaAttack(): Must include table name if " +
                        "there is more than one table in database")
                sys.exit()
            self._p['table'] = tables[0]

        # Get the column names for computing susceptibility later
        self._colNamesTypes = self.getColNamesAndTypes()
        if self._vb: print(f"Columns are '{self._colNamesTypes}'")
        self._initAtkRes()

        # Setup the database which holds already executed queries so we
        # don't have to repeat them if we are restarting
        self._setupLocalCacheDB()
        # Setup the threads and queues
        self._setupThreadsAndQueues()
        numThreads = threading.active_count()
        expectedThreads = (self._p['numRawDbThreads'] +
                self._p['numAnonDbThreads'] + 1)
        if len(self._p['pubDb']) > 0:
            expectedThreads += self._p['numPubDbThreads']
        if numThreads < expectedThreads:
            print(f"Error: Some thread(s) died "
                   f"(count {numThreads}, expected {expectedThreads}). "
                   f"Aborting.")
            self.cleanUp(cleanUpCache=False,doExit=True)

    def getResults(self):
        """ Returns all of the compiled attack results.

            This can be input to class `gdaScores()` and method
            `gdaScores.addResult()`."""
        # Add the operational parameters
        self._atrs['operational'] = self.getOpParameters()
        return self._atrs

    def getOpParameters(self):
        """ Returns a variety of performance measurements.

            Useful for debugging."""
        self._op['avQueryDuration'] = 0
        if self._op['numQueries'] > 0:
            self._op['avQueryDuration'] = (
                    self._op['timeQueries'] / self._op['numQueries'])
        self._op['avCachePutDuration'] = 0
        if self._op['numCachePuts'] > 0:
            self._op['avCachePutDuration'] = (
                    self._op['timeCachePuts'] / self._op['numCachePuts'])
        self._op['avCacheGetDuration'] = 0
        if self._op['numCacheGets'] > 0:
            self._op['avCacheGetDuration'] = (
                    self._op['timeCacheGets'] / self._op['numCacheGets'])
        return self._op

    def setVerbose(self):
        """Sets Verbose to True"""
        self._vb = True

    def unsetVerbose(self):
        """Sets Verbose to False"""
        self._vb = False

    def cleanUp(self, cleanUpCache=True, doExit=False,
                exitMsg="Finished cleanUp, exiting"):
        """ Garbage collect queues, threads, and cache.
        
            By default, this wipes the cache. The idea being that if the
            entire attack finished successfully, then it won't be
            repeated and the cache isn't needed. Do `cleanUpCache=False`
            if that isn't what you want."""
        if self._vb: print(f"Calling {__name__}.cleanUp")
        if self._rawQ.empty() != True:
            print("Warning, trying to clean up when raw queue not empty!")
        if self._anonQ.empty() != True:
            print("Warning, trying to clean up when anon queue not empty!")
        # Stuff in end signals for the workers (this is a bit bogus, cause
        # if a thread is gone or hanging, not all signals will get read)
        for i in range(self._p['numRawDbThreads']):
            self._rawQ.put(None)
        for i in range(self._p['numAnonDbThreads']):
            self._anonQ.put(None)
        for t in self._rawThreads + self._anonThreads:
            if t.isAlive(): t.join()
        if len(self._p['pubDb']) > 0:
            if self._pubQ.empty() != True:
                print("Warning, trying to clean up when pub queue not empty!")
            for i in range(self._p['numPubDbThreads']):
                self._pubQ.put(None)
            for t in self._pubThreads:
                if t.isAlive(): t.join()
        if cleanUpCache:
            self._removeLocalCacheDB()
        if doExit:
            sys.exit(exitMsg)

    def askClaim(self,spec,cache=True,claim=True):
        """Generate Claim query for raw and optionally pub databases.

        Making a claim results in a query to the raw database, and if
        linkability attack, the pub database, to check
        the correctness of the claim. Multiple calls to this method will
        cause the corresponding queries to be queued up, so `askClaim()`
        returns immediately. `getClaim()` harvests one claim result. <br/>
        Set `claim=False` if this claim should not be applied to the
        confidence improvement score. In this case, the probability score
        will instead be reduced accordingly. <br/>
        The `spec` is formatted as follows: <br/>

            {'known':[{'col':'colName','val':'value'},...],
              'guess':[{'col':'colName','val':'value'},...],
            }

        `spec['known']` are the columns and values the attacker already knows
        (i.e. with prior knowledge). Optional. <br/>
        `spec['guess']` are the columns and values the attacker doesn't know,
        but rather is trying to predict. Mandatory for 'singling out'
        and 'inference'. Optional for 'linkabiblity' <br/>
        Answers are cached <br/>
        Returns immediately"""
        if self._vb: print(f"Calling {__name__}.askClaim with spec '{spec}', count {self._claimCounter}")
        self._claimCounter += 1
        sql = self._makeSqlFromSpec(spec)
        if self._vb: print(f"Sql is '{sql}'")
        sqlConfs = self._makeSqlConfFromSpec(spec)
        if self._vb: print(f"SqlConf is '{sqlConfs}'")
        # Make a copy of the query for passing around
        job = {}
        job['q'] = self._claimQ
        job['claim'] = claim
        job['queries'] = [{'sql':sql,'cache':cache}]
        job['spec'] = spec
        for sqlConf in sqlConfs:
            job['queries'].append({'sql':sqlConf,'cache':cache})
        self._rawQ.put(job)

    def getClaim(self):
        """ Wait for and gather results of askClaim() calls
        
            Returns a data structure that contains both the result
            of one finished claim, and the claim's input parameters.
            Note that the order in which results are returned by
            `getClaim()` are not necessarily the same order they were
            inserted by `askClaim()`. <br/>
            Assuming `result` is returned: <br/>
            `result['claim']` is the value supplied in the corresponding
            `askClaim()` call <br/>
            `result['spec']` is a copy of the `spec` supplied in the
            corresponding `askClaim()` call. <br/>
            `result['queries']` is a list of the queries generated in order to
            validate the claim. <br/>
            `result['answers']` are the answers to the queries in
            `result['queries']`. <br/>
            `result['claimResult']` is 'Correct' or 'Incorrect', depending
            on whether the claim satisfies the critieria or not. <br/>
            `result['stillToCome']` is a counter showing how many more
            claims are still queued. When `stillToCome` is 0, then all
            claims submitted by `askClaim()` have been returned."""

        if self._vb: print(f"Calling {__name__}.getClaim")
        if self._claimCounter == 0:
            # Caller shouldn't be calling if there are no expected
            # answers, but is anyway, so just return
            return {'query':{'sql':'None'},'error':'Nothing to do',
                    'stillToCome':0,'claimResult':'Error'}
        job = self._claimQ.get()
        claim = job['claim']
        self._claimQ.task_done()
        self._claimCounter -= 1
        job['stillToCome'] = self._claimCounter
        self._addToAtkRes('claimTrials', job['spec'], 1)
        # The claim is tested against the first reply
        reply = job['replies'][0]
        job['claimResult'] = 'Wrong'
        if claim:
            self._addToAtkRes('claimMade', job['spec'], 1)
        if 'error' in reply:
            self._addToAtkRes('claimError', job['spec'], 1)
            job['claimResult'] = 'Error'
        else:
            if self._cr == 'singlingOut':
                claimIsCorrect = self._checkSinglingOut(reply['answer'])
            elif self._cr == 'inference':
                claimIsCorrect = self._checkInference(reply['answer'])
            elif self._cr == 'linkability':
                claimIsCorrect = self._checkLinkability(reply['answer'])
            if claim == 1 and claimIsCorrect:
                self._addToAtkRes('claimCorrect', job['spec'], 1)
                job['claimResult'] = 'Correct'
            elif claim == 0 and claimIsCorrect:
                self._addToAtkRes('claimPassCorrect', job['spec'], 1)
                job['claimResult'] = 'Correct'
        if self._cr == 'singlingOut' or self._cr == 'inference':
            # Then measure confidence against the second and third replies
            if 'answer' in job['replies'][1]:
                if job['replies'][1]['answer']:
                    guessedRows = job['replies'][1]['answer'][0][0]
                else:
                    guessedRows = 0
            elif 'error' in job['replies'][1]:
                self._pp.pprint(job)
                print(f"Error: conf query:\n{job['replies'][1]['error']}")
                self.cleanUp(cleanUpCache=False,doExit=True)
            if 'answer' in job['replies'][2]:
                if job['replies'][2]['answer']:
                    totalRows = job['replies'][2]['answer'][0][0]
                else:
                    totalRows = 0
            elif 'error' in job['replies'][2]:
                self._pp.pprint(job)
                print(f"Error: conf query:\n{job['replies'][2]['error']}")
                self.cleanUp(cleanUpCache=False,doExit=True)
            if totalRows:
                self._addToAtkRes('sumConfidenceRatios', job['spec'],
                        guessedRows/totalRows)
                self._addToAtkRes('numConfidenceRatios', job['spec'], 1)
                self._atrs['tableStats']['totalRows'] = totalRows
        else:
            # For linkability, the confidence is always 1/2
            self._addToAtkRes('sumConfidenceRatios', job['spec'], 0.5)
            self._addToAtkRes('numConfidenceRatios', job['spec'], 1)
        if 'q' in job:
            del job['q']
        return(job)

    def askAttack(self,query,cache=True):
        """ Generate and queue up an attack query for database.

            `query` is a dictionary with (currently) one value: <br/>
            `query['sql'] contains the SQL query."""
        self._attackCounter += 1
        if self._vb: print(f"Calling {__name__}.askAttack with query '{query}', count {self._attackCounter}")
        # Make a copy of the query for passing around
        qCopy = copy.copy(query)
        job = {}
        job['q'] = self._attackQ
        qCopy['cache'] = cache
        job['queries'] = [qCopy]
        self._anonQ.put(job)

    def getAttack(self):
        """ Returns the result of one askAttack() call
        
            Blocks until the result is available. Note that the order
            in which results are received is not necesarily the order
            in which `askAttack()` calls were made. <br/>
            Assuming `result` is returned: <br/>
            `result['answer']` is the answer returned by the DB. The
            format is: <br/>
                `[(C1,C2...,Cn),(C1,C2...,Cn), ... (C1,C2...,Cn)]` <br/>
            where C1 is the first element of the `SELECT`, C2 the second
            element, etc. <br/>
            `result['cells']` is the number of cells returned in the answer
            (used by `gdaAttack()` to compute total attack cells) <br/>
            `result['query']['sql']` is the query from the corresponding
            `askAttack()`."""

        if self._vb: print(f"Calling {__name__}.getAttack")
        if self._attackCounter == 0:
            # Caller shouldn't be calling if there are no expected
            # answers, but is anyway, so just return
            return {'query':{'sql':'None'},'error':'Nothing to do',
                    'stillToCome':0}
        job = self._attackQ.get()
        self._attackQ.task_done()
        self._attackCounter -= 1
        reply = job['replies'][0]
        reply['stillToCome'] = self._attackCounter
        self._atrs['base']['attackGets'] += 1
        if 'cells' in reply:
            if reply['cells'] == 0:
                self._atrs['base']['attackCells'] += 1
            else:
                self._atrs['base']['attackCells'] += reply['cells']
        else:
            self._atrs['base']['attackCells'] += 1
        return(reply)

    def askKnowledge(self,query,cache=True):
        """ Generate and queue up a prior knowledge query for database

            The class keeps track of how many prior knowledge cells were
            returned and uses this to compute a score. <br/>
            Input parameters formatted the same as with `askAttack()`"""

        self._knowledgeCounter += 1
        if self._vb: print(f"Calling {__name__}.askKnowledge with query "
                f"'{query}', count {self._knowledgeCounter}")
        # Make a copy of the query for passing around
        qCopy = copy.copy(query)
        job = {}
        job['q'] = self._knowledgeQ
        qCopy['cache'] = cache
        job['queries'] = [qCopy]
        self._rawQ.put(job)

    def getKnowledge(self):
        """ Wait for and gather results of prior askKnowledge() calls
        
            Blocks until the result is available. Note that the order
            in which results are received is not necesarily the order
            in which `askKnowledge()` calls were made. <br/>
            Return parameter formatted the same as with `getAttack()`"""

        if self._vb: print(f"Calling {__name__}.getKnowledge")
        if self._knowledgeCounter == 0:
            # Caller shouldn't be calling if there are no expected
            # answers, but is anyway, so just return
            return {'query':{'sql':'None'},'error':'Nothing to do',
                    'stillToCome':0}
        job = self._knowledgeQ.get()
        self._knowledgeQ.task_done()
        self._knowledgeCounter -= 1
        reply = job['replies'][0]
        reply['stillToCome'] = self._knowledgeCounter
        self._atrs['base']['knowledgeGets'] += 1
        if 'cells' in reply:
            self._atrs['base']['knowledgeCells'] += reply['cells']
        return(reply)

    def askExplore(self,query,cache=True):
        """ Generate and queue up an exploritory query for database

            No score book-keeping is done here. An analyst may make
            any number of queries without impacting the GDA score. <br/>
            `query` is a dictionary with two values: <br/>
            `query['sql']` contains the SQL query. <br/>
            `query['db']` determines which database is queried, and
            is one of 'rawDb', 'anonDb', or (if linkability), 'pubDb'."""

        self._exploreCounter += 1
        if self._vb: print(f"Calling {__name__}.askExplore with "
                f"query '{query}', count {self._exploreCounter}")
        # Make a copy of the query for passing around
        qCopy = copy.copy(query)
        job = {}
        job['q'] = self._exploreQ
        qCopy['cache'] = cache
        job['queries'] = [qCopy]
        if qCopy['db'] == 'rawDb' or qCopy['db'] == 'raw':
            self._rawQ.put(job)
        elif qCopy['db'] == 'anonDb' or qCopy['db'] == 'anon':
            self._anonQ.put(job)
        else:
            self._pubQ.put(job)

    def getExplore(self):
        """ Wait for and gather results of prior askExplore() calls.
        
            Blocks until the result is available. Note that the order
            in which results are received is not necesarily the order
            in which `askExplore()` calls were made. <br/>
            Return parameter formatted the same as with `getAttack()`"""
        if self._vb: print(f"Calling {__name__}.getExplore")
        if self._exploreCounter == 0:
            # Caller shouldn't be calling if there are no expected
            # answers, but is anyway, so just return
            return {'query':{'sql':'None'},'error':'Nothing to do',
                    'stillToCome':0}
        job = self._exploreQ.get()
        self._exploreQ.task_done()
        self._exploreCounter -= 1
        reply = job['replies'][0]
        reply['stillToCome'] = self._exploreCounter
        return(reply)

    def getPublicColValues(self,colName,tableName=''):
        """Return list of "publicly known" column values and counts

        Column value has index 0, count of distinct UIDs has index 1
        Must specify column name.
        """
        if len(colName) == 0:
            print(f"Must specify column 'colName'")
            return None

        if len(tableName) == 0:
            # caller didn't supply a table name, so get it from the
            # class init
            tableName = self._p['table']

        # Establish connection to database
        db = getDatabaseInfo(self._p['rawDb'])
        connStr = str(f"host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']} password={db['password']}")
        conn = psycopg2.connect(connStr)
        cur = conn.cursor()
        # First we need to know the total number of distinct users
        sql = str(f"""select count(distinct {self._p['uid']})
                      from {tableName}""")
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Error: getPublicColValues() query: '{e}'")
            self.cleanUp(cleanUpCache=False,doExit=True)
        ans = cur.fetchall()
        numUid = ans[0][0]
        # Query the raw db for values in the column
        sql = str(f"""select {colName}, count(distinct {self._p['uid']})
                      from {tableName}
                      group by 1
                      order by 2 desc
                      limit 200""")
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Error: getPublicColValues() query: '{e}'")
            self.cleanUp(cleanUpCache=False,doExit=True)
        ans = cur.fetchall()
        ret = []
        for row in ans:
            # row[0] is the value, row[1] is the count
            if (((row[1]/numUid) > 0.002) and
                    (row[1] >= 50)):
                ret.append((row[0],row[1]))
        conn.close()
        return ret

    def getColNames(self,dbType='rawDb',tableName=''):
        """Return simple list of column names

        `dbType` is one of 'rawDb' or 'anonDb'"""

        if len(tableName) == 0:
            colsAndTypes = self.getColNamesAndTypes(dbType=dbType)
        else:
            colsAndTypes = self.getColNamesAndTypes(
                    dbType=dbType,tableName=tableName)
        if not colsAndTypes:
            return None
        cols = []
        for tup in colsAndTypes:
            cols.append(tup[0])
        return cols

    def getAttackTableName(self):
        """Returns the name of the table being used in the attack."""
        return self._p['table']

    def getTableCharacteristics(self,tableName=''):
        """Returns the full contents of the table characteristics
        
           Return value is a dict indexed by column name: <br/>

               { '<colName>':
                   {
                       'av_rows_per_vals': 3.93149,
                       'av_uids_per_val': 0.468698,
                       'column_label': 'continuous',
                       'column_name': 'dropoff_latitude',
                       'column_type': 'real',
                       'max': '898.29382000000000',
                       'min': '-0.56333297000000',
                       'num_distinct_vals': 24216,
                       'num_rows': 95205,
                       'num_uids': 11350,
                       'std_rows_per_val': 10.8547,
                       'std_uids_per_val': 4.09688},
                   }
               }

        """
        if len(tableName) == 0:
            # caller didn't supply a table name, so get it from the
            # class init
            tableName = self._p['table']

        # Modify table name to the default for the characteristics table
        tableName += '_char'

        # Establish connection to database
        db = getDatabaseInfo(self._p['rawDb'])
        connStr = str(f"host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']} password={db['password']}")
        conn = psycopg2.connect(connStr)
        cur = conn.cursor()
        # Set up return dict
        ret = {}
        # Query it for column names
        sql = str(f"""select column_name, data_type 
                  from information_schema.columns where
                  table_name='{tableName}'""")
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Error: getTableCharacteristics() query: '{e}'")
            self.cleanUp(cleanUpCache=False,doExit=True)
        cols = cur.fetchall()
        # Make index for column name (should be 0, but just to be sure)
        for colNameIndex in range(len(cols)):
            if cols[colNameIndex][0] == 'column_name':
                break

        # Query it for table contents
        sql = str(f"SELECT * FROM {tableName}")
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Error: getTableCharacteristics() query: '{e}'")
            self.cleanUp(cleanUpCache=False,doExit=True)
        ans = cur.fetchall()
        for row in ans:
            colName = row[colNameIndex]
            ret[colName] = {}
            for i in range(len(row)):
                ret[colName][cols[i][0]] = row[i]
        conn.close()
        return ret

    # Note that following is used internally, but we expose it to the
    # caller as well because it is a useful function for exploration
    def getColNamesAndTypes(self,dbType='rawDb',tableName=''):
        """Return raw database column names and types (or None if error)

        dbType is one of 'rawDb' or 'anonDb' <br/>
        return format: [(col,type),(col,type),...]"""
        if len(tableName) == 0:
            # caller didn't supply a table name, so get it from the
            # class init
            tableName = self._p['table']

        # Establish connection to database
        db = getDatabaseInfo(self._p[dbType])
        if db['type'] != 'postgres' and db['type'] != 'aircloak':
            print(f"DB type '{db['type']}' must be 'postgres' or 'aircloak'")
            return None
        connStr = str(f"host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']} password={db['password']}")
        conn = psycopg2.connect(connStr)
        cur = conn.cursor()
        # Query it for column names
        if db['type'] == 'postgres':
            sql = str(f"""select column_name, data_type 
                      from information_schema.columns where
                      table_name='{tableName}'""")
        elif db['type'] == 'aircloak':
            sql = str(f"show columns from {tableName}")
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Error: getColNamesAndTypes() query: '{e}'")
            self.cleanUp(cleanUpCache=False,doExit=True)
        ans = cur.fetchall()
        ret = []
        for row in ans:
            ret.append((row[0],row[1]))
        conn.close()
        return ret

    def getTableNames(self,dbType='rawDb'):
        """Return database table names
        
        dbType is one of 'rawDb' or 'anonDb' <br/>
        Table names returned as list, unless error then return None"""

        # Establish connection to database
        db = getDatabaseInfo(self._p[dbType])
        if db['type'] != 'postgres' and db['type'] != 'aircloak':
            print(f"DB type '{db['type']}' must be 'postgres' or 'aircloak'")
            return None
        connStr = str(f"host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']} password={db['password']}")
        conn = psycopg2.connect(connStr)
        cur = conn.cursor()
        # Query it for column names
        if db['type'] == 'postgres':
            sql = """SELECT tablename
                     FROM pg_catalog.pg_tables
                     WHERE schemaname != 'pg_catalog' AND
                           schemaname != 'information_schema'"""
        elif db['type'] == 'aircloak':
            sql = "show tables"
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(f"Error: getTableNames() query: '{e}'")
            self.cleanUp(cleanUpCache=False,doExit=True)
        ans = cur.fetchall()
        ret = []
        for row in ans:
            ret.append(row[0])
        conn.close()
        return ret

    def getUidColName(self):
        """ Returns the name of the UID column"""
        return self._p['uid']

    # -------------- Private Methods -------------------
    def _assignGlobalParams(self,params):
        self._pp = pprint.PrettyPrinter(indent=4)
        for key, val in params.items():
            self._p[key] = val
            # assign verbose value to a smaller variable name
            if key == "verbose":
                if val != False:
                    self._vb = True
            # Check criteria
            if key == "criteria":
                if (val == 'singlingOut' or val == 'inference' or
                        val == 'linkability'):
                    self._cr = val
                else:
                    print("""Error: criteria must be one of 'singlingOut',
                             'inference', or 'linkability'""")
                    sys.exit('')

    def _setupLocalCacheDB(self):
        path = self._p['locCacheDir'] + "/" + self._p['name'] + ".db"
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        if self._p['flushCache'] == True:
            sql = "DROP TABLE IF EXISTS tab"
            if self._vb: print(f"   cache DB: {sql}")
            cur.execute(sql)
        sql = """CREATE TABLE IF NOT EXISTS tab
                 (qid text, answer text)"""
        if self._vb: print(f"   cache DB: {sql}")
        cur.execute(sql)
        conn.close()

    def _removeLocalCacheDB(self):
        path = self._p['locCacheDir'] + "/" + self._p['name'] + ".db"
        if os.path.exists(path):
            try:
                os.remove(path)
            except:
                print(f"ERROR: Failed to remove cache DB {path}")

    def _setupThreadsAndQueues(self):
        self._anonThreads = []
        self._rawThreads = []
        self._pubThreads = []
        self._exploreQ = queue.Queue()
        self._knowledgeQ = queue.Queue()
        self._attackQ = queue.Queue()
        self._claimQ = queue.Queue()
        self._guessQ = queue.Queue()
        self._rawQ = queue.Queue()
        if self._cr == 'linkability':
            self._pubQ = queue.Queue()
        self._anonQ = queue.Queue()
        backQ = queue.Queue()
        for i in range(self._p['numRawDbThreads']):
            d = dict(db=self._p['rawDb'],q=self._rawQ,
                     kind='raw',backQ=backQ)
            t = threading.Thread(target=self._dbWorker,kwargs=d)
            t.start()
            self._rawThreads.append(t)
        for i in range(self._p['numAnonDbThreads']):
            d = dict(db=self._p['anonDb'],q=self._anonQ,
                     kind='anon',backQ=backQ)
            t = threading.Thread(target=self._dbWorker,kwargs=d)
            t.start()
            self._anonThreads.append(t)
        if self._cr == 'linkability':
            for i in range(self._p['numPubDbThreads']):
                d = dict(db=self._p['pubDb'],q=self._pubQ,
                         kind='pub',backQ=backQ)
                t = threading.Thread(target=self._dbWorker,kwargs=d)
                t.start()
                self._pubThreads.append(t)
        num = (self._p['numRawDbThreads'] + self._p['numAnonDbThreads'])
        if self._cr == 'linkability':
            num += self._p['numPubDbThreads']
        # Make sure all the worker threads are ready
        for i in range(num):
            msg = backQ.get()
            if self._vb: print(f"{msg} is ready")
            backQ.task_done()


    def _dbWorker(self,db,q,kind,backQ):
        if self._vb: print(f"Starting {__name__}.dbWorker:{db,kind}")
        me = threading.current_thread()
        d = getDatabaseInfo(db)
        # Establish connection to database
        connStr = str(f"host={d['host']} port={d['port']} dbname={d['dbname']} user={d['user']} password={d['password']}")
        if self._vb: print(f"    {me}: Connect to DB with DSN '{connStr}'")
        conn = psycopg2.connect(connStr)
        cur = conn.cursor()
        # Establish connection to local cache
        path = self._p['locCacheDir'] + "/" + self._p['name'] + ".db"
        # Set timeout low so that we don't spend a lot of time inserting
        # into the cache in case it gets overloaded
        connInsert = sqlite3.connect(path, timeout=0.1)
        curInsert = connInsert.cursor()
        connRead = sqlite3.connect(path)
        curRead = connRead.cursor()
        backQ.put(me)
        while True:
            jobOrig = q.get()
            q.task_done()
            if jobOrig is None:
                if self._vb: print(f"    {me}: dbWorker done {db,kind}")
                conn.close()
                connRead.close()
                connInsert.close()
                break
            # make a copy for passing around
            job = copy.copy(jobOrig)
            replyQ = job['q']
            replies = []
            for query in job['queries']:
                reply = self._processQuery(query,conn,cur,
                        connInsert,curInsert,curRead)
                replies.append(reply)
            job['replies'] = replies
            replyQ.put(job)

    def _processQuery(self,query,conn,cur,connInsert,curInsert,curRead):
        # record and remove the return queue
        cache = query['cache']
        del query['cache']
        # Check the cache for the answer
        # Note that at this point query is a dict
        # containing the sql, the db (raw, anon, or pub),
        # and any tags that the source added
        cachedReply = None
        if cache:
           cachedReply = self._getCache(curRead,query)
        if cachedReply:
            if self._vb: print("    Answer from cache")
            if 'answer' in cachedReply:
                numCells = self._computeNumCells(cachedReply['answer'])
                cachedReply['cells'] = numCells
            return cachedReply
        else:
            start = time.perf_counter()
            try:
                cur.execute(query['sql'])
            except psycopg2.Error as e:
                reply = dict(error=e.pgerror)
            else:
                ans = cur.fetchall()
                numCells = self._computeNumCells(ans)
                reply = dict(answer=ans,cells=numCells)
            end = time.perf_counter()
            duration = end - start
            self._op['numQueries'] += 1
            self._op['timeQueries'] += duration
            reply['query'] = query
            # only cache if the native query is slow
            if duration > 0.1:
                self._putCache(connInsert,curInsert,query,reply)
            return reply

    def _checkInference(self,ans):
        # column 0 must be UID
        # User is inferred if all users in answer have same attributes
        # Returns 1 if inference claim correct, else returns 0
        if len(ans) == 0:
            return 0
        numRows = len(ans)
        numColumns = len(ans[0])
        if numColumns < 2:
            # Can't test inference unless there is at least one column
            # (other than UID) that is the same
            return 0
        for c in range(1,numColumns):
            val = ans[0][c]
            for r in range(1,numRows):
                if val != ans[r][c]:
                    return 0
        return 1

    def _checkLinkability(self,ans):
        # The test is the same as with singling out
        # Returns 1 if linkability claim correct, else returns 0
        return self._checkSinglingOut(ans)

    def _checkSinglingOut(self,ans):
        # column 0 must be UID
        # User is singled-out if there is only one distinct UID
        # Returns 1 if singling out claim correct, else returns 0
        if len(ans) == 0:
            return 0
        uids = {}
        for row in ans:
            uids[row[0]] = 1
        numUids = len(uids)
        if numUids == 1:
            return 1
        else:
            return 0

    def _computeNumCells(self,ans):
        # ans is a list of tuples [(x,y),(x,y),(x,y) ...
        # Count the number of columns (in the first row)
        if len(ans) == 0:
            return 0
        numColumns = len(ans[0])
        numRows = len(ans)
        numCells = numColumns * numRows
        return numCells

    def _doParamChecks(self):
        dbInfoRaw = getDatabaseInfo(self._p['rawDb'])
        if not dbInfoRaw:
            sys.exit('rawDb now found in database config')
        if len(self._p['anonDb']) == 0:
            self._p['anonDb'] = self._p['rawDb']
        else:
            dbInfoAnon = getDatabaseInfo(self._p['anonDb'])
            if not dbInfoAnon:
                sys.exit('anonDb not found in database config')
        if self._cr == 'linkability':
            dbInfo = getDatabaseInfo(self._p['pubDb'])
            if not dbInfo:
                sys.exit('Must specify pubDb if criteria is linkability')
        numThreads = self._p['numRawDbThreads'] + self._p['numAnonDbThreads']
        if self._cr == 'linkability':
            numThreads += self._p['numPubDbThreads']
        if numThreads > 50:
            sys.exit("Error: Can't have more than 50 threads total")

    def _getCache(self,cur,query):
        # turn the query (dict) into a string
        qStr = self._dict2Str(query)
        if qStr is None:
            return None
        sql = str(f"SELECT answer FROM tab where qid = '{qStr}'")
        if self._vb: print(f"   cache DB: {sql}")
        start = time.perf_counter()
        try:
            cur.execute(sql)
        except sqlite3.Error as e:
            print(f"getCache error '{e.args[0]}'")
            return None
        end = time.perf_counter()
        self._op['numCacheGets'] += 1
        self._op['timeCacheGets'] += (end - start)
        answer = cur.fetchone()
        if not answer:
            return None
        rtnDict = self._str2Dict(answer[0])
        return rtnDict

    def _putCache(self,conn,cur,query,reply):
        # turn the query and reply (dict) into a string
        qStr = self._dict2Str(query)
        if qStr is None:
            return
        rStr = self._dict2Str(reply)
        if rStr is None:
            return
        sql = str(f"INSERT INTO tab VALUES ('{qStr}','{rStr}')")
        if self._vb: print(f"   cache DB: {sql}")
        start = time.perf_counter()
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.Error as e:
            print(f"putCache error '{e.args[0]}'")
        end = time.perf_counter()
        self._op['numCachePuts'] += 1
        self._op['timeCachePuts'] += (end - start)

    def _dict2Str(self,d):
        try:
            dStr = simplejson.dumps(d)
        except TypeError:
            print("simpleJson failed")
            return None
        dByte = str.encode(dStr)
        dByte64 = base64.b64encode(dByte)
        try:
            dByte64Str = str(dByte64, "utf-8")
        except MemoryError:
            print("str(dByte64) failed")
            return None
        return dByte64Str

    def _str2Dict(self,dByte64Str):
        dByte64 = str.encode(dByte64Str)
        dByte = base64.b64decode(dByte64)
        dStr = str(dByte, "utf-8")
        d = simplejson.loads(dStr)
        return d

    def _makeSqlFromSpec(self,spec):
        sql = "select "
        if 'known' in spec:
            numKnown = len(spec['known'])
        else:
            numKnown = 0
        if 'guess' in spec:
            numGuess = len(spec['guess'])
        else:
            numGuess = 0
        if self._cr == 'inference':
            sql += str(f"{self._p['uid']}, ")
            for i in range(numGuess):
                sql += str(f"{spec['guess'][i]['col']}")
                if i == (numGuess - 1):
                    sql += " "
                else:
                    sql += ", "
            sql += str(f"from {self._p['table']} ")
            if numKnown:
                sql += "where "
            for i in range(numKnown):
                sql += str(f"{spec['known'][i]['col']} = ")
                sql += str(f"'{spec['known'][i]['val']}' ")
                if i == (numKnown - 1):
                    sql += " "
                else:
                    sql += "and "
        elif self._cr == 'singlingOut' or self._cr == 'linkability':
            sql += str(f"{self._p['uid']} from {self._p['table']} where ")
            for i in range(numKnown):
                sql += str(f"{spec['known'][i]['col']} = ")
                sql += str(f"'{spec['known'][i]['val']}' and ")
            for i in range(numGuess):
                sql += str(f"{spec['guess'][i]['col']} = ")
                sql += str(f"'{spec['guess'][i]['val']}' ")
                if i == (numGuess - 1):
                    sql += " "
                else:
                    sql += "and "
        return sql

    def _makeSqlConfFromSpec(self,spec):
        sqls = []
        numGuess = len(spec['guess'])
        if self._cr == 'inference' or self._cr == 'singlingOut':
            sql = str(f"select count(*) from {self._p['table']} where ")
            # This first sql learns the number of rows matching the
            # guessed values
            for i in range(numGuess):
                sql += str(f"{spec['guess'][i]['col']} = ")
                sql += str(f"'{spec['guess'][i]['val']}'")
                if i != (numGuess - 1):
                    sql += " and "
            sqls.append(sql)
            # This second sql learns the total number of rows (should
            # normally be a cached result)
            sql = str(f"select count(*) from {self._p['table']}")
            sqls.append(sql)
        elif self._cr == 'linkability':
            # nothing happens for linkability
            pass
        return sqls

    def _addToAtkRes(self, label, spec, val):
        """Adds the value to each column in the guess"""
        for tup in spec['guess']:
            col = tup['col']
            if col not in self._atrs['col']:
                print(f"Error: addToAtkRes(): Bad column in spec: '{col}'")
                self.cleanUp(cleanUpCache=False,doExit=True)
            if label not in self._atrs['col'][col]:
                print(f"Error: addToAtkRes(): Bad label '{label}'")
                self.cleanUp(cleanUpCache=False,doExit=True)
            self._atrs['col'][col][label] += val

    def _initAtkRes(self):
        self._atrs = {}
        self._atrs['attack'] = {}
        self._atrs['base'] = {}
        self._atrs['tableStats'] = {}
        self._atrs['col'] = {}
        # ----- Attack parameters
        self._atrs['attack']['attackName'] = self._p['name']
        self._atrs['attack']['rawDb'] = self._p['rawDb']
        self._atrs['attack']['anonDb'] = self._p['anonDb']
        if self._cr == 'linkability':
            self._atrs['attack']['pubDb'] = self._p['anonDb']
        self._atrs['attack']['criteria'] = self._p['criteria']
        self._atrs['attack']['table'] = self._p['table']
        # add parameters for the database machine itself
        db = getDatabaseInfo(self._p['rawDb'])
        self._atrs['attack']['rawHost'] = db['host']
        self._atrs['attack']['rawDbName'] = db['dbname']
        self._atrs['attack']['rawPort'] = db['port']
        if self._cr == 'linkability':
            db = getDatabaseInfo(self._p['pubDb'])
            self._atrs['attack']['pubHost'] = db['host']
            self._atrs['attack']['pubDbName'] = db['dbname']
            self._atrs['attack']['pubPort'] = db['port']
        db = getDatabaseInfo(self._p['anonDb'])
        self._atrs['attack']['anonHost'] = db['host']
        self._atrs['attack']['anonDbName'] = db['dbname']
        self._atrs['attack']['anonPort'] = db['port']
        # and a timestamp
        self._atrs['attack']['startTime'] = str(datetime.datetime.now())
        # ----- Params for computing knowledge:
        # number of prior knowledge cells requested
        self._atrs['base']['knowledgeCells'] = 0    
        # number of times knowledge was queried
        self._atrs['base']['knowledgeGets'] = 0     

        # ----- Params for computing how much work needed to attack:
        # number of attack cells requested
        self._atrs['base']['attackCells'] = 0       
        # number of times attack was queried
        self._atrs['base']['attackGets'] = 0        
        self._atrs['tableStats']['colNamesAndTypes'] = self._colNamesTypes
        self._atrs['tableStats']['numColumns'] = len(self._colNamesTypes)
        for tup in self._colNamesTypes:
            col = tup[0]
            if self._vb: print(f"initAtkRes() init column '{col}'")
            self._atrs['col'][col] = {}

            # ----- Params for computing claim success rate:
            # total possible number of claims
            self._atrs['col'][col]['claimTrials'] = 0       
            # actual number of claims
            self._atrs['col'][col]['claimMade'] = 0         
            # number of correct claims
            self._atrs['col'][col]['claimCorrect'] = 0      
            # number of claims that produced bad SQL answer
            self._atrs['col'][col]['claimError'] = 0        
            # claims where the attacker chose to pass (not make a claim), 
            # but where the claim would have been correct
            self._atrs['col'][col]['claimPassCorrect'] = 0    

            # ----- Params for computing confidence:
            # sum of all known count to full count ratios
            self._atrs['col'][col]['sumConfidenceRatios'] = 0    
            # number of such ratios
            self._atrs['col'][col]['numConfidenceRatios'] = 0    
            # average confidence ratio (division of above two params)
            self._atrs['col'][col]['avgConfidenceRatios'] = 0    

    def _initOp(self):
        self._op['numQueries'] = 0
        self._op['timeQueries'] = 0
        self._op['numCachePuts'] = 0
        self._op['timeCachePuts'] = 0
        self._op['numCacheGets'] = 0
        self._op['timeCacheGets'] = 0

    def _initCounters(self):
        self._exploreCounter = 0
        self._knowledgeCounter = 0
        self._attackCounter = 0
        self._claimCounter = 0
        self._guessCounter = 0
