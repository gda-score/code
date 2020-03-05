
try:
    from .gdaTools import getInterpolatedValue, getDatabaseInfo
except ImportError:
    from gdaTools import getInterpolatedValue, getDatabaseInfo


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
        for col, data in result['col'].items():
            for key, val in data:
                self._ar['col'][col][key] += val
        self._computeConfidence()
        self._assignDefaultSusceptability()
        return True

    def assignColumnSusceptibility(self, column, susValue):
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

    def getScores(self, method='mpi_sws_basic_v1', numColumns=-1):
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
        (1, 1, 0), (1, .01, .1), (1, .001, .3), (1, .0001, .7), (1, .00001, 1),
        (.95, 1, .1), (.95, .01, .3), (.95, .001, .7), (.95, .0001, .8), (.95, .00001, 1),
        (.90, 1, .3), (.90, .01, .6), (.90, .001, .8), (.90, .0001, .9), (.90, .00001, 1),
        (.75, 1, .7), (.75, .01, .9), (.75, .001, .95), (.75, .0001, 1), (.75, .00001, 1),
        (.50, 1, .95), (.50, .01, .95), (.50, .001, 1), (.50, .0001, 1), (.5, .00001, 1),
        (0, 1, 1), (0, .01, 1), (0, .001, 1), (0, .0001, 1), (0, .00001, 1)
    ]
    # This is organized by susceptibility score and multipliticative factor
    # on the overall score
    _suscList1 = [(1.0, 1.0), (0.01, 0.95), (0.001, 0.9), (0.0001, 0.6),
                  (0.00001, 0.3), (0.000001, 0.1), (0.0, 0.0)
                  ]

    def _appendScoreToScores(self, sc):
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
        for colName, data in cols.items():
            if data['claimTrials'] > 0:
                tuples.append([colName, data['defense']])
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

    def _getSuscListScore(self, susc):
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
        frac = (susc - nextSusc) / (lastSusc - nextSusc)
        score = (frac * (lastScore - nextScore)) + nextScore
        return (1 - score)
