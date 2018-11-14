import sys
import pprint
import random
import string
import copy
sys.path.append('../common')
from gdaScore import gdaAttack
from gdaUtilities import makeGroupBy

class findQueryConditions:
    """Build query conditions (WHERE) with between X and Y distinct users

    """
    # print or not, for debugging
    _p = 0
    _pp = None      # for pretty printing (debugging)
    _ret = []       # holds the discovered query conditions
    # The following used by getNextCondition()
    _nextConditionType = 0
    _nextConditionInstance = 0
    
    def _findSnap(self, val):
        # simple brute force approach
        start = 1
        # find a starting value less than the target value val
        while(1):
            if start > val:
                start /= 10
            else:
                break
        # then find the pair of snapped values above and below val
        while(1):
            # the loop always starts at a power of 10
            below = start
            above = below * 2
            if self._p: print(f"below {below} above {above}")
            if ((below <= val) and (above >= val)):
                break
            below = start * 2
            above = start * 5
            if self._p: print(f"below {below} above {above}")
            if ((below <= val) and (above >= val)):
                break
            below = start * 5
            above = start * 10
            if self._p: print(f"below {below} above {above}")
            if ((below <= val) and (above >= val)):
                break
            start *= 10
        final = below
        if ((above - val) < (val - below)):
            final = above
        return final
    
    # Can call this with either the amount to grow, in which case the 
    # number of target buckets is computed, or the number of target buckets
    # factor is basically the number of buckets that the other column has
    def _generalizeNumber(self, col, colInfo, grow=0, targetBuckets=0, factor=1):
        if grow != 0:
            targetBuckets = colInfo[col]['dVals']/(grow * factor)
        if targetBuckets < 1:
            # Can't really grow the buckets
            if self._p: print(f"generalizeNumber: abort with grow {grow}, "
                    f"targetBuckets {targetBuckets}")
            return None
        targetRange = (
                (colInfo[col]['maxVal'] - colInfo[col]['minVal'])/targetBuckets)
        # Lets find the nearest Diffix snapped range to the target range.
        snappedRange = self._findSnap(targetRange)
        if self._p: print(f"snappedRange for column {col} is: {snappedRange}")
        colInfo2 = copy.deepcopy(colInfo)
        # Change colInfo2 so that it encodes the range rather than the
        # original native columns
        colInfo2[col]['condition'] = snappedRange
        return colInfo2
    
    def _generalizeTextOrDatetime(self, col, colInfo, grow=0, targetBuckets=0, factor=1):
        if grow != 0:
            targetBuckets = colInfo[col]['dVals']/(grow * factor)
        if targetBuckets < 1:
            # Can't really grow the buckets
            if self._p: print(f"generalizeNumber: abort with grow {grow}, "
                    f"targetBuckets {targetBuckets}")
            return None
        # Find the closest number of distinct values
        index = -1
        nearestNum = 100000000
        for i in range(len(colInfo[col]['buckets'])):
            num = colInfo[col]['buckets'][i][1]
            if abs(num - targetBuckets) < nearestNum:
                index = i
                nearestNum = abs(num - targetBuckets)
        if self._p: print(f"closest condition for column {col} is: "
                f"{colInfo[col]['buckets'][index][0]} "
                f"({colInfo[col]['buckets'][index][1]} against {targetBuckets})")
        colInfo2 = copy.deepcopy(colInfo)
        # Change colInfo2 so that it encodes the range rather than the
        # original native columns
        colInfo2[col]['condition'] = colInfo[col]['buckets'][index][0]
        return colInfo2
    
    def _makeHistSql(self, table,columns,colInfo,uid,minCount,maxCount):
        if self._p: self._pp.pprint(colInfo)
        sql = "select ";
        for col in columns:
            if colInfo[col]['condition'] == 'none':
                sql += str(f"{col}, ")
            else:
                unit = colInfo[col]['condition']
                if colInfo[col]['colType'][:4] == 'date':
                    sql += str(f"extract({unit} from {col})::integer, ")
                elif colInfo[col]['colType'] == 'text':
                    sql += str(f"lower(left({col}, {unit})), ")
                else:    # int or real
                    sql += str(f"floor({col}/{unit})*{unit}, ")
        duidClause = str(f"count(distinct {uid}) ")
        groupby = makeGroupBy(columns)
        sql += duidClause + str(f"from {table} ") + groupby
        #sql += "having " + duidClause 
        #sql += str(f"between {minCount} and {maxCount}")
        return(sql)
    
    def _queryAndGather(self,x,sql,colInfo,columns,minCount,maxCount):
        query = dict(db="raw",sql=sql)
        x.askExplore(query)
        ans = x.getExplore()
        if not ans:
            msg = "Failed _queryAndGather: " + "sql"
            x.cleanUp(exitMsg=msg)
        if 'error' in ans:
            x.cleanUp(exitMsg=msg)
        if self._p: print(ans['answer'])
        numIn = 0
        numAbove = 0
        numBelow = 0
        filtered = []
        i = len(columns)
        for row in ans['answer']:
            if row[i] < minCount:
                numBelow += 1
            elif row[i] > maxCount:
                numAbove += 1
            else:
                numIn += 1
                filtered.append(row)
        # Start building return structure
        ret = {}
        if numAbove > numIn:
            ret['guess'] = 'bucketsTooBig'
        elif numBelow > numIn:
            ret['guess'] = 'bucketsTooSmall'
        else:
            ret['guess'] = 'bucketsJustRight'
        ret['info'] = []
        for col in columns:
            ret['info'].append(dict(
                col=col,
                condition=colInfo[col]['condition'],
                colType=colInfo[col]['colType']))
        ret['buckets'] = filtered
        return ret
    
    def _ansNotDup(self,answer):
        for ans in self._ret:
            gotMatch = 1
            for i in range(len(ans['info'])):
                if ans['info'][i]['col'] != answer['info'][i]['col']:
                    gotMatch = 0
                if ans['info'][i]['colType'] != answer['info'][i]['colType']:
                    gotMatch = 0
                if ans['info'][i]['condition'] != answer['info'][i]['condition']:
                    gotMatch = 0
            if gotMatch == 1:
                return 0
        return 1
    
    def _buildWhereClause(self,info,bucket):
        clause = "WHERE "
        if self._p: print(f"info {info}, bucket {bucket}")
        print(f"info {info}, bucket {bucket}")
        for i in range(len(info)):
            col = info[i]
            unit = bucket[i]
            if col['condition'] == 'none':
                clause += str(f"{col['col']} = {unit} AND ")
            elif col['colType'][:4] == 'date':
                clause += str(f"extract({col['condition']} FROM "
                        f"{col['col']}) = {unit} AND ")
            elif col['colType'] == 'text':
                clause += str(f"lower(left({col['col']}, {col['condition']})) "
                        f"= '{unit}' AND ")
            else:    # int or real
                clause += str(f"floor({col['col']}/{col['condition']})"
                        f"*{col['condition']} AND ")
        # Strip off the last AND
        clause = clause[:-4]
        return clause

    def getDataStructure(self):
        """ Returns the internal data structure """
        return self._ret

    def initWhereClauseLoop(self):
        """ Initializes the `getNextWhereClause()` loop

        Normally does not need to be called to use `getNextWhereClause()`
        Only necessary to call if `getNextWhereClause()` has already been
        called, and the caller wants to start over"""
        self._nextConditionType = 0
        self._nextConditionInstance = 0

    def getNextWhereClause(self):
        """ Returns the following structure, or None if no more
        """
        # Decide if the next condition exists
        nt = self._nextConditionType
        ni = self._nextConditionInstance
        if nt > (len(self._ret) - 1):
            return None
        con = self._ret[nt]
        if ni > (len(con['buckets']) - 1):
            return None
        bucket = con['buckets'][ni]
        # Condition exists, so build return structure and WHERE clause
        ret = {}
        ret['info'] = con['info']
        ret['bucket'] = bucket
        ret['whereClause'] = self._buildWhereClause(con['info'],bucket)
        ret['numUids'] = bucket[-1]
        # Increment the indices
        if ni == (len(con['buckets']) - 1):
            self._nextConditionInstance = 0
            self._nextConditionType += 1
        else:
            self._nextConditionInstance += 1
        return ret

    def __init__(self, params, columns, minCount, maxCount, table = ''):
        """ Find values and ranges that have between minCount and maxCount UIDs.
    
            `params` is the same structure used to call gdaAttack(params).
            Note that the `anonDb` in params must be a cloak
            `columns` is a list of one or two column names. One of them must
            be of numeric type (real or int).
            Builds an internal structure like this (which can be returned
            using `getDataStructure()`:
                [   {   'buckets': [   (1, 1918.0, 8),
                                       (77, 1950.0, 5)],
                        'info': [   {   'col': 'cli_district_id',
                                        'colType': 'integer',
                                        'condition': 'none'},
                                    {   'col': 'birthdate',
                                        'colType': 'date',
                                        'condition': 'year'}
                                ]
                     }
                ]
            This is a list of query condition (WHERE clause) settings, each
            of which has multiple individual value combinations. The routine
            getNextWhereClause() shows how this structure is used.
        """
        self.initWhereClauseLoop()
        self._ret = []    # this is the internal data struct

        if len(columns) > 2:
            return
    
        self._pp = pprint.PrettyPrinter(indent=4)
        if self._p: print(f"Call getValuesAnd Ranges with min {minCount}, max {maxCount}")
        # We're going to create a gdaAttack() object, so we need to make a
        # name for it that is unique with high probability. Otherwise two calls
        # to this subroutine will collide.
        randomName = (
                ''.join([random.choice(string.ascii_letters + string.digits)
                    for n in range(32)]))
        params['name'] = "getValuesAndRanges_" + randomName
        params['flushCache'] = True
        x = gdaAttack(params)
        if len(table) == 0:
            table = x.getAttackTableName()
        # Learn the columns' types.
        ans = x.getColNamesAndTypes(tableName=table,dbType="anonDb");
        colInfo = {}
        # The 'condition' variable in colInfo tells us what to do to form the
        # query that looks for the needed bucket sizes. 'none' means don't 
        # make any condition at all
        for col in columns:
            colInfo[col] = dict(condition='none')
        for ct in ans:
            if (ct[0] in colInfo) and colInfo[ct[0]]:
                colInfo[ct[0]]['colType'] = ct[1]
        if self._p: self._pp.pprint(colInfo)
        # To decide how big our ranges should be, we need to know how many
        # distinct UIDs per value
        uid = params['uid']
        sql = str(f"select count(distinct {uid}), ")
        more = ''
        for col in columns:
            more += str(f"count(distinct {col}), min({col}), max({col}), ")
        more = more[0:-2] + ' '
        sql += more + str(f"from {table}") 
        if self._p: print(sql)
        query = dict(db="raw",sql=sql)
        x.askExplore(query)
        ans = x.getExplore()
        if not ans:
            x.cleanUp(exitMsg="Failed query 1")
        if 'error' in ans:
            x.cleanUp(exitMsg="Failed query 1 (error)")
        if self._p: self._pp.pprint(ans)
        row = ans['answer'][0]
        dUids = row[0]
        i = 1
        for col in columns:
            colInfo[col]['dVals'] = row[i]
            colInfo[col]['minVal'] = row[i+1]
            colInfo[col]['maxVal'] = row[i+2]
            i += 3
        if self._p: self._pp.pprint(colInfo)
    
        sql = str(f"select count(*) from (select ")
        more = ''
        for col in columns:
            more += str(f"{col}, ")
        more = more[0:-2] + ' '
        groupby = makeGroupBy(columns)
        sql += more + str(f"from {table} ") + groupby + ") t"
        if self._p: print(sql)
        query = dict(db="raw",sql=sql)
        x.askExplore(query)
        ans = x.getExplore()
        if not ans:
            x.cleanUp(exitMsg="Failed query 2")
        if 'error' in ans:
            x.cleanUp(exitMsg="Failed query 2 (error)")
        if self._p: self._pp.pprint(ans)
        dVals = ans['answer'][0][0]
        # base is the number of UIDs per combined val
        base = dUids/dVals
        # target here is the number of number of UIDs per bucket that I want
        target = minCount + ((maxCount - minCount)/2);
        # I want to compute by what factor I need to grow the uid/bucket
        # count in order to get close to the target
        grow = target/base
        if self._p: print(f"base {base}, target {target}, grow {grow}")
        if grow <= 2:
            # I can't usually grow by anything less than 2x, so let's just
            # go with no conditions
            if self._p: print("Needed growth to small, so use column values as is")
            sql = self._makeHistSql(table,columns,colInfo,uid,minCount,maxCount)
            if self._p: print(sql)
            self._ret.append(self._queryAndGather(x,sql,colInfo,columns,minCount,maxCount))
            if self._p: self._pp.pprint(self._ret)
            x.cleanUp(doExit=False)
            return
    
        # We'll need to generalize, so see if we have text or datetime columns,
        # and gather the information needed to know roughly the number of
        # distinct UIDs we'll be able to get
        for col in columns:
            if colInfo[col]['colType'] != 'text':
                continue
            sql = str(f"select count(distinct ones), count(distinct twos), "
                    f"count(distinct threes) from ( "
                    f"select lower(left({col},1)) as ones, "
                    f"lower(left({col},2)) as twos, "
                    f"lower(left({col},3)) as threes from {table}) t")
            if self._p: print(sql)
            query = dict(db="raw",sql=sql)
            x.askExplore(query)
            ans = x.getExplore()
            if not ans:
                x.cleanUp(exitMsg="Failed query")
            if 'error' in ans:
                x.cleanUp(exitMsg="Failed query (error)")
            if self._p: self._pp.pprint(ans)
            colInfo[col]['buckets'] = []
            colInfo[col]['buckets'].append([1,ans['answer'][0][0]])
            colInfo[col]['buckets'].append([2,ans['answer'][0][1]])
            colInfo[col]['buckets'].append([3,ans['answer'][0][2]])
            if self._p: self._pp.pprint(colInfo)
    
        for col in columns:
            if colInfo[col]['colType'][:4] != 'date':
                continue
            sql = str(f"select count(distinct years), count(distinct months), "
                    f"count(distinct days) from ( select "
                    f"extract(year from {col})::integer as years, "
                    f"extract(month from {col})::integer as months, "
                    f"extract(day from {col})::integer as days "
                    f"from {table}) t")
            if self._p: print(sql)
            query = dict(db="raw",sql=sql)
            x.askExplore(query)
            ans = x.getExplore()
            if not ans:
                x.cleanUp(exitMsg="Failed query")
            if 'error' in ans:
                x.cleanUp(exitMsg="Failed query (error)")
            if self._p: self._pp.pprint(ans)
            colInfo[col]['buckets'] = []
            colInfo[col]['buckets'].append(['year',ans['answer'][0][0]])
            colInfo[col]['buckets'].append(['month',ans['answer'][0][1]])
            colInfo[col]['buckets'].append(['day',ans['answer'][0][2]])
            if self._p: self._pp.pprint(colInfo)
    
        if len(columns) == 1:
            # If just one column, then simply find a good bucketization
            factor = 1
            while(1):
                if colInfo[col]['colType'][:4] == 'date':
                    newColInfo = self._generalizeTextOrDatetime(col,colInfo,grow)
                elif colInfo[col]['colType'] == 'text':
                    newColInfo = self._generalizeTextOrDatetime(col,colInfo,grow)
                else:    # int or real
                    newColInfo = self._generalizeNumber(col,colInfo,grow=grow)
                if newColInfo is None:
                    if self._p: print("Couldn't generalize at all")
                    return
                sql = self._makeHistSql(table,columns,newColInfo,uid,minCount,maxCount)
                if self._p: print(sql)
                answer = self._queryAndGather(x,sql,newColInfo,columns,minCount,maxCount)
                if self._p: self._pp.pprint(answer)
                if (len(answer['buckets']) > 0) and self._ansNotDup(answer):
                    self._ret.append(answer)
                if factor != 1:
                    break
                if answer['guess'] == 'bucketsJustRight':
                    break
                if answer['guess'] == 'bucketsTooBig':
                    factor = 0.5
                else:
                    factor = 2
                if self._p: self._pp.pprint(self._ret)
            x.cleanUp(doExit=False)
            return
    
        # What I'll do is take one of the columns, create an increasing number
        # of buckets for it, and then set the appropriate number of bucket for
        # the other column. Ideal candidate for this is a numerical column with
        # lots of distinct values (cause can make more bucket sizes). Next would
        # be datetime, and last would be text
        numDistinct = 0
        col1 = ''
        for col in columns:
            if ((colInfo[col]['colType'] == "real") or
                    ((colInfo[col]['colType'][:3] == "int"))):
                if colInfo[col]['dVals'] > numDistinct:
                    numDistinct = colInfo[col]['dVals']
                    col1 = col
        if numDistinct == 0:
            # Didn't find a numeric column, so look for datetime
            for col in columns:
                if colInfo[col]['colType'][:4] == "date":
                    col1 = col
        if len(col1) == 0:
            # Didn't find a datetime either, so just pick the first one
            col1 = columns[0]
    
        if columns[0] == col1:
            col2 = columns[1]
        else:
            col2 = columns[0]
        if self._p: print(f"col1 is {col1}, type {colInfo[col1]['colType']}")
        if self._p: print(f"col2 is {col2}, type {colInfo[col2]['colType']}")
        if ((colInfo[col1]['colType'] == "real") or
                ((colInfo[col1]['colType'][:3] == "int"))):
            numBuckets = 2
            while(1):
                partColInfo = self._generalizeNumber(
                        col1,colInfo,targetBuckets=numBuckets)
                if partColInfo == None:
                    if self._p: print(f"partColInfo == None (numBuckets {numBuckets})")
                    break
                # partColInfo now has the structure for col1 set. We need to set
                # the sturcture for col2.
                if self._p: print("partColInfo:")
                if self._p: self._pp.pprint(partColInfo)
                fudge = 1
                allDone = 0
                while(1):
                    if colInfo[col2]['colType'][:4] == 'date':
                        newColInfo = self._generalizeTextOrDatetime(col2,colInfo,grow)
                        allDone = 1
                    elif colInfo[col2]['colType'] == 'text':
                        newColInfo = self._generalizeTextOrDatetime(col2,colInfo,grow)
                        allDone = 1
                    else:    # int or real
                        newColInfo = self._generalizeNumber(
                            col2,partColInfo,grow=grow,factor=(numBuckets*fudge))
                    if newColInfo is None:
                        if self._p: print(f"newColInfo == None (numBuckets {numBuckets})")
                        allDone = 1
                        break
                    if self._p: print("newColInfo:")
                    if self._p: self._pp.pprint(newColInfo)
                    sql = self._makeHistSql(
                            table,columns,newColInfo,uid,minCount,maxCount)
                    if self._p: print(sql)
                    answer = self._queryAndGather(
                        x,sql,newColInfo,columns,minCount,maxCount)
                    if self._p: self._pp.pprint(answer)
                    if (len(answer['buckets']) > 0) and self._ansNotDup(answer):
                        self._ret.append(answer)
                    if fudge != 1:
                        break
                    if answer['guess'] == 'bucketsJustRight':
                        break
                    if answer['guess'] == 'bucketsTooBig':
                        fudge = 0.5
                    else:
                        fudge = 2
                if allDone:
                    break
                if self._p: self._pp.pprint(self._ret)
                numBuckets *= 2
    
            x.cleanUp(doExit=False)
            return
    
        # Neither column is a number. For now, we require that at least one
        # column is numeric
        x.cleanUp(doExit=False)
        return
