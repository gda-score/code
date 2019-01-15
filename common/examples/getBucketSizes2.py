import sys
import pprint
# note simplejson because issues serializing decimal.Decimal
import simplejson as json
sys.path.append('../../common')
from gdaQuery import findQueryConditions
from gdaScore import gdaAttack

pp = pprint.PrettyPrinter(indent=4)

# This example gathers a set of query conditions for one set of bin sizes,
# between 15 and 50 distinct users. (This is the case for building synthetic
# data)

results = []

params = dict(name='getBucketSizes2',
              rawDb='gdaScoreTaxiRaw',
              anonDb='cloakTaxi',
              criteria='singlingOut',
              table='rides',
              uid='uid',
              flushCache=False,
              verbose=False)

x = gdaAttack(params)
colsTypes = x.getColNamesAndTypes(dbType="anonDb")
pp.pprint(colsTypes)
x.cleanUp(doExit=False)

f = open('getBucketSizes2.txt', 'w')

# I'm just going to do this brute force ...
for entries in colsTypes:
    col = entries[0]
    colType = entries[1]
    # For now we'll only deal with ints and reals
    if ((colType != 'real') and (colType != 'integer')):
        print(f"Skip column {col} with type {colType}")
        continue
    q = findQueryConditions(params, [col], 15, 50)
    while(1):
        res = q.getNextWhereClause()
        if res is None:
            break
        jres = json.dumps(res, indent=4)
        f.write(jres)
        print(jres)
f.close()
