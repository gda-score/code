import sys
import pprint
import json
sys.path.append('../../common')
from gdaQuery import findQueryConditions
from gdaScore import gdaAttack

pp = pprint.PrettyPrinter(indent=4)

# This example gather a set of query conditions of 6 different size ranges
# and stores the results as json

results = []

params = dict(name='exampleExplore1',
              rawDb='gdaScoreBankingRaw',
              anonDb='cloakBanking',
              criteria='singlingOut',
              table='accounts',
              uid='uid',
              flushCache=False,
              verbose=False)

x = gdaAttack(params)
colsTypes = x.getColNamesAndTypes(dbType="anonDb")
pp.pprint(colsTypes)
x.cleanUp(doExit=False)

# Build the structure that records number of duids in buckets
buckets = []
buckets.append(dict(min=5,max=10,count=0))
buckets.append(dict(min=10,max=20,count=0))
buckets.append(dict(min=20,max=50,count=0))
buckets.append(dict(min=50,max=100,count=0))
buckets.append(dict(min=100,max=200,count=0))
buckets.append(dict(min=200,max=500,count=0))

maxCount = 50     # we want this many buckets of each size

# I'm just going to do this brute force ...
for colType in colsTypes:
    col = colType[0]
    if col == 'uid':
        continue
    for bucket in buckets:
        if bucket['count'] >= maxCount:
            continue
        q = findQueryConditions(params, [col], bucket['min'], bucket['max'])
        while(1):
            res = q.getNextWhereClause()
            if res is None:
                break
            results.append(res)
            bucket['count'] += 1
            print(f"{col}: min {bucket['min']}, max {bucket['max']}, "
                    f"count {bucket['count']}")
            if bucket['count'] >= maxCount:
                break

for bucket in buckets:
    print(f"min {bucket['min']}, max {bucket['max']}, "
            f"count {bucket['count']}")

jresults = json.dumps(results, indent=4)
print(jresults)
with open('getBucketSizes.txt', 'w') as f:
    f.write(jresults)
