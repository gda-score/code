import pprint
from common.gdaQuery import findQueryConditions

pp = pprint.PrettyPrinter(indent=4)

def oneThing(params,cols,minCount,maxCount,numColumns):
    q = findQueryConditions(params, cols, minCount, maxCount,
            numColumns=numColumns)
    print("------------------------------------------------")
    pp.pprint(q.getDataStructure())
    print("------------------------------------------------")
    while(1):
        res = q.getNextWhereClause()
        if res is None:
            break
        pp.pprint(res)
        print("-----")
    return

params = dict(name='exampleExplore1',
              rawDb='gdaScoreBankingRaw',
              anonDb='cloakBanking',
              criteria='singlingOut',
              table='accounts',
              uid='uid',
              flushCache=False,
              verbose=False)
oneThing(params, [],20,30,2)
quit()
oneThing(params, ["lastname", "cli_district_id", "birthdate", "firstname"], 2, 2, 0)
oneThing(params, ["lastname", "cli_district_id"], 2, 2, 0)
oneThing(params, ["cli_district_id"], 400, 800, 0)
oneThing(params, ["cli_district_id", "account_id"], 5, 10, 0)
oneThing(params, ["birthdate"], 100, 200, 0)
oneThing(params, ["lastname"], 50, 100, 0)
oneThing(params, ["cli_district_id","birthdate"], 5, 10, 0)
