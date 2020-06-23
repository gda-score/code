import pprint
from gdascore.gdaAttack import gdaAttack
from gdascore.gdaScore import gdaScores
from gdascore.gdaTools import setupGdaAttackParameters

def showResult(x,result,uidCol=None):
    print(f"Received result length {len(result)}")
    if uidCol is not None:
        uids = []
        for thing in result:
            uids.append(thing[uidCol])
        print(f"Distinct UIDs: {len(set(uids))}")
    for i in range(5):
        print(f"   {result[i]}")
    attackResult = x.getResults()
    sc = gdaScores(attackResult)
    score = sc.getScores()
    print(f"knowledgeCells = {score['base']['knowledgeCells']}")
    x.cleanUp(doExit=False)

pp = pprint.PrettyPrinter(indent=4)

doCache = True

config = {
    'anonTypes': [ ['no_anon'] ],
    'tables': [ ['banking','transactions'] ]
}

paramsList = setupGdaAttackParameters(config)
params = paramsList[0]
pp.pprint(params)


# Test bad inputs
if False:
    x = gdaAttack(params)
    result = x.getPriorKnowledge(['frequency'],'users',selectColumn='bad',values=[1])
    result = x.getPriorKnowledge(['bad'],'rows',count=20)
    result = x.getPriorKnowledge(['frequency'],'users',selectColumn='uid',values='bad')
    result = x.getPriorKnowledge(['frequency'],'users',selectColumn='uid',colRange='bad')
    result = x.getPriorKnowledge(['frequency'],'users',selectColumn='uid')
    result = x.getPriorKnowledge(['frequency'],'rows',count=20,selectColumn='uid')
    result = x.getPriorKnowledge(['frequency'],'rows',fraction=0.55,count=20)
    result = x.getPriorKnowledge(['frequency'],'rows',count=3.55)
    result = x.getPriorKnowledge(['frequency'],'rows',fraction=20)
    result = x.getPriorKnowledge(['frequency'],'rows',count='bad')
    result = x.getPriorKnowledge(['frequency'],'rows',fraction='bad')
    result = x.getPriorKnowledge(['frequency'],'rows')
    result = x.getPriorKnowledge(['frequency'],'boo')
    result = x.getPriorKnowledge('uid','rows')
    x.cleanUp(doExit=False)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','lastname'],'users',count=500")
result = x.getPriorKnowledge(['uid','lastname'],'users',count=500)
print("x.getPriorKnowledge(['uid','lastname'],'rows',count=500")
result = x.getPriorKnowledge(['uid','lastname'],'rows',count=500)
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','lastname'],'users',count=500")
result = x.getPriorKnowledge(['uid','lastname'],'users',count=500)
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','lastname'],'rows',count=500")
result = x.getPriorKnowledge(['uid','lastname'],'rows',count=500)
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','lastname'],'rows',selectColumn='acct_district_id',values=[1,2,3,4])")
result = x.getPriorKnowledge(['uid','lastname'],'rows',selectColumn='acct_district_id',values=[1,2,3,4])
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','lastname'],'rows',selectColumn='acct_district_id',colRange=[0,20])")
result = x.getPriorKnowledge(['uid','lastname'],'rows',selectColumn='acct_district_id',colRange=[0,20])
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','lastname'],'rows',selectColumn='lastname',colRange=['A','C'])")
result = x.getPriorKnowledge(['uid','lastname'],'rows',selectColumn='lastname',colRange=['A','C'])
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','frequency'],'users',fraction=0.05)")
result = x.getPriorKnowledge(['uid','frequency'],'users',fraction=0.05)
showResult(x,result,uidCol=0)

print("------------------------------------------------")
x = gdaAttack(params)
print("x.getPriorKnowledge(['uid','frequency'],'rows',fraction=0.05)")
result = x.getPriorKnowledge(['uid','frequency'],'rows',fraction=0.05)
showResult(x,result,uidCol=0)


