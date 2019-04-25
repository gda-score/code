from common.gdaUtilities import getInterpolatedValue

utilityScore = [
    (1,1,0), (1,.25,0),  (1,.1,0),  (1,.05,0),  (1,.01,0),  (1,0,0),
    (.6,1,0),(.6,.25,.1),(.6,.1,.3),(.6,.05,.4),(.6,.01,.5),(.6,0,.6),
    (.4,1,0),(.4,.25,.2),(.4,.1,.4),(.4,.05,.6),(.4,.01,.7),(.4,0,.8),
    (.2,1,0),(.2,.25,.3),(.2,.1,5), (.2,.05,.7),(.2,.01,.8),(.2,0,.9),
    (.1,1,0),(.1,.25,.4),(.1,.1,.7),(.1,.05,.8),(.1,.01,.9),(.1,0,.95),
    (0,1,0), (0,.25,.5), (0,.1,.75),(0,.05,.9), (0,.01,.95),(0,0,1)
]

def tryUtilityScore(cov,acc):
    score = getInterpolatedValue((1-cov),acc,utilityScore)
    print(f"coverage {cov}, accuracy {acc}, score {score}")

tryUtilityScore(-1,2)
tryUtilityScore(0,1)
tryUtilityScore(.2,.8)
tryUtilityScore(.2,.5)
tryUtilityScore(.4,.5)
tryUtilityScore(.4,.2)
tryUtilityScore(.6,.3)
tryUtilityScore(.6,.1)
tryUtilityScore(.9,.02)
tryUtilityScore(1,.02)
tryUtilityScore(1,.01)
tryUtilityScore(1,0)
tryUtilityScore(2,-1)
