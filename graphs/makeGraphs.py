import sys
import os
import pprint
import json
import argparse
sys.path.append('../graphs')
from gdaPlotDefense import plotDefenseScore
from gdaPlotUtility import plotUtilityScore

def runOneDirectory(jsonDir, graphDir, plotType, scoreType, force):
    jsonFiles = os.listdir(jsonDir)
    graphFiles = os.listdir(graphDir)
    for jsonFile in jsonFiles:
        if jsonFile[-4:] != 'json':
            continue
        body = jsonFile[0:-5]
        if not force:
            check = body + '.png'
            if check in graphFiles:
                next
        jsonPath = jsonDir + '/' + jsonFile
        plotBody = graphDir + '/' + body
        with open(jsonPath, 'r') as f:
            print(f"loading {jsonPath}")
            score = json.load(f)
            #pp.pprint(score)
            if scoreType == 'defense':
                plotDefenseScore(score, fileName=plotBody,
                        form=['png'], show=False, plotType=plotType)
            else:
                plotUtilityScore(score, fileName=plotBody,
                        form=['png'], show=False, plotType=plotType)

forceOverwrite = False
if len(sys.argv) == 2 and sys.argv[1] == '-f':
    forceOverwrite = True

pp = pprint.PrettyPrinter(indent=4)
indices1 = [0,1]
jsonDirs = ['../attacks/attackResults', '../utility/utilityResults']
scoreTypes = ['defense', 'utility']
indices2 = [0,1]
subDirs = ['webGraphs','fullGraphs']
plotTypes = ['web','full']

for i in indices1:
    jsonDir = jsonDirs[i]
    scoreType = scoreTypes[i]
    for j in indices2:
        subDir = subDirs[j]
        plotType = plotTypes[j]
        graphDir = jsonDir + "/" + subDir
        runOneDirectory(jsonDir, graphDir, plotType, scoreType, forceOverwrite)
