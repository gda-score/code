import sys
import os
import pprint
import json
import argparse
sys.path.append('../graphs')
from gdaPlotUtility import plotUtilityScore

force = False
if len(sys.argv) == 2 and sys.argv[1] == '-f':
    force = True

pp = pprint.PrettyPrinter(indent=4)
resDir = '../utility/utilityResults'

files = os.listdir(resDir)
for file in files:
    if file[-4:] != 'json':
        continue
    body = file[0:-5]
    if not force:
        check = body + '.png'
        if check in files:
            next
    path = resDir + '/' + file
    plotName = resDir + '/' + body
    with open(path, 'r') as f:
        print(f"loading {path}")
        score = json.load(f)
        #pp.pprint(score)
        sc = dict(column='aColumn',method='aMethod')
        plot = plotUtilityScore(score, fileName=plotName,
                form=['png'], show=False)