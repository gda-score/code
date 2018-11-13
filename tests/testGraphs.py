import sys
import os
import pprint
import json
import argparse
sys.path.append('../graphs')
from gdaPlot import plotGdaScore

# score data
score = {}
score['score'] = {}
score['score']['scores'] = {}
s = score['score']['scores']
s['workNeeded'] = 2
s['defense'] = .4
s['confidenceImprovement'] = .8
s['claimProbability'] = .2
s['knowledgeNeeded'] = .46
s['susceptibility'] = .6
s['color'] = 'black'
# code changes by Anirban 02-10-2018
s['name'] =""
s['rawDb'] ="gdaScoreBankingRaw"
s['anonDb'] = "gdaScoreBankingRaw"
s['criteria'] = ""
s['dbType'] = "banking"
s['annonScheme'] = "Diffrential Privacy"
s['attackText'] = "Averaging"

# code change by Anirban 20-10-2018
# score column data
score_column = {}
sc = score_column
sc['column'] = 2
sc['method'] = 'mpi_sws_basic_v1'

# utility data
u = {}
# TODO: Commented utility measures like coverage and accuracy as it comes from .json
# file from now.
u['filelocation'] = os.path.abspath('../utilityjson/exampleUtility.json')
# u['coverage'] = 0.8
# u['accuracy'] = 0.48
# u['utility'] = 0.8
u['color'] = 'black'
# Code Added By Anirban 30-08-2018
# Add another parameter on the callable method for score and column combination # Issue 16
plot = plotGdaScore(score,sc,u)
