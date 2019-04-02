import os
import sys
import matplotlib.pyplot as plt
import math
from matplotlib.patches import Rectangle
import json
import numpy as np
sys.path.append('../common')
from common.gdaUtilities import getInterpolatedValue
import pprint
pp = pprint.PrettyPrinter(indent=4)


# Future use for static unchanged strings
doubleColumnScore = "doubleColumnScores"
singleColumnScore = "singleColumnScores"

# Only for accuracy
acc = "accuracy"
simplerelerrormatrix = "simpleRelativeErrorMetrics"
mse = "meanSquareError"

# Only for coverage
cov = "coverage"
covPerCol = "coveragePerCol"
# This method is responsible for generate
# Score and column data structure.
# Tested by static values
def score_column_method(column, method):
        scorestring = "Scoring:%s,%s columns" % (method, column)
        return scorestring

# This method is responsible for generate
# utility measure parameters.
# Tested by dynamically read from json file from a desired location.
def readjsonfile(filelocation, util):
    try:
        # check the desired file exist on the location or not.
        fileexist = open(filelocation, 'r')
        util['accuracy'] = round(getaccuracyvalue(filelocation), 2)
        util['coverage'] = round(getcoveragevalue(filelocation), 2)
        fileexist.close()
    except FileNotFoundError:
        print("File is not present in the location.")
        return False
    except KeyboardInterrupt:
        print("Program closed.")
        return False

    return True

def getaccuracyvalue(filelocation):
    accuracy = None
    # Read JSON data into the datastore variable
    if filelocation:
        with open(filelocation, 'r') as f:
            datastore = json.load(f)
    # Use the new datastore datastructure
    # Utility data
    utility_acc_dc = []
    utility_acc_sc = []

    for i in range(len(datastore[doubleColumnScore])):
        if datastore[doubleColumnScore][i][acc] is not None:
            utility_acc_dc.append(datastore[doubleColumnScore][i][acc][simplerelerrormatrix][mse])

    for i in range(len(datastore[singleColumnScore])):
        if datastore[singleColumnScore][i][acc] is not None:
            utility_acc_sc.append(datastore[singleColumnScore][i][acc][simplerelerrormatrix][mse])

    accuracy = ((np.mean(utility_acc_dc) + np.mean(utility_acc_sc)) / 2)
    return accuracy

def getcoveragevalue(filelocation):
    coverage = None
    # Read JSON data into the datastore variable
    if filelocation:
        with open(filelocation, 'r') as f:
            datastore = json.load(f)
    # Use the new datastore datastructure
    # Utility data coverage
    utility_cov_dc = []
    utility_cov_sc = []

    for i in range(len(datastore[doubleColumnScore])):
        if datastore[doubleColumnScore][i][cov] is not None:
            if datastore[doubleColumnScore][i][cov][covPerCol] is not None:
                utility_cov_dc.append(datastore[doubleColumnScore][i]["coverage"][covPerCol])

    for i in range(len(datastore[singleColumnScore])):
        if datastore[singleColumnScore][i][cov] is not None:
            if datastore[singleColumnScore][i][cov][covPerCol] is not None:
                utility_cov_sc.append(datastore[singleColumnScore][i][cov][covPerCol])
    coverage = ((np.mean(utility_cov_dc) + np.mean(utility_cov_sc)) / 2)
    return coverage

# This method is responsible for generate
# gdaScore plot using matplotlib.
# Tested by static and dynamic values.
def plotGdaScore(score, sc, util, fileName='', form=[], show=True):
    """ Produces a GDA Score Diagram from GDA Score data.

        `score` is the score data structure returned from
        `gdaScores.getScore()` <br/>
        `util` is the utility data structure (to be developed) <br\>
        `fileName` is where the output file, if any, should be saved <br\>
        `form` is a list of output types, and can be 'png', 'pdf', 'ps',
        'eps', and 'svg' <br\>
        Set `show` to True if you want the graph displayed
    """
    # tweak the shape by playing with following numbers
    base = 1
    gap = base / 4
    high = base * 3
    aspect = 5
    maxY = 0.5
    minY = -0.5

    # add parameters for axis change
    minXaxis = -0.3
    minYaxis = 0.3
    axisLength = 9.75
    textPlotXvalue = -0.1

    maintextValue = 0.05
    maintextYValue = 0.67
    midTextValue = 0.09
    endTextValue = 0.2

    horlineX = -0.3
    horlineY = 24.5

    verlineX = 10
    verlineY = 10
    verlineGap = 0.2

    scoregap = 0.04

    heightposth = 0.05
    heightnegth = -0.05

    # end of parameters

    # Code Added By Anirban 20-10-2018
    # Parameters for score and column
    numofcolumn = sc['column']
    methodname = sc['method']

    xaxisscorecolumn = 4.3
    yaxisscorecolumn = -0.65

    horbarYaxis = 0.0

    acc, cov, conf, prob, know, work, susc = set(range(7))
    labels = ['Acc', 'Cov', 'Conf', 'Prob', 'Know', 'Work', 'Susc']
    if util:
        doLabel = [1, 1, 1, 1, 1, 1, 1]
    else:
        doLabel = [0, 0, 1, 1, 1, 1, 1]
    maxWork = 10000
    centers = list(range(7))
    widths = list(range(7))

    # accuracy bar
    left = gap
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[acc] = mid
    widths[acc] = wid

    # coverage bar
    left = right + gap
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[cov] = mid
    widths[cov] = wid

    midVertical = right + gap

    # confidence bar
    left = right + (2 * gap)
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[conf] = mid
    widths[conf] = wid

    # probability bar
    left = right + gap
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[prob] = mid
    widths[prob] = wid

    # knowledge bar
    left = right + (2 * gap)
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[know] = mid
    widths[know] = wid

    # work bar
    left = right + gap
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[work] = mid
    widths[work] = wid

    # susceptibility bar
    left = right + gap
    right = left + base
    wid = right - left
    mid = (wid / 2) + left
    centers[susc] = mid
    widths[susc] = wid

    rightEdge = right + gap
    # Change the axis due to generate Boundary box Anirban 30-09-2011
    plt.figure()
    plt.axis([minXaxis, rightEdge + 2 * gap, minY - minYaxis, maxY + minYaxis])
    plt.axis('off')
    colors = ['tan', 'tan',
              'cadetblue', 'cadetblue',
              'grey', 'grey', 'grey']
    s = score['score']['scores'][0]
    # For work needed, best case is 1 cell learned for 1 cell attacked,
    # so this given value 1. We can set worst case arbitrarily at say
    # 10000, which gets value 0 on graph. We can plot log scale. So:
    workBar = s['workNeeded']
    if workBar != None:
        if workBar < 1:
            workBar = 1
        if workBar > 10000:
            workBar = 10000
        workBar = math.log10(workBar)
        maxWork = math.log10(maxWork)
        workBar /= maxWork
    else:
        # This will cause the work bar to not appear
        workBar = maxY
        doLabel[work] = 0

    # Code Added By Anirban 25-10-2018
    # Dynamically added acccuracy and coverage by reading json file
    # No further change till now to python data structure

    if util:
        filevalues = readjsonfile(util['filelocation'], util)
    else:
        filevalues = None
    # Do calculation if file exist on the location and initial reading calculation
    utilityScore = [
        (1, 1, 0), (1, .25, 0), (1, .1, 0), (1, .05, 0), (1, .01, 0), (1, 0, 0),
        (.6, 1, 0), (.6, .25, .1), (.6, .1, .3), (.6, .05, .4), (.6, .01, .5), (.6, 0, .6),
        (.4, 1, 0), (.4, .25, .2), (.4, .1, .4), (.4, .05, .6), (.4, .01, .7), (.4, 0, .8),
        (.2, 1, 0), (.2, .25, .3), (.2, .1, 5), (.2, .05, .7), (.2, .01, .8), (.2, 0, .9),
        (.1, 1, 0), (.1, .25, .4), (.1, .1, .7), (.1, .05, .8), (.1, .01, .9), (.1, 0, .95),
        (0, 1, 0), (0, .25, .5), (0, .1, .75), (0, .05, .9), (0, .01, .95), (0, 0, 1)]
    if filevalues:
        score = getInterpolatedValue((1 - util['coverage']), util['accuracy'], utilityScore)

    if util:
        accuracy = util['accuracy']
        coverage = util['coverage']
    else:
        # This basically results in no bars for utility
        accuracy = maxY
        coverage = maxY

    if s['knowledgeNeeded'] != None:
        knowledgeNeeded = s['knowledgeNeeded']
    else:
        # results in no bar for knowledgeNeeded
        knowledgeNeeded = maxY
        doLabel[know] = 0

    # Accuracy bar scaled Issue10
    accuracyHeight = None
    if util['accuracy'] == 0:
        accuracyHeight = 0.5
    elif util['accuracy'] == 0.01:
        accuracyHeight = 0.4
    elif util['accuracy'] == 0.05:
        accuracyHeight = 0.3
    elif util['accuracy'] == 0.1:
        accuracyHeight = 0.1
    elif util['accuracy'] == 0.25:
        accuracyHeight = 0.0
    elif util['accuracy'] == 0.5:
        accuracyHeight = -0.25
    else:
        accuracyHeight = -0.5

    heights = [accuracyHeight,
               # maxY - accuracy, Changes for Issue10
               # maxY - coverage, Changes for Issue10
               coverage - maxY,
               maxY - s['confidenceImprovement'],
               maxY - s['claimProbability'],
               knowledgeNeeded - maxY,
               workBar - maxY,
               maxY - s['susceptibility']]

    # Plot the bars
    plt.bar(centers, heights, width=widths, color=colors)
    # Plot the axes
    plt.plot([0, axisLength], [0, 0], color='black', linewidth=4)  # Changes done by Anirban 30-08-2018
    plt.plot([midVertical, midVertical], [minY, maxY], color='black', linewidth=4)
    # Plot the text
    nudge = 0.03
    plt.text(midVertical, maxY + nudge, 'good',
             horizontalalignment='center', verticalalignment='center')
    plt.text(midVertical, minY - nudge, 'bad',
             horizontalalignment='center', verticalalignment='center')

    # Plot the score above of the edge of the Bar. If Bar is not visible properly then
    # Just beside the Bar.
    # Changes Done By Anirban 09-10-2018
    # region Code Added to Add score as number
    labels_score = [util['accuracy'], util['coverage'], s['confidenceImprovement'], s['claimProbability'],
                    s['knowledgeNeeded'], s['workNeeded'], s['susceptibility']]
    for i in range(len(labels)):
        heightoriginal = heights[i]
        # heightmid = heights[i] - scoregap  # Changes done for displaying score just below of the bar
        if doLabel[i] == 0:
            continue
        if (heightoriginal > 0):
            if (heightoriginal <= heightposth):
                plt.text(centers[i], heightoriginal + scoregap, labels_score[i],
                         horizontalalignment='center', verticalalignment='center')
            else:
                plt.text(centers[i], heightoriginal - scoregap, labels_score[i],
                         horizontalalignment='center', verticalalignment='center')
        else:
            if (heightoriginal >= heightnegth):
                plt.text(centers[i], heightoriginal - scoregap, labels_score[i],
                         horizontalalignment='center', verticalalignment='center')
            else:
                plt.text(centers[i], heightoriginal + scoregap, labels_score[i],
                         horizontalalignment='center', verticalalignment='center')

    # endregion

    # plot the bar labels
    for i in range(len(labels)):
        if doLabel[i] == 0:
            continue
        if heights[i] > 0:
            plt.text(centers[i], 0 - nudge, labels[i],
                     horizontalalignment='center', verticalalignment='top')
        else:
            plt.text(centers[i], 0 + nudge, labels[i],
                     horizontalalignment='center', verticalalignment='bottom')
    # Plot the summary values and colors
    # Code Added
    currentAxis = plt.gca()
    # util['utility'] depends on score calculated above and Yaxis value of the Hbar.
    util['utility'] = score
    if score == 0:
        horbarYaxis = -0.5
    elif score == 1:
        horbarYaxis = 0.5
    else:
        horbarYaxis = score

    if util:
        # region Code Commented By Anirban 03-10-2018
        # plt.plot([0,midVertical-gap],
        #         [(util['utility']-maxY),(util['utility']-maxY)],
        #         color=util['color'],linewidth=18,alpha=0.6) #Changes needs to be done
        # end region
        # we must reuse the same code in future
        if util['utility'] > 0.9:
            color = 'green'
        elif util['utility'] > 0.7:
            color = 'blue'
        elif util['utility'] > 0.5:
            color = 'yellow'
        elif util['utility'] > 0.3:
            color = 'orange'
        else:
            color = 'red'
    currentAxis.add_patch(Rectangle((0, horbarYaxis), midVertical, 0.05, alpha=0.6,
                                        facecolor=color))  # height and width can be adjusted
    currentAxis.add_patch(Rectangle((0, horbarYaxis), midVertical, -0.05, alpha=0.6,
                                    facecolor=color))  # height and width can be adjusted

    if s['defense'] > 0.9:
        color = 'green'
    elif s['defense'] > 0.7:
        color = 'blue'
    elif s['defense'] > 0.5:
        color = 'yellow'
    elif s['defense'] > 0.3:
        color = 'orange'
    else:
        color = 'red'
    # TODO: Y axis value for defense is currently hardcoded, Need to be updated in future.
    currentAxis.add_patch(Rectangle((midVertical, -0.3), axisLength - midVertical, 0.045, alpha=0.6,
                                    facecolor=color))  # s['defense'] - maxY, # height and width can be adjusted
    currentAxis.add_patch(Rectangle((midVertical, -0.3), axisLength - midVertical, -0.045, alpha=0.6,
                                    facecolor=color))  # s['defense'] - maxY, # height and width can be adjusted
    # region Code Commented By Anirban 03-10-2018
    # plt.plot([midVertical+gap,rightEdge],[(s['defense']-maxY),(s['defense']-maxY)],
    # color=color,linewidth=23,alpha=0.6) #Changes needs to be done
    # end region

    # Set the overall proportion of the figure
    plt.axes().set_aspect(aspect)

    mainText = s['annonScheme']  # hard coded data validation
    attackText = s['attackText']
    dbType = s["dbType"]


    plt.text(textPlotXvalue, maintextYValue, mainText,
             horizontalalignment='left', verticalalignment='top', fontsize=20)
    plt.text(textPlotXvalue, minY - midTextValue, "Attack: " + attackText,
              horizontalalignment='left', verticalalignment='top', fontsize=15)
    plt.text(textPlotXvalue, minY - endTextValue, "Database: " + dbType,
                 horizontalalignment='left', verticalalignment='top', fontsize=15)

    # Changes Done By Anirban 30-08-2018
    # Plot the box
    # draw horizontal lines
    plt.plot([horlineX, horlineY], [maxY + verlineGap, maxY + verlineGap], color='black', linewidth=2.8)  # line 1 top
    plt.plot([horlineX, horlineY], [minY + horlineX, minY + horlineX], color='black', linewidth=5)  # line 2 down

    # draw vertical lines
    changegap = -0.6
    plt.plot([horlineX, horlineX], [maxY + verlineGap, minY + changegap], color='black', linewidth=3.5)  # line 1 left
    plt.plot([verlineX, verlineY], [maxY + verlineGap, minY + horlineX], color='black', linewidth=3.9)  # line 2 right

    # Code Added by Anirban 20-10-2018
    # code to display method and number of column used in score generation
    displaytext = score_column_method(numofcolumn, methodname)
    plt.text(xaxisscorecolumn, yaxisscorecolumn, displaytext,
             horizontalalignment='left', verticalalignment='top', fontsize=11)
    # End of Change

    # For some reason, savefig has to come before show
    if len(fileName) > 0:
        for ext in form:
            path = fileName + '.' + ext
            plt.savefig(path)
    if show:
        plt.show()
    return
