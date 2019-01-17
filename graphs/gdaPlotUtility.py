import os
import sys
import math
import json
import matplotlib.pyplot as plt
sys.path.append('../common')
from gdaUtilities import getInterpolatedValue
import pprint
pp = pprint.PrettyPrinter(indent=4)


# Future use for static unchanged strings
doubleColumnScore = "doubleColumnScores"
singleColumnScore = "singleColumnScores"

# Only for accuracy
acc = "accuracy"
simplerelerrormatrix = "simpleRelativeErrorMetrics"
mse = "meanSquareError"

# getAccuracyScore() for now returns the average complex relative error
# computed as: "(abs(count(distinct_rawDb)-count(distinct_anonDb))/
#                max(count(distinct_rawDb),count(distinct_anonDb)))"
def getAccuracyScore(score):
    return score['accuracy']['accuracy']['relErrorMetrics']['avg']

# getCoverageScore() computes the average per-column coverage
def getCoverageScore(score):
    coverage = score['coverage']
    numColumns = len(coverage)
    covScore = 0.0
    for col in coverage:
        cpc = col['coverage']['coveragePerCol']
        if cpc is None:
            numColumns -= 1
        else:
            covScore += cpc
    covScore /= numColumns
    return covScore

def buildOneDiagram(score, fileName, form, show):
    # tweak the shape by playing with following numbers
    base = 1
    gap = base / 4
    high = base * 3
    aspect = 5
    maxY = 0.5
    minY = -0.5
    nudge = 0.03

    # add parameters for axis change
    minXaxis = -0.3
    minYaxis = 0.3
    axisLength = 9.75
    textPlotXvalue = -0.1
    textIndent = 1

    anonTypeLift = 0.2
    lowerTextDrop = 0.11

    horlineX = -0.3
    horlineY = 24.5

    verlineX = 10
    verlineY = 10
    verlineGap = 0.25

    scoregap = 0.04

    heightposth = 0.05
    heightnegth = -0.05

    # end of parameters

    xaxisscorecolumn = 4.3
    yaxisscorecolumn = -0.65

    numBars = 3
    over, acc, cov = set(range(numBars))
    labels = ['Combined', 'Acc', 'Cov']
    centers = list(range(numBars))
    widths = list(range(numBars))

    # overall bar
    left = gap
    right = (left + base) * 2
    wid = right - left
    mid = (wid / 2) + left
    centers[over] = mid
    widths[over] = wid

    midVertical = right + gap

    # accuracy bar
    left = right + (2 * gap)
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

    rightEdge = right + gap
    # Change the axis due to generate Boundary box
    plt.figure()
    plt.axis([minXaxis, rightEdge + 2 * gap, minY - minYaxis, maxY + minYaxis])
    plt.axis('off')

    covScore = getCoverageScore(score)
    accScore = getAccuracyScore(score)

    overallMapping = [
        (1, 1, 0), (1, .25, 0), (1, .1, 0), (1, .05, 0),
        (1, .01, 0), (1, 0, 0),
        (.6, 1, 0), (.6, .25, .1), (.6, .1, .3), (.6, .05, .4),
        (.6, .01, .5), (.6, 0, .6),
        (.4, 1, 0), (.4, .25, .2), (.4, .1, .4), (.4, .05, .6),
        (.4, .01, .7), (.4, 0, .8),
        (.2, 1, 0), (.2, .25, .3), (.2, .1, 5), (.2, .05, .7),
        (.2, .01, .8), (.2, 0, .9),
        (.1, 1, 0), (.1, .25, .4), (.1, .1, .7), (.1, .05, .8),
        (.1, .01, .9), (.1, 0, .95),
        (0, 1, 0), (0, .25, .5), (0, .1, .75), (0, .05, .9),
        (0, .01, .95), (0, 0, 1)]
    overall = getInterpolatedValue((1 - covScore), accScore, overallMapping)

    accuracyMapping = [
            (1,1,-0.5),(0.5,0.5,-0.25),(0.25,0.25,0),(0.1,0.1,0.1),
            (0.05,0.05,0.3),(0.01,0.01,0.4),(0.001,0.001,0.48),(0,0,0.5)
            ]
    accuracyHeight = getInterpolatedValue(accScore, accScore, accuracyMapping)

    valColors = ['black','black','black']
    if overall > 0.9:
        overallColor = 'green'
        valColors[over] = 'white'
    elif overall > 0.7:
        overallColor = 'blue'
        valColors[over] = 'white'
    elif overall > 0.5:
        overallColor = 'yellow'
        valColors[over] = 'black'
    elif overall > 0.3:
        overallColor = 'orange'
        valColors[over] = 'black'
    else:
        overallColor = 'red'
        valColors[over] = 'black'

    colors = [overallColor, 'tan', 'tan']

    heights = [overall - maxY,
               accuracyHeight,
               covScore - maxY]

    # Plot the bars
    plt.bar(centers, heights, width=widths, color=colors)
    # Plot the axes
    plt.plot([0, axisLength], [0, 0], color='black', linewidth=4)
    plt.plot([midVertical, midVertical], [minY, maxY], color='black', linewidth=4)
    # Plot the good/bad labels
    plt.text(midVertical, maxY + (1.5 * nudge), 'good',
             horizontalalignment='center', verticalalignment='center')
    plt.text(midVertical, minY - (1.5 * nudge), 'bad',
             horizontalalignment='center', verticalalignment='center')

    # Plot the score above of the edge of the Bar.
    # If Bar is not visible properly then just beside the Bar.
    # Need to limit decimal places
    prOver = "%.2f" % overall
    prAcc = "%.2f" % accScore
    prCov = "%.2f" % covScore
    labels_score = [prOver,prAcc,prCov]
    for i in range(len(labels_score)):
        heightoriginal = heights[i]
        if (heightoriginal > 0):
            if (heightoriginal <= heightposth):
                plt.text(centers[i], heightoriginal + scoregap,
                        labels_score[i], horizontalalignment='center',
                        verticalalignment='center',color=valColors[i])
            else:
                plt.text(centers[i], heightoriginal - scoregap,
                        labels_score[i], horizontalalignment='center',
                        verticalalignment='center',color=valColors[i])
        else:
            if (heightoriginal >= heightnegth):
                plt.text(centers[i], heightoriginal - scoregap,
                        labels_score[i], horizontalalignment='center',
                        verticalalignment='center',color=valColors[i])
            else:
                plt.text(centers[i], heightoriginal + scoregap,
                        labels_score[i], horizontalalignment='center',
                        verticalalignment='center',color=valColors[i])

    # plot the bar labels
    for i in range(len(labels)):
        if heights[i] > 0:
            plt.text(centers[i], 0 - nudge, labels[i],
                     horizontalalignment='center', verticalalignment='top')
        else:
            plt.text(centers[i], 0 + nudge, labels[i],
                     horizontalalignment='center', verticalalignment='bottom')

    # Set the overall proportion of the figure
    plt.axes().set_aspect(aspect)

    params = score['params']
    if 'anonType' in params:
        anonType = params['anonType']
    else:
        anonType = " "
    if 'anonSubType' in params and len(params['anonSubType']) > 0:
        anonSubType = params['anonSubType']
    if 'dbType' in params:
        dbType = params['dbType']
    else:
        dbType = " "

    # Draw the title and explanation texts
    highText = maxY + anonTypeLift + lowerTextDrop
    plt.text(textPlotXvalue, highText, anonType,
             horizontalalignment='left', verticalalignment='top', fontsize=20)
    highText = highText - lowerTextDrop
    plt.text(textPlotXvalue + textIndent, highText, anonSubType,
             horizontalalignment='left', verticalalignment='top', fontsize=14)
    lowText = minY - lowerTextDrop
    plt.text(textPlotXvalue, lowText, "Database: " + dbType,
                 horizontalalignment='left', verticalalignment='top',
                 fontsize=15)

    # For some reason, savefig has to come before show
    if len(fileName) > 0:
        for ext in form:
            path = str(f"{fileName}.{ext}")
            plt.savefig(path)
    if show:
        plt.show()
    return

# This method generates the defense GDA Score diagram
def plotUtilityScore(score, fileName='', form=['png'], show=True):
    """ Produces a GDA Utility Score Diagram from GDA Score data.

        `score` is the score data structure returned from
        `gdaScores.getScore()` <br/>
        `fileName` is where the output file, if any, should be saved <br\>
        `form` is a list of output types, and can be 'png', 'pdf', 'ps',
        'eps', and 'svg' <br\>
        Set `show` to True if you want the graph displayed
    """

    buildOneDiagram(score, fileName, form, show)
