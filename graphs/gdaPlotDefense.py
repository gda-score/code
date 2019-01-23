import os
import sys
import matplotlib.pyplot as plt
import math
from matplotlib.patches import Rectangle
import json
import numpy as np
sys.path.append('../common')
from gdaUtilities import getInterpolatedValue
import pprint
pp = pprint.PrettyPrinter(indent=4)

def buildOneDiagram(score, oneScore, fileName, form, show, plotType):
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

    over, conf, prob, know, work, susc = set(range(6))
    labels = ['Combined', 'Conf', 'Prob', 'Know', 'Work', 'Susc']
    doLabel = [1, 1, 1, 1, 1, 1]
    maxWork = 10000
    centers = list(range(6))
    widths = list(range(6))

    # overall bar
    left = gap
    right = (left + base) * 2
    wid = right - left
    mid = (wid / 2) + left
    centers[over] = mid
    widths[over] = wid

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
    # Change the axis due to generate Boundary box
    plt.figure()
    plt.axis([minXaxis, rightEdge + 2 * gap, minY - minYaxis, maxY + minYaxis])
    plt.axis('off')
    s = oneScore
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

    if s['knowledgeNeeded'] != None:
        knowledgeNeeded = s['knowledgeNeeded']
    else:
        # results in no bar for knowledgeNeeded
        knowledgeNeeded = maxY
        doLabel[know] = 0

    accuracyHeight = None

    # Compute Overall height and color
    if s['defense'] == 0:
        overallHt = -0.5
    elif s['defense'] == 1:
        overallHt = 0.5
    else:
        overallHt = s['defense'] - maxY

    valColors = ['black','black','black','black','black','black']
    if s['defense'] > 0.9:
        overallColor = 'green'
        valColors[over] = 'white'
    elif s['defense'] > 0.7:
        overallColor = 'blue'
        valColors[over] = 'white'
    elif s['defense'] > 0.5:
        overallColor = 'yellow'
        valColors[over] = 'black'
    elif s['defense'] > 0.3:
        overallColor = 'orange'
        valColors[over] = 'black'
    else:
        overallColor = 'red'
        valColors[over] = 'black'

    colors = [overallColor,
              'cadetblue', 'cadetblue',
              'grey', 'grey', 'grey']

    confImp = s['confidenceImprovement']
    if confImp > 1:
        confImp = 1
    elif confImp < 0:
        confImp = 0

    heights = [overallHt,
               maxY - confImp,
               maxY - s['claimProbability'],
               knowledgeNeeded - maxY,
               workBar - maxY,
               maxY - s['susceptibility']]

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
    if s['defense'] is not None:
        prOver = "%.2f" % (s['defense'])
    else:
        prOver = None
    if s['confidenceImprovement'] is not None:
        prConf = "%.2f" % (s['confidenceImprovement'])
    else:
        prConf = None
    if s['claimProbability'] is not None:
        prClaim = "%.2f" % (s['claimProbability'])
    else:
        prClaim = None
    if s['knowledgeNeeded'] is not None:
        prKnow = "%.2f" % (s['knowledgeNeeded'])
    else:
        prKnow = None
    if s['workNeeded'] is not None:
        prWork = "%.2f" % (s['workNeeded'])
    else:
        prWork = None
    if s['susceptibility'] is not None:
        prSus = "%.2f" % (s['susceptibility'])
    else:
        prSus = None
    labels_score = [prOver,prConf,prClaim,prKnow,prWork,prSus]
    for i in range(len(labels_score)):
        heightoriginal = heights[i]
        if doLabel[i] == 0:
            continue
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
        if doLabel[i] == 0:
            continue
        if heights[i] > 0:
            plt.text(centers[i], 0 - nudge, labels[i],
                     horizontalalignment='center', verticalalignment='top')
        else:
            plt.text(centers[i], 0 + nudge, labels[i],
                     horizontalalignment='center', verticalalignment='bottom')

    if (False):
        # Leave these lines, but don't make vertical bar for now
        currentAxis = plt.gca()
        currentAxis.add_patch(Rectangle((midVertical, overallHt),
            axisLength - midVertical, 0.045, alpha=0.6,
            facecolor=overallColor))
        currentAxis.add_patch(Rectangle((midVertical, overallHt),
            axisLength - midVertical, -0.045, alpha=0.6,
            facecolor=overallColor))

    # Set the overall proportion of the figure
    #plt.axes().set_aspect(aspect)
    plt.tight_layout()

    attack = score['params']
    anonSubType = None
    if 'attackType' in attack:
        attackType = attack['attackType']
    else:
        attackType = " "
    if 'anonType' in attack:
        anonType = attack['anonType']
    else:
        anonType = " "
    if 'anonSubType' in attack and len(attack['anonSubType']) > 0:
        anonSubType = attack['anonSubType']
    else:
        anonSubType = None
    if 'dbType' in attack:
        dbType = attack['dbType']
    else:
        dbType = " "
    used = len(s['columnsUsed'])
    if used == 0:
        info = "No attackable columns"
    elif used == 1:
        info = "Report on worst case attackable column"
    else:
        info = str(f"Report on {used} attacked columns")


    # Draw the title and explanation texts
    lowText = minY - lowerTextDrop
    if plotType == 'full':
        if anonSubType:
            highText = maxY + anonTypeLift + lowerTextDrop
        else:
            highText = maxY + anonTypeLift
        plt.text(textPlotXvalue, highText, anonType,
                 horizontalalignment='left', verticalalignment='top',
                 fontsize=20)
        if anonSubType:
            highText = highText - lowerTextDrop
            plt.text(textPlotXvalue + textIndent, highText, anonSubType,
                     horizontalalignment='left', verticalalignment='top',
                     fontsize=14)
        plt.text(textPlotXvalue, lowText, "Attack: " + attackType,
                  horizontalalignment='left', verticalalignment='top',
                  fontsize=15)
        lowText -= lowerTextDrop
        plt.text(textPlotXvalue + textIndent, lowText, info,
                  horizontalalignment='left', verticalalignment='top',
                  fontsize=12)
        lowText -= lowerTextDrop
        plt.text(textPlotXvalue, lowText, "Database: " + dbType,
                     horizontalalignment='left', verticalalignment='top',
                     fontsize=15)
    else:
        text = str(f"{anonType}, {dbType}, {attackType}")
        plt.text(textPlotXvalue, lowText, text,
                     horizontalalignment='left', verticalalignment='top',
                     fontsize=10)
        lowText -= (lowerTextDrop * 0.8)
        plt.text(textPlotXvalue + textIndent, lowText, info,
                     horizontalalignment='left', verticalalignment='top',
                     fontsize=10)

    # Plot the box
    if(False):
        # Go without box for now
        # draw horizontal lines
        plt.plot([horlineX, horlineY],
                [maxY + verlineGap, maxY + verlineGap],
                color='black', linewidth=2.8)  # line 1 top
        plt.plot([horlineX, horlineY],
                [minY + horlineX, minY + horlineX],
                color='black', linewidth=5)  # line 2 down

        # draw vertical lines
        changegap = -0.6
        plt.plot([horlineX, horlineX],
                [maxY + verlineGap, minY + changegap],
                color='black', linewidth=3.5)  # line 1 left
        plt.plot([verlineX, verlineY],
                [maxY + verlineGap, minY + horlineX],
                color='black', linewidth=3.9)  # line 2 right

    # For some reason, savefig has to come before show
    if len(fileName) > 0:
        for ext in form:
            path = str(f"{fileName}_{used}.{ext}")
            plt.savefig(path)
    if show:
        plt.show()
    return

# This method generates the defense GDA Score diagram
def plotDefenseScore(score, fileName='', form=['png'], show=True,
        plotType='full'):
    """ Produces a GDA Defense Score Diagram from GDA Score data.

        `score` is the score data structure returned from
        `gdaScores.getScore()` <br/>
        `fileName` is where the output file, if any, should be saved <br\>
        `form` is a list of output types, and can be 'png', 'pdf', 'ps',
        'eps', and 'svg' <br\>
        Set `show` to True if you want the graph displayed
    """

    for oneScore in score['score']['scores']:
        buildOneDiagram(score, oneScore, fileName, form, show, plotType)
