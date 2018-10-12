import matplotlib.pyplot as plt
import math
from matplotlib.patches import Rectangle


def plotGdaScore(score, util, fileName='', form=[], show=True):
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
    minXaxis = -0.5
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

    # end of parameters
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
    s = score['score']['scores']
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

    heights = [maxY - accuracy,
               maxY - coverage,
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
    if util:
        # region Code Commented By Anirban 03-10-2018
        # plt.plot([0,midVertical-gap],
        #         [(util['utility']-maxY),(util['utility']-maxY)],
        #         color=util['color'],linewidth=18,alpha=0.6) #Changes needs to be done
        # end region
        currentAxis.add_patch(Rectangle((0, util['utility'] - maxY), midVertical, 0.10, alpha=0.6,
                                        facecolor=util['color']))  # height and width can be adjusted

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

    currentAxis.add_patch(Rectangle((midVertical, s['defense'] - maxY), axisLength - midVertical, 0.11, alpha=0.6,
                                    facecolor=color))  # height and width can be adjusted
    # region Code Commented By Anirban 03-10-2018
    # plt.plot([midVertical+gap,rightEdge],[(s['defense']-maxY),(s['defense']-maxY)],
    # color=color,linewidth=23,alpha=0.6) #Changes needs to be done
    # end region

    # Set the overall proportion of the figure
    plt.axes().set_aspect(aspect)
    # Changes Done By Anirban 30-08-2018
    # Plot the box
    # draw horizontal lines
    plt.plot([horlineX, horlineY], [maxY + verlineGap, maxY + verlineGap], color='black', linewidth=2)  # line 1 top
    plt.plot([horlineX, horlineY], [minY + horlineX, minY + horlineX], color='black', linewidth=4)  # line 2 down

    # draw vertical lines
    plt.plot([horlineX, horlineX], [maxY + verlineGap, minY + horlineX], color='black', linewidth=2)  # line 1 left
    plt.plot([verlineX, verlineY], [maxY + verlineGap, minY + horlineX], color='black', linewidth=4)  # line 2 right

    mainText = s['annonScheme']  # hard coded data validation
    attackText = s['attackText']
    dbType = s["dbType"]

    plt.text(textPlotXvalue, maintextYValue, mainText,
             horizontalalignment='left', verticalalignment='top', fontsize=20)
    plt.text(textPlotXvalue, minY - midTextValue, "Attack: " + attackText,
             horizontalalignment='left', verticalalignment='top', fontsize=15)
    plt.text(textPlotXvalue, minY - endTextValue, "Database: " + dbType,
             horizontalalignment='left', verticalalignment='top', fontsize=15)
    # End of Change

    # For some reason, savefig has to come before show
    if len(fileName) > 0:
        for ext in form:
            path = fileName + '.' + ext
            plt.savefig(path)
    if show:
        plt.show()
    return
