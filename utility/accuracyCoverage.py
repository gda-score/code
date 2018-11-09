import sys
from utility.gdaUtility import gdaUtility
import pprint

gdaUtilityObj=gdaUtility()
paramsList = gdaUtilityObj._setupGdautilityParameters(sys.argv, criteria="singlingOut")
print(f" param list: {paramsList}")
for param in paramsList:
    if param['finished'] == True:
        print("The following Utility measures has been previously completed:")
        pp.pprint(param)
        print(f"Results may be found at {param['resultsPath']}")
        continue
    gdaUtilityObj._distinctUidUtilityMeasureSingleAndDoubleColumn(param)
    gdaUtilityObj._finishGdaUtility(param)
