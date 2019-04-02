import sys

sys.path.append('../common')
from utility.gdaUtility import gdaUtility
import pprint
pp = pprint.PrettyPrinter(indent=4)

gdaUtilityObj=gdaUtility()
paramsList = gdaUtilityObj.setupGdaUtilityParameters(sys.argv)
print(f" param list:")
pp.pprint(paramsList)
for param in paramsList:
    if param['finished'] == True:
        print("The following Utility measure has been previously completed:")
        pp.pprint(param)
        print(f"Results may be found at {param['resultsPath']}")
        continue
    gdaUtilityObj.distinctUidUtilityMeasureSingleAndDoubleColumn(param)
    print("Finish up")
    gdaUtilityObj.finishGdaUtility(param)
