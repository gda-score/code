import sys
from utility.gdaUtility import gdaUtility
import pprint

gdaUtilityObj=gdaUtility()
gdaUtilityObj.generateDBSqlForTable(sys.argv,'rawDb')