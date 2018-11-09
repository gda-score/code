import sys
sys.path.append('../utility')
from gdaUtility import gdaUtility
import pprint

gdaUtilityObj=gdaUtility()
gdaUtilityObj.generateDBSqlForTable(sys.argv,'rawDb')
