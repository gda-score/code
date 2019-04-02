import sys
sys.path.append('../common')
from common.gdaTool import gdaTool
import pprint
pp = pprint.PrettyPrinter(indent=4)

gdaToolObj=gdaTool()
pp.pprint(gdaToolObj)
gdaToolObj.generateDBSqlForTable(sys.argv,'rawDb')
