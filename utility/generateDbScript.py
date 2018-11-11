import sys
sys.path.append('../common')
from gdaTool import gdaTool
import pprint

gdaToolObj=gdaTool()
gdaToolObj.generateDBSqlForTable(sys.argv,'rawDb')
