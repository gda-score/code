import sys
import pprint
# note simplejson because issues serializing decimal.Decimal
import simplejson as json
sys.path.append('../../common')
from gdaScore import gdaAttack

pp = pprint.PrettyPrinter(indent=4)

# This is an example of using getTableCharacteristics() to get the full
# contents of the tab_char tables

results = []

params = dict(name='testGetCharTable',
              rawDb='gdaScoreTaxiRaw',
              anonDb='cloakTaxi',
              criteria='singlingOut',
              table='rides',
              uid='uid',
              flushCache=False,
              verbose=False)

x = gdaAttack(params)
tabChar = x.getTableCharacteristics()
pp.pprint(tabChar)
x.cleanUp(doExit=True)

