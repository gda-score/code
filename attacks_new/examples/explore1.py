import pprint
from common.gdaScore import gdaAttack
from common.gdaUtilities import setupGdaAttackParameters

# This script examines the schema of a raw and anon database, and
# stores the schema (table names, column names and types) in a data
# structure. It also makes a few simple queries.

pp = pprint.PrettyPrinter(indent=4)

config = {
    'anonTypes': [ ['diffix','latest'] ],
    'tables': [ ['banking','accounts'] ]
}

paramsList = setupGdaAttackParameters(config)
params = paramsList[0]
pp.pprint(params)
x = gdaAttack(params)
# Let's make another gdaAttack object to validate that we can run two of
# these in parallel. But give it another name or else the cache cleanup
# can get confused.
params['name'] = 'exampleExplore2'
y = gdaAttack(params)

# Start by exploring tables and columns
# First a list of tables from the raw (postgres) database

print("Tables and columns in both databases using class methods")
for database in ('rawDb', 'anonDb'):
    print(f"{database}:")
    tables = x.getTableNames(dbType=database)
    pp.pprint(tables)
    for table in tables:
        print(f"    {table}")
        cols = x.getColNamesAndTypes(dbType=database, tableName=table)
        pp.pprint(cols)

sql = """SELECT tablename
         FROM pg_catalog.pg_tables
         WHERE schemaname != 'pg_catalog' AND
               schemaname != 'information_schema';"""
query = dict(db="raw", sql=sql)
x.askExplore(query)
rawTables = x.getExplore()
if not rawTables:
    x.cleanUp(exitMsg="Failed to get raw tables")
if 'error' in rawTables:
    x.cleanUp(exitMsg="Failed to get raw tables")
print("Tables in raw DB:")
for row in rawTables['answer']:
    print(f"   {row[0]}")

# Now a list of tables from the anon (cloak) database
sql = "show tables"
query = dict(db="anon", sql=sql)
y.askExplore(query)
anonTables = y.getExplore()
if not anonTables:
    y.cleanUp(exitMsg="Failed to get anon tables")
if 'error' in anonTables:
    y.cleanUp(exitMsg="Failed to get anon tables")
print("Tables in anon DB:")
pp.pprint(anonTables)
for row in anonTables['answer']:
    print(f"   {row[0]}")

# Let's build data structures of tables and columns
rawInfo = {}
for tab in rawTables['answer']:
    tableName = tab[0]
    rawInfo[tableName] = {}
    sql = str(f"""select column_name, data_type 
                  from information_schema.columns where
                  table_name='{tableName}'""")
    query = dict(db="raw", sql=sql)
    x.askExplore(query)
    reply = x.getExplore()
    for row in reply['answer']:
        colName = row[0]
        colType = row[1]
        rawInfo[tableName][colName] = colType
pp.pprint(rawInfo)

anonInfo = {}
for tab in anonTables['answer']:
    tableName = tab[0]
    anonInfo[tableName] = {}
    sql = str(f"show columns from {tableName}")
    query = dict(db="anon", sql=sql)
    y.askExplore(query)
    reply = y.getExplore()
    for row in reply['answer']:
        colName = row[0]
        colType = row[1]
        anonInfo[tableName][colName] = colType
pp.pprint(anonInfo)

query = dict(db="raw", sql="select count(*) from accounts")
for i in range(5):
    query['myTag'] = i
    x.askExplore(query)
while True:
    answer = x.getExplore()
    print(answer)
    tag = answer['query']['myTag']
    print(f"myTag is {tag}")
    if answer['stillToCome'] == 0:
        break

publicList = y.getPublicColValues("cli_district_id")
for entry in publicList:
    print(f"Value {entry[0]} has count {entry[1]}")
x.cleanUp(doExit=False)
y.cleanUp()
