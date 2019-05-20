import sys
import pprint
from itertools import combinations
from common.gdaScore import gdaAttack, gdaScores
from common.gdaUtilities import setupGdaAttackParameters,comma_ize,makeGroupBy,finishGdaAttack,makeInNotNullConditions

pp = pprint.PrettyPrinter(indent=4)


def dumb_list_singling_out_attack(params):
    """ Dumb List attack for the Singling Out criteria.

        All it does is request rows with all columns from the anonymized
        database. The attack succeeds if the anonymized database returns
        rows that single out users, and fails otherwise. It is designed to
        work against raw and pseudonymized data."""
    attack = gdaAttack(params)
    # -------------------  Exploration Phase  ------------------------
    # We need to know the columns that are in the anonymized database
    # and in the raw database. It is these columns that we can attack.
    # (Note that pseudonymization schemes can delete some columns.)

    table = attack.getAttackTableName()
    rawColNames = attack.getColNames(dbType='rawDb')
    anonColNames = attack.getColNames(dbType='anonDb')
    uid = attack.getUidColName()
    # Select the prior columns which user wants to attack
    priorColNames = rawColNames
    # Use 3 priors. Compute the combinations of 3 columns from all the columns
    perm = combinations(priorColNames, 3)




    # -------------------  Prior Knowledge Phase  --------------------
    # This section is to learn the prior column's true values using askKnowledge()
    answer = []
    column = []
    for i in list(perm):
        query = {}
        sql = "SELECT "
        sql += str(f"ALL_THREE.{i[0]}, ALL_THREE.{i[1]}, ALL_THREE.{i[2]} FROM ")

        sql += str(f"(SELECT COMB2.{i[0]} AS one,COMB2.{i[2]} as two,COMB1.{i[1]} as three FROM ")

        sql += str(f" (SELECT {i[0]},{i[2]} FROM {table} where birthdate < '1920-01-01' ")
        sql += str(f" GROUP BY 1,2 HAVING COUNT(DISTINCT uid)>6) ")
        sql += str(f" AS COMB2 ")

        sql += str(f"JOIN ")

        sql += str(f"(SELECT {i[0]},{i[1]} FROM {table} where birthdate < '1920-01-01' ")
        sql += str(f" GROUP BY 1,2 HAVING COUNT(DISTINCT uid)>6 ")
        sql += str(f" ORDER BY {i[0]} limit 10) ")
        sql += str(f" AS COMB1 ")
        sql += str(f" ON CAST(COMB1.{i[0]} as TEXT)=COMB2.{i[0]} :: TEXT ")

        sql += str(f"JOIN ")

        sql += str(f"(SELECT {i[1]},{i[2]} FROM {table} where birthdate < '1920-01-01' ")
        sql += str(f"GROUP BY 1,2 HAVING COUNT(DISTINCT uid)>6 ")
        sql += str(f" ORDER BY {i[1]} limit 10) ")
        sql += str(f"AS COMB3 ")
        sql += str(f"ON COMB1.{i[1]} :: TEXT =COMB3.{i[1]} :: TEXT AND COMB2.{i[2]} :: TEXT =COMB3.{i[2]} :: TEXT ")

        sql += str(f") AS PAIRS ")

        sql += str(f"JOIN ")

        sql += str("(SELECT ")
        sql += comma_ize(i)
        sql += str(f" count(distinct uid) FROM {table} ")
        sql += makeGroupBy(i)
        sql += str(f"HAVING count(DISTINCT uid)=1) ")
        #sql += str("LIMIT 1) ")
        sql += str("AS ALL_THREE ")

        sql += str(f"ON PAIRS.one :: TEXT=ALL_THREE.{i[0]} :: TEXT ")
        sql += str(f"AND PAIRS.three :: TEXT =ALL_THREE.{i[1]} :: TEXT ")
        sql += str(f"AND PAIRS.two :: TEXT =ALL_THREE.{i[2]}:: TEXT ")


        query['sql'] = sql
        print("-------------------- Knowledge query:")
        print(sql)
        attack.askKnowledge(query)
        reply = attack.getKnowledge()
        if v: print("-------------------- Knowledge reply:")
        if v: pp.pprint(reply)

        for row in reply['answer']:
            answer.append(row)
            column.append(i)
    print(answer)
    print(column)

    # -------------------  Attack Phase  -----------------------------
    # This section user makes an attack on the new column(4th column) when the prior values of 3 columns is learned.
    for i in range(len(column)):

        list_col = []
        list_val = []
        perm_2 = combinations(column[i], 2)
        perm_3 = combinations(answer[i], 2)
        for a in list(perm_2):
            list_col.append(a)

        for b in list(perm_3):
            list_val.append(b)

        # ----------- Prepare attack column list ------------
        attackColNames_1 = rawColNames.copy()
        attackColNames_1 = [n for n in attackColNames_1 if n not in list(column[i])]

        # ----------- Make a loop here for all the columns in attackColNames ----------
        for j in attackColNames_1:
            print(j)

            # ------------- Learn Unknown Attributes Phase --------------------------------------
            # This section will return only 50 values of those columns where disticnt users are more than distinct uids
            reply1 = attack.getPublicColValues(colName=j)
            print(reply1)

            # ---------------- Execute for each Vu (unknown attribute) in reply1 ----------------------

            list_claim = []
            for row1 in reply1:
                print(row1)
                list_average = []

                # ----------------- For 1st Attribute --------------------------

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[2][0]} :: TEXT  = '{list_val[2][0]}' ")
                sql += str(f"AND {list_col[2][1]} :: TEXT  = '{list_val[2][1]}' ")
                sql += str(f"AND {j} :: TEXT ='{row1[0]}' ")  # If you add more than 1 column in attackColNames, chnage the AND condition also
                query['sql'] = sql
                print("------------------- Inclusive query_1:")
                print(sql)
                attack.askAttack(query)
                reply_i = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_i = attack.getKnowledge()
                if v: print("-------------------- Inclusive reply_1:")
                if v: pp.pprint(reply_i)

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[2][0]} :: TEXT  = '{list_val[2][0]}' ")
                sql += str(f"AND {list_col[2][1]} :: TEXT  = '{list_val[2][1]}' ")
                sql += str(f"AND {j} :: TEXT ='{row1[0]}' ")
                sql += str(f"AND {list_col[0][0]} :: TEXT  <> '{list_val[0][0]}' ")
                query['sql'] = sql
                print("------------------- Exclusive query_1:")
                print(sql)
                attack.askAttack(query)
                reply_e = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_e = attack.getKnowledge()
                if v: print("-------------------- Exclusive reply_1:")
                if v: pp.pprint(reply_e)

                # print(reply_i['answer'][0][0])
                # print(reply_e['answer'][0][0])

                if (reply_i['answer'][0][1] != None) and (reply_e['answer'][0][1] != None):
                    difference = reply_i['answer'][0][0] - reply_e['answer'][0][0]
                    print("Difference between inclusive and exclusive query is", difference)
                    # print(difference)
                    list_average.append(difference)

                # --------------- Negating the attribute -------------------------
                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[2][0]} :: TEXT  = '{list_val[2][0]}' ")
                sql += str(f"AND {list_col[2][1]} :: TEXT  = '{list_val[2][1]}' ")
                sql += str(f"AND {j} :: TEXT  <>'{row1[0]}' ")
                query['sql'] = sql
                print("------------------- Inclusive query after negating the unknown attribute:")
                print(sql)
                attack.askAttack(query)
                reply_negate_i = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_negate_i = attack.getKnowledge()
                if v: print("-------------------- Inclusive reply after negating the unknown attribute:")
                if v: pp.pprint(reply_negate_i)

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[2][0]} :: TEXT  = '{list_val[2][0]}' ")
                sql += str(f"AND {list_col[2][1]} :: TEXT  = '{list_val[2][1]}' ")
                sql += str(f"AND {j} :: TEXT  <>'{row1[0]}' ")
                sql += str(f"AND {list_col[0][0]} :: TEXT  <> '{list_val[0][0]}' ")
                query['sql'] = sql
                print("------------------- Exclusive query after negating the unknown attribute:")
                print(sql)
                attack.askAttack(query)
                reply_negate_e = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_negate_e = attack.getKnowledge()
                if v: print("-------------------- Exclusive reply after negating the unknown attribute:")
                if v: pp.pprint(reply_negate_e)

                # This if condition is used to check for suppressed columns
                if (reply_negate_i['answer'][0][1] != None) and (reply_negate_e['answer'][0][1] != None):
                    difference_neg = 1 - reply_negate_i['answer'][0][0] + reply_negate_e['answer'][0][0]
                    print("Difference between inclusive and exclusive when negating the attribute is", difference_neg)
                    # print(difference)
                    list_average.append(difference_neg)

                # ------ 1st Attribute end -----------------------------------

                # ----------------- For 2nd Attribute --------------------------

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[1][0]} :: TEXT  = '{list_val[1][0]}' ")
                sql += str(f"AND {list_col[1][1]} :: TEXT  = '{list_val[1][1]}' ")
                sql += str(f"AND {j} :: TEXT ='{row1[0]}' ")
                query['sql'] = sql
                #print("------------------- Inclusive query_2:")
                #print(sql)
                attack.askAttack(query)
                reply_i = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_i = attack.getKnowledge()
                #if v: print("-------------------- Inclusive reply_@:")
                #if v: pp.pprint(reply_i)

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[1][0]} :: TEXT  = '{list_val[1][0]}' ")
                sql += str(f"AND {list_col[1][1]} :: TEXT  = '{list_val[1][1]}' ")
                sql += str(f"AND {j} :: TEXT ='{row1[0]}' ")
                sql += str(f"AND {list_col[0][1]} :: TEXT  <> '{list_val[0][1]}' ")
                query['sql'] = sql
                print("------------------- Exclusive query:")
                #print(sql)
                attack.askAttack(query)
                reply_e = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_e = attack.getKnowledge()
                #if v: print("-------------------- Exclusive reply:")
                #if v: pp.pprint(reply_e)

                # print(reply_i['answer'][0][0])
                # print(reply_e['answer'][0][0])

                if (reply_i['answer'][0][1] != None) and (reply_e['answer'][0][1] != None):
                    difference = reply_i['answer'][0][0] - reply_e['answer'][0][0]
                    print("Difference between inclusive and exclusive query is", difference)
                    # print(difference)
                    list_average.append(difference)

                # --------------- Negating the attribute -------------------------
                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[1][0]} :: TEXT  = '{list_val[1][0]}' ")
                sql += str(f"AND {list_col[1][1]} :: TEXT  = '{list_val[1][1]}' ")
                sql += str(f"AND {j} :: TEXT  <>'{row1[0]}' ")
                query['sql'] = sql
                #print("------------------- Inclusive query after negating the unknown attribute:")
                #print(sql)
                attack.askAttack(query)
                reply_negate_i = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_negate_i = attack.getKnowledge()
                #if v: print("-------------------- Inclusive reply after negating the unknown attribute:")
                #if v: pp.pprint(reply_negate_i)

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[1][0]} :: TEXT  = '{list_val[1][0]}' ")
                sql += str(f"AND {list_col[1][1]} :: TEXT = '{list_val[1][1]}' ")
                sql += str(f"AND {j} :: TEXT  <>'{row1[0]}' ")
                sql += str(f"AND {list_col[0][1]} :: TEXT  <> '{list_val[0][1]}' ")
                query['sql'] = sql
                #print("------------------- Exclusive query after negating the unknown attribute:")
                #print(sql)
                attack.askAttack(query)
                reply_negate_e = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_negate_e = attack.getKnowledge()
                #if v: print("-------------------- Exclusive reply after negating the unknown attribute:")
                #if v: pp.pprint(reply_negate_e)

                # This if condition is used to check for suppressed columns
                if (reply_negate_i['answer'][0][1] != None) and (reply_negate_e['answer'][0][1] != None):
                    difference_neg = 1 - reply_negate_i['answer'][0][0] + reply_negate_e['answer'][0][0]
                    print("Difference between inclusive and exclusive when negating the attribute is", difference_neg)
                    # print(difference)
                    list_average.append(difference_neg)

                # --------------- 2nd Attribute end --------------------------------

                # ----------------- For 3rd Attribute --------------------------

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[0][0]} :: TEXT  = '{list_val[0][0]}' ")
                sql += str(f"AND {list_col[0][1]} :: TEXT = '{list_val[0][1]}' ")
                sql += str(f"AND {j} :: TEXT ='{row1[0]}' ")
                query['sql'] = sql
                #print("------------------- Inclusive query_3:")
                #print(sql)
                attack.askAttack(query)
                reply_i = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_i = attack.getKnowledge()
                #if v: print("-------------------- Inclusive reply_3:")
                #if v: pp.pprint(reply_i)

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[0][0]} :: TEXT  = '{list_val[0][0]}' ")
                sql += str(f"AND {list_col[0][1]} :: TEXT  = '{list_val[0][1]}' ")
                sql += str(f"AND {j} :: TEXT ='{row1[0]}' ")
                sql += str(f"AND {list_col[1][1]} :: TEXT  <> '{list_val[1][1]}'  ")
                query['sql'] = sql
                #print("------------------- Exclusive query_3:")
                #print(sql)
                attack.askAttack(query)
                reply_e = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_e = attack.getKnowledge()
                #if v: print("-------------------- Exclusive reply_3:")
                #if v: pp.pprint(reply_e)

                # print(reply_i['answer'][0][0])
                # print(reply_e['answer'][0][0])

                # This if condition is used to check for suppressed columns
                if (reply_i['answer'][0][1] != None) and (reply_e['answer'][0][1] != None):
                    difference = reply_i['answer'][0][0] - reply_e['answer'][0][0]
                    print("Difference between inclusive and exclusive query is", difference)
                    # print(difference)
                    list_average.append(difference)

                # --------------- Negating the attribute -------------------------
                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[0][0]} :: TEXT  = '{list_val[0][0]}' ")
                sql += str(f"AND {list_col[0][1]} :: TEXT  = '{list_val[0][1]}' ")
                sql += str(f"AND {j} :: TEXT  <>'{row1[0]}' ")
                query['sql'] = sql
                #print("------------------- Inclusive query after negating the unknown attribute:")
                #print(sql)
                attack.askAttack(query)
                reply_negate_i = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_negate_i = attack.getKnowledge()
                if v: print("-------------------- Inclusive reply after negating the unknown attribute:")
                #if v: pp.pprint(reply_negate_i)

                query = {}
                sql = "SELECT "
                sql += str(f"count(DISTINCT uid),count_noise(*) :: integer FROM {table} ")
                sql += str(f"WHERE {list_col[0][0]} :: TEXT  = '{list_val[0][0]}' ")
                sql += str(f"AND {list_col[0][1]} :: TEXT  = '{list_val[0][1]}' ")
                sql += str(f"AND {j} :: TEXT  <>'{row1[0]}' ")
                sql += str(f"AND {list_col[1][1]} :: TEXT  <> '{list_val[1][1]}' ")
                query['sql'] = sql
                #print("------------------- Exclusive query after negating the unknown attribute:")
                #print(sql)
                attack.askAttack(query)
                reply_negate_e = attack.getAttack()
                # attack.askKnowledge(query)
                # reply_negate_e = attack.getKnowledge()
                #if v: print("-------------------- Exclusive reply after negating the unknown attribute:")
                #if v: pp.pprint(reply_negate_e)

                # This if condition is used to check for suppressed columns
                if (reply_negate_i['answer'][0][1] != None) and (reply_negate_e['answer'][0][1] != None):
                    difference_neg = 1 - reply_negate_i['answer'][0][0] + reply_negate_e['answer'][0][0]
                    print("Difference between inclusive and exclusive when negating the attribute is", difference_neg)
                    # print(difference)
                    list_average.append(difference_neg)
                # --------------- 3rd Attribute end --------------------------------

                if not list_average: continue
                print("Guess contents", list_average)
                list_average_value = round(sum(list_average) / len(list_average), 3)
                print("Average of the Guesses", list_average_value)

                # If the average value for the claims is greater than 0.5, then consider it
                if (list_average_value) > 0.5:
                    list_claim.append({'col_val': row1[0], 'val': list_average_value})


            # -------------------- Claim Phase ----------------------------------------------------------

            print("Value of Claims:", list_claim)

            if not list_claim: continue
            spec = {}
            guess = []
            known = []
            guess.append({'col': j, 'val': max(list_claim,key=lambda item:item['val'])['col_val']})
            print(guess)
            for k in range(3):
                known.append({'col': column[i][k], 'val': answer[i][k]})
            print(known)
            spec['guess'] = guess
            spec['known'] = known
            attack.askClaim(spec)

            if v: print("------------------- Attack claims:")
            while True:
                reply_claim = attack.getClaim()
                if v: pp.pprint(reply_claim)
                if reply_claim['stillToCome'] == 0:
                    break


    # -------------------  Scores Phase  ----------------------------

    attackResult = attack.getResults()
    sc = gdaScores(attackResult)
    score = sc.getScores()
    if v: pp.pprint(score)
    attack.cleanUp()
    final = finishGdaAttack(params, score)
    pp.pprint(final)


verbose = True
v = verbose

paramsList = setupGdaAttackParameters(sys.argv, criteria="singlingOut",
        attackType = "Simple List Singling-Out Attack")
for params in paramsList:
    if params['finished'] == True:
        print("The following attack has been previously completed:")
        pp.pprint(params)
        print(f"Results may be found at {params['resultsPath']}")
        continue

    print("Running attack with following parameters:")
    pp.pprint(params)
    dumb_list_singling_out_attack(params)
