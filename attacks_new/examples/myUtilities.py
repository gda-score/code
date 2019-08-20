
def checkMatch(score, expect, colName):
    passed = True
    for k,v in expect.items():
        if k in score['base']:
            set = score['base']
        elif k in score['col'][colName]:
            set = score['col'][colName]
        val = set[k]
        if (val != v):
            print(f"    {k}: Expect {expect[k]}, got {val}")
            passed = False
    if passed:
        print("---------------------------------------------- Pass")
    else:
        print("---------------------------------------------- Fail")
