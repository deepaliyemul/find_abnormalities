import pandas
csv="/Users/yemuldeepali/find_abnormalities/test_dir/Data-GS100B000000001-240227_21.21.55.csv"
df = pandas.read_csv(csv, skiprows=4)

col1="AnaIn_DB.PT115H2CondTankPresPsig.Output"
col2="AnaIn_DB.TT213InvSpaceTempC.Output"
if col2 in df.columns:
    print("=======", col2)
print(df[col2])
query="`AnaIn_DB.TT213InvSpaceTempC.Output` > 2 & `AnaIn_DB.TT213InvSpaceTempC.Output` > 2"
df.query(query)

